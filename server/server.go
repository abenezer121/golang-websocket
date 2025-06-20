package main

import (
	"context"
	"errors"
	"fastsocket/epoll"
	"fastsocket/handlers"
	"fastsocket/models"
	"fastsocket/util"
	"flag"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	serverMetrics := &models.Metrics{StartTime: time.Now()} // Initialize Metrics

	util.SetupRlimit(false)

	numWorkers := *models.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 2
	}

	jobChan := make(chan models.EventJob, *models.Workers*4)

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	notifyMap := make(map[string][]*websocket.Conn)
	driverTrackMap := make(map[*websocket.Conn]string)
	driverTrackMapMutex := &sync.RWMutex{}
	notifyMapMutex := &sync.RWMutex{}
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	epollInstance, err := epoll.NewEpoll(jobChan, appCtx, serverMetrics, *models.ReadTimeout, *models.WriteTimeout, notifyMap, notifyMapMutex, rdb, driverTrackMap, driverTrackMapMutex)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize epoll: %v", err)
	}

	// Ensure epoll FD is closed on shutdown (after wait loop exits)
	defer func() {
		log.Printf("Closing epoll FD: %d", epollInstance.Fd)
		if err := unix.Close(epollInstance.Fd); err != nil {
			log.Printf("ERROR: Failed closing epoll FD %d: %v", epollInstance.Fd, err)
		}
	}()

	workerWg := &sync.WaitGroup{}

	// Configure HTTP Server for WebSocket endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handlers.WsHander(upgrader, w, r, epollInstance)
	})

	mux.HandleFunc("/activity", func(w http.ResponseWriter, r *http.Request) {
		handlers.ControlHandler(upgrader, w, r, epollInstance)
	})

	if *models.MetricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", handlers.MetricsHandler(serverMetrics))
		metricsSrv := &http.Server{
			Addr:    *models.MetricsAddr,
			Handler: metricsMux,
		}
		go func() {
			log.Printf("Starting Metrics server on %s", *models.MetricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("ERROR: Metrics server failed: %v", err)
			}
			log.Println("Metrics server stopped.")
		}()
		// Add metrics server shutdown logic
		defer func() {
			shutdownCtxMetrics, cancelMetrics := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelMetrics()
			log.Println("Shutting down metrics server...")
			if err := metricsSrv.Shutdown(shutdownCtxMetrics); err != nil {
				log.Printf("ERROR: Metrics server shutdown failed: %v", err)
			} else {
				log.Println("Metrics server shutdown complete.")
			}
		}()
	} else {
		log.Println("Metrics server disabled.")
	}

	srv := &http.Server{
		Addr:    *models.Addr,
		Handler: mux,

		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("Starting WebSocket server on %s", *models.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("FATAL: WebSocket server ListenAndServe failed: %v", err)
		}
		log.Println("WebSocket server stopped listening.")
	}()

	// --- Graceful Shutdown Handling ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Received shutdown signal: %s. Starting graceful shutdown...", sig)

	// Stop accepting new HTTP connections
	shutdownCtxHTTP, cancelHTTP := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelHTTP()
	log.Println("Shutting down main HTTP server (stop accepting new connections)...")
	if err := srv.Shutdown(shutdownCtxHTTP); err != nil {
		log.Printf("WARN: Main HTTP server shutdown failed: %v", err)
	} else {
		log.Println("Main HTTP server stopped accepting new connections.")
	}

	// signal Epoll loop, Workers, and Health Checker to stop
	log.Println("Signalling epoll loop, workers, and health checker to stop...")
	cancelApp()

	log.Println("Waiting for epoll loop to stop...")
	epollInstance.ShutdownWg.Wait()
	log.Println("Epoll loop finished.")

	log.Println("Closing worker job channel...")
	close(jobChan)

	log.Println("Waiting for workers to finish...")
	workerWg.Wait()
	log.Println("All workers finished.")

	log.Println("Closing any remaining active connections...")
	closedCount := 0
	epollInstance.Connections.Range(func(key, value interface{}) bool {
		fd := key.(int)
		conn := value.(net.Conn)
		log.Printf("Closing connection FD %d from final cleanup.", fd)
		conn.Close()
		closedCount++
		return true
	})
	log.Printf("Closed %d connections during final cleanup.", closedCount)

	log.Println("Server gracefully shut down.")
}
