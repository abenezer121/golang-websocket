package main

import (
	"context"
	"embed"
	"errors"
	"fastsocket/epoll"
	"fastsocket/handlers"
	"fastsocket/models"
	"fastsocket/util"
	"flag"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
	"io/fs"
	"log"
	// "net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

//go:embed web/*
var webAssets embed.FS

// to monitor the system
func monitorSystem() {
	for {
		log.Printf("[MONITOR] Active Goroutines: %d", runtime.NumGoroutine())
		time.Sleep(2 * time.Second)
	}
}

func main() {
	go monitorSystem()
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	serverMetrics := &models.Metrics{StartTime: time.Now()} // Initialize Metrics

	util.SetupRlimit(false)

	numWorkers := *models.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 2
	}

	jobChan := make(chan models.EventJob, numWorkers*4)

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	epollInstance, err := epoll.NewEpoll(jobChan, numWorkers, appCtx, serverMetrics, *models.ReadTimeout, *models.WriteTimeout, rdb)
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

	// Configure HTTP Server for WebSocket endpoint
	mux := http.NewServeMux()
	webFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize embedded web assets: %v", err)
	}
	webHandler := http.FileServer(http.FS(webFS))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		webHandler.ServeHTTP(w, r)
	}))

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("ROUTE HIT: driver websocket path=%s remote=%s", r.URL.Path, r.RemoteAddr)
		handlers.WsHander(upgrader, w, r, epollInstance)
	})

	mux.HandleFunc("/activity", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("ROUTE HIT: watcher websocket path=%s remote=%s", r.URL.Path, r.RemoteAddr)
		handlers.ControlHandler(upgrader, w, r, epollInstance)
	})
	// sse endpoint
	mux.HandleFunc("/sse", handlers.SSEHandler(epollInstance))
	// Http endpoint
	mux.Handle("/driver/update", http.TimeoutHandler(
		handlers.HandleDriverUpdateHTTP(epollInstance),
		10*time.Second,
		`{"status":"error","message":"driver update request timed out"}`,
	))

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

		ReadTimeout: 10 * time.Second,
		// WriteTimeout: 10 * time.Second,
		WriteTimeout: 0 * time.Second,
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

	log.Println("Waiting for workers to finish...")
	epollInstance.WorkerWg.Wait()
	log.Println("All workers finished.")

	log.Println("Closing worker job channel...")
	close(jobChan)

	log.Println("Closing SSE subscribers...")
	epollInstance.UnregisterAllSSESubscribers()

	log.Println("Closing any remaining active connections...")
	closedCount := 0
	epollInstance.Connections.Range(func(key, value interface{}) bool {
		fd := key.(int)
		conn := value.(*websocket.Conn)
		log.Printf("Closing connection FD %d from final cleanup.", fd)
		conn.Close()
		closedCount++
		return true
	})
	log.Printf("Closed %d connections during final cleanup.", closedCount)

	log.Println("Server gracefully shut down.")
}
