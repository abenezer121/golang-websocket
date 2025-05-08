package main

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/config"
	"fastsocket/epoll"
	"fastsocket/handlers"
	"fastsocket/models"
	"fastsocket/util"
	"flag"
	"fmt"
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
	driverTrackMap := make(map[*websocket.Conn][]string)
	driverTrackMapMutex := &sync.RWMutex{}
	notifyMapMutex := &sync.RWMutex{}
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	// Subscribe to keyspace events for expired keys
	pubsub := rdb.PSubscribe(context.Background(), "__keyevent@0__:expired")
	defer pubsub.Close()

	ch := pubsub.Channel()

	go func() {
		for msg := range ch {
			log.Printf("Expired key received: %s\n", msg.Payload)
			if msg.Payload == config.WorkerDetailsHash {

				existingData, err := rdb.HGet(context.Background(), config.WorkerDetailsHash, msg.Payload).Result()
				if err == nil {
					continue
				}

				var existingWorker models.Command
				if err := json.Unmarshal([]byte(existingData), &existingWorker); err != nil {
					continue
				}
				// make the driver inactive
				activeDriver := false
				workerToStore := existingWorker
				existingWorker.Active = &activeDriver

				workerJSON, err := json.Marshal(workerToStore)
				if err != nil {
					log.Printf("Error marshalling worker details for %s: %v\n", msg.Payload, err)
					continue
				}
				fmt.Println(workerJSON)
				//rdb.HSet(context.Background(), config.WorkerDetailsHash, msg.Payload, string(workerJSON))

			}
		}
	}()

	// Add error handling for subscription
	go func() {
		for err := range pubsub.Channel() {
			if err != nil {
				log.Printf("Subscription error: %v\n", err)
			}
		}
	}()

	epollInstance, err := epoll.NewEpoll(jobChan, appCtx, serverMetrics, *models.ReadTimeout, *models.WriteTimeout, notifyMap, notifyMapMutex, rdb, driverTrackMap, driverTrackMapMutex)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize epoll: %v", err)
	}

	//go epollInstance.StartConnectionHealthCheck(context.Background(), 2*time.Second)

	// Ensure epoll FD is closed on shutdown (after wait loop exits)
	defer func() {
		log.Printf("Closing epoll FD: %d", epollInstance.Fd)
		if err := unix.Close(epollInstance.Fd); err != nil {
			log.Printf("ERROR: Failed closing epoll FD %d: %v", epollInstance.Fd, err)
		}
	}()

	// Start Worker Goroutines
	//eventQueue := &models.EventQueue{}
	workerWg := &sync.WaitGroup{}
	//util.StartWorkers(numWorkers, epollInstance, jobChan, workerWg, eventQueue)

	//// Start Connection Health Checker
	//healthCheckCtx, cancelHealthCheck := context.WithCancel(appCtx) // Inherit from appCtx
	//defer cancelHealthCheck()
	//go epollInstance.StartConnectionHealthCheck(healthCheckCtx, *models.PingInterval)

	// Configure HTTP Server for WebSocket endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handlers.WsHander(upgrader, w, r, epollInstance)
	})

	// track
	// get path
	mux.HandleFunc("/activity", func(w http.ResponseWriter, r *http.Request) {
		handlers.ControlHandler(upgrader, w, r, epollInstance)
	})

	// Configure and Start Metrics Server (on a separate port)
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

	// Configure and Start Main WebSocket Server
	srv := &http.Server{
		Addr:    *models.Addr,
		Handler: mux,

		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Goroutine to run the main server
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
	cancelApp() // This triggers shutdown in epoll.wait, workerFunc, startConnectionHealthCheck

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

// Todo filter out drivers based on user input
// TODO currently the redis is filtered out when requested
// TODO handle incoming messages efficiently
// TODO health checker
// TODO remove disconnected drivers from redis
// TODO filter out unpdated from redis
// Todo Redis Cleanup
// Todo use gorouting pool instead of creating everytime in the epoll
