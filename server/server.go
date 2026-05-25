package main

import (
	"context"
	"errors"
	"fastsocket/epoll"
	grpcapi "fastsocket/grpc"
	"fastsocket/handlers"
	"fastsocket/models"
	"fastsocket/tracker"
	"fastsocket/util"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

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
	serverGrpcMetrics := &models.GRPCMetrics{StartTime: time.Now()}

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
	trackerSvc := tracker.NewService(rdb)

	epollInstance, err := epoll.NewEpoll(jobChan, numWorkers, appCtx, serverMetrics, *models.ReadTimeout, *models.WriteTimeout, rdb, trackerSvc)
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

	if *models.GRPCMetricsAddr != "" {
		grpcMetricsMux := http.NewServeMux()
		grpcMetricsMux.Handle("/metrics", handlers.GRPCMetricsHandler(serverGrpcMetrics))
		grpcMetricsSrv := &http.Server{
			Addr:    *models.GRPCMetricsAddr,
			Handler: grpcMetricsMux,
		}
		go func() {
			log.Printf("Starting gRPC Metrics server on %s", *models.GRPCMetricsAddr)
			if err := grpcMetricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("ERROR: gRPC Metrics server failed: %v", err)
			}
			log.Println("gRPC Metrics server stopped.")
		}()
		defer func() {
			shutdownCtxMetrics, cancelMetrics := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelMetrics()
			log.Println("Shutting down gRPC metrics server...")
			if err := grpcMetricsSrv.Shutdown(shutdownCtxMetrics); err != nil {
				log.Printf("ERROR: gRPC Metrics server shutdown failed: %v", err)
			} else {
				log.Println("gRPC Metrics server shutdown complete.")
			}
		}()
	} else {
		log.Println("gRPC Metrics server disabled.")
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

	grpcLis, err := net.Listen("tcp", *models.GRPCAddr)
	if err != nil {
		log.Fatalf("FATAL: Failed to listen for gRPC on %s: %v", *models.GRPCAddr, err)
	}
	grpcSrv := grpc.NewServer()
	
	godotenv.Load()
	grpcapi.Register(grpcSrv, trackerSvc, serverGrpcMetrics) 
	if os.Getenv("ENABLE_GRPC_REFLECTION") == "true" { 
		reflection.Register(grpcSrv) 
	}

	go func() {
		log.Printf("Starting gRPC server on %s", *models.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
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

	log.Println("Shutting down gRPC server...")
	grpcStopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcStopped)
	}()

	select {
	case <-grpcStopped:
		log.Println("gRPC server shutdown complete.")
	case <-time.After(5 * time.Second):
		log.Println("gRPC graceful shutdown timed out, forcing stop.")
		grpcSrv.Stop()
	}
	_ = grpcLis.Close()

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
