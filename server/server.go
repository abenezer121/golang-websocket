package main

import (
	"context"
	"errors"
	"fastsocket/handlers"
	"fastsocket/models"
	"fastsocket/util"
	"flag"
	"strings"

	// "github.com/dullkingsman/go-pkg/roga/core"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"golang.org/x/sys/unix"
)

func startWorkers(n int, ep *models.Epoll, jobChan <-chan models.EventJob, wg *sync.WaitGroup, eventQueue *models.EventQueue) {
	if n <= 0 {
		n = runtime.NumCPU() * 2
	}

	log.Printf("Starting worker pool with %d goroutines", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go workerFunc(i, ep, jobChan, wg, eventQueue)
	}
}

func workerFunc(id int, ep *models.Epoll, jobChan <-chan models.EventJob, wg *sync.WaitGroup, eventQueue *models.EventQueue) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				log.Printf("Worker %d stopping: Job channel closed.", id)
				return // Channel closed, time to exit
			}
			ep.HandleEvents(job.Fd, job.Events)
		case <-ep.ShutdownCtx.Done():
			log.Printf("Worker %d stopping: Shutdown signal received.", id)
			return

		default:
			if job, ok := eventQueue.Dequeue(); ok {
				ep.HandleEvents(job.Fd, job.Events)
			} else {
				// Small sleep to prevent busy-waiting
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
}

func wsHander(w http.ResponseWriter, r *http.Request, ep *models.Epoll) {
	log.Printf("hello htere")
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
			log.Printf("ERROR: WebSocket upgrade failed: Too many open files (EMFILE/ENFILE). Check RLIMIT_NOFILE. %v", err)
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
		} else if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset by peer") {
			log.Printf("INFO: WebSocket upgrade failed, client likely disconnected prematurely: %v", err)
		} else {
			log.Printf("ERROR: WebSocket upgrade failed: %v", err)
			http.Error(w, "Failed to upgrade connection", http.StatusInternalServerError)
		}
		ep.Metrics.UpgradesFailed.Add(1)
		return
	}
	if err := ep.Add(conn); err != nil {
		log.Printf("ERROR: Failed to add WebSocket connection (FD potentially obtained) to epoll: %v", err)
		ep.Metrics.UpgradesFailed.Add(1)
		conn.Close()
		return
	}

	ep.Metrics.UpgradesSuccess.Add(1)
	log.Printf("WebSocket connection established and added to epoll.")
}
func main() {
	flag.Parse()
	// functions begin operation
	// funcion end operation
	// roga

	// serviceName
	//roga := core.Init(core.Config{ServiceName: "SocketServer"})

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	// Initialize Metrics
	serverMetrics := &models.Metrics{StartTime: time.Now()}

	util.SetupRlimit(false)

	numWorkers := *models.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 2
	}

	jobChan := make(chan models.EventJob, *models.Workers*4)

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	epollInstance, err := models.NewEpoll(jobChan, appCtx, serverMetrics, *models.ReadTimeout, *models.WriteTimeout)
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

	// Start Worker Goroutines
	eventQueue := &models.EventQueue{}
	workerWg := &sync.WaitGroup{}
	startWorkers(numWorkers, epollInstance, jobChan, workerWg, eventQueue)

	// Start Connection Health Checker
	healthCheckCtx, cancelHealthCheck := context.WithCancel(appCtx) // Inherit from appCtx
	defer cancelHealthCheck()
	go epollInstance.StartConnectionHealthCheck(healthCheckCtx, *models.PingInterval)

	// Configure HTTP Server for WebSocket endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHander(w, r, epollInstance)
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

// get all my drivers paginated
