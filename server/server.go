package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"golang.org/x/sys/unix"
)

var (
	addr         = flag.String("addr", ":8080", "WebSocket service address (e.g., :8080)")
	workers      = flag.Int("workers", runtime.NumCPU()*2, "Number of worker goroutines")
	readBufSize  = flag.Int("readBuf", 4096, "Read buffer size per connection")
	writeBufSize = flag.Int("writeBuf", 4096, "Write buffer size per connection")
)

type epoll struct {
	fd          int
	connections sync.Map

	totalSocket atomic.Int64

	workerChan  chan<- eventJob
	shutdownCtx context.Context
}

type eventJob struct {
	fd     int
	events uint32
}

type eventQueue struct {
	mu    sync.Mutex
	queue []eventJob
}

func (eq *eventQueue) enqueue(job eventJob) {
	eq.mu.Lock()
	defer eq.mu.Unlock()
	eq.queue = append(eq.queue, job)
}

func (eq *eventQueue) dequeue() (eventJob, bool) {
	eq.mu.Lock()
	defer eq.mu.Unlock()
	if len(eq.queue) == 0 {
		return eventJob{}, false
	}
	job := eq.queue[0]
	eq.queue = eq.queue[1:]
	return job, true
}

func newEpoll(workerChan chan<- eventJob, shutdownCtx context.Context) (*epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("Canot create epoll in the kernel")
	}
	log.Printf("created epoll instance with fd : %d", fd)
	return &epoll{
		fd:          fd,
		shutdownCtx: shutdownCtx,
		workerChan:  workerChan,
	}, nil
}

func (e *epoll) add(conn net.Conn) error {

	// Get file descriptor
	fd, err := getFd(conn)
	if err != nil {
		fmt.Println("Unable to get fd in add")
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	// Add connection FD to epoll with edge-triggered monitoring for read, write readiness, and peer close.
	// EPOLLET (Edge Triggered): Notify only on changes. Requires careful reading/writing all available data.
	// EPOLLIN: Monitor for read readiness.
	// EPOLLOUT: Monitor for write readiness (useful for handling backpressure, currently write handling is basic).
	// EPOLLRDHUP: Monitor for peer closing connection or half-closing write side. Robust way to detect closure.
	// EPOLLERR: Monitor for errors (implicitly monitored, but good to include).
	// EPOLLHUP: Monitor for hangup (implicitly monitored).

	// Add to epoll with edge-triggered mode
	err = unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP | unix.EPOLLET | unix.EPOLLERR | unix.EPOLLHUP,
		Fd:     int32(fd),
	})
	if err != nil {
		conn.Close() // close the
		fmt.Println("failed to add to epoll ", err)
		return fmt.Errorf("failed to add to epoll: %w", err)
	}

	e.connections.Store(fd, conn)
	connCount := e.totalSocket.Add(1)
	fmt.Printf("New connection: Total count %d\n", connCount)

	return nil
}

func (e *epoll) delete(fd int) error {
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		// ENOENT means it was already removed or never added
		return fmt.Errorf("epoll ctl del error: %w", err)
	}
	_, loaded := e.connections.LoadAndDelete(fd)
	if loaded {

		newCount := e.totalSocket.Add(-1)
		log.Printf("FD %d removed from connection map. Remaining connections: %d", fd, newCount)
	} else {
	}

	return nil

}

func getFd(conn net.Conn) (int, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, errors.New("connection does not implement syscall.Conn")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("failed to get raw connection: %w", err)
	}
	var fd int
	var opErr error
	err = rc.Control(func(fdPtr uintptr) {
		fd = int(fdPtr)
	})
	if err != nil {
		return -1, fmt.Errorf("failed to control raw connection: %w", err)
	}
	if opErr != nil {
		return -1, fmt.Errorf("operation error during control: %w", opErr)
	}

	if fd <= 0 {
		return -1, errors.New("invalid file descriptor obtained")
	}
	return fd, nil
}

func (e *epoll) wait() {
	events := make([]unix.EpollEvent, 128)
	eventQueue := &eventQueue{}
	for {
		select {
		case <-e.shutdownCtx.Done():
			fmt.Println("Epoll loop shutting down")
			return
		default:
		}
		n, err := unix.EpollWait(e.fd, events, -1)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if errors.Is(err, unix.EBADF) {
				return
			}
			fmt.Printf("EpollWait failed: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			job := eventJob{
				fd:     int(ev.Fd),
				events: ev.Events,
			}
			select {
			case e.workerChan <- job:
			case <-e.shutdownCtx.Done():
				// log.Println("Shutdown while dispatching epoll event.")
				return
			default:
				eventQueue.enqueue(job)
				// log.Printf("WARNING: Worker channel full. Queuing event for FD %d (Events: 0x%x).", job.fd, job.events)
			}
		}
	}
}

func wsHander(w http.ResponseWriter, r *http.Request, ep *epoll) {

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		if errors.Is(err, syscall.EMFILE) { // "Too many open files"
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
			// Log this specific error clearly!
			log.Printf("ws.UpgradeHTTP failed: %v (EMFILE)", err)
			return
		}
		// Log other upgrade errors too
		log.Printf("ws.UpgradeHTTP failed: %v", err)
		fmt.Printf("Upgrade error: %v\n", err) // You have this already
		os.Exit(1)
		return
	}
	if err := ep.add(conn); err != nil {
		fmt.Println("Failed to add connection to epoll:", err)
		conn.Close()
		return
	}
}

func startWorkers(n int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup, eventQueue *eventQueue) {
	fmt.Println("Starting worker pool ", " count ", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go workerFunc(i, ep, jobChan, wg, eventQueue)
	}
}

func (ep *epoll) deleteAndClose(conn net.Conn) {
	fd, err := getFd(conn)
	if err != nil {
		return
	}
	err = ep.delete(fd)
	if err != nil {

		log.Printf("Error removing fd %d from epoll: %v\n", fd, err)
	}

	err = conn.Close()
	if err != nil {
		if !errors.Is(err, net.ErrClosed) {
			log.Printf("Error closing connection for fd %d: %v\n", fd, err)
		}
	}
}

func (ep *epoll) handleEvents(fd int, events uint32) {
	conn, ok := ep.connections.Load(fd)

	if !ok {
		fmt.Println("Ignoring events for already closed client")
		ep.delete(fd)
		return
	}
	wsConn, ok := conn.(net.Conn)
	if !ok {
		fmt.Println("Invalid connection type")
		ep.deleteAndClose(wsConn)
		return
	}

	// Check for errors or hangup events first.
	// EPOLLRDHUP indicates the peer has closed their writing end or the whole connection.
	// EPOLLHUP often indicates an unexpected close or reset.
	// EPOLLERR indicates an error on the FD.
	if events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {

		ep.deleteAndClose(wsConn)
		return
	}

	if events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		ep.deleteAndClose(wsConn)
		return
	}

	if events&unix.EPOLLOUT != 0 {
		// handle write
		// c.handleWrite()
		// if c.closed.Load() {
		// 	return
		// }
	}

	if events&unix.EPOLLIN != 0 {
		// handle read
		ep.handleRead(conn.(net.Conn), fd)
		// if c.closed.Load() {
		// 	return
		// }
	}

}

func (ep *epoll) handleRead(conn net.Conn, fd int) {
	// For edge-triggered mode, we must read until we get EAGAIN/EWOULDBLOCK
	for {
		// Read a single WebSocket message
		msg, op, err := wsutil.ReadClientData(conn)
		if err != nil {
			// Handle different error types
			var reason string
			switch {
			case errors.Is(err, io.EOF):
				reason = "EOF (client disconnected)"
			case errors.Is(err, net.ErrClosed):
				reason = "connection already closed"
			case errors.Is(err, syscall.EPIPE):
				reason = "broken pipe"
			case errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK):
				// In edge-triggered mode, this means we've read all available data
				return
			default:
				if opErr, ok := err.(*net.OpError); ok {
					if errors.Is(opErr.Err, syscall.ECONNRESET) {
						reason = "connection reset by peer"
					} else if errors.Is(opErr.Err, syscall.ETIMEDOUT) {
						reason = "read timeout"
					}
				}
				if reason == "" {
					reason = fmt.Sprintf("read error: %v", err)
				}
			}

			log.Printf("Closing FD %d: %s", fd, reason)
			ep.deleteAndClose(conn)
			return
		}

		// Process the message (example implementation)
		if err := ep.processMessage(conn, fd, msg, op); err != nil {
			log.Printf("Failed to process message on FD %d: %v", fd, err)
			ep.deleteAndClose(conn)
			return
		}

		// For edge-triggered mode, we should continue reading to drain the buffer
		// until we get EAGAIN/EWOULDBLOCK
	}
}

// Example message processor (you should customize this)
func (ep *epoll) processMessage(conn net.Conn, fd int, msg []byte, op ws.OpCode) error {
	// Simple echo server example:
	switch op {
	case ws.OpText, ws.OpBinary:
		log.Printf("Received message on FD %d: %s", fd, string(msg))

		// Echo the message back
		if err := wsutil.WriteServerMessage(conn, op, msg); err != nil {
			return fmt.Errorf("failed to write response: %w", err)
		}

	case ws.OpClose:
		return fmt.Errorf("client initiated close")

	case ws.OpPing:
		// Respond to ping with pong
		if err := wsutil.WriteServerMessage(conn, ws.OpPong, nil); err != nil {
			return fmt.Errorf("failed to write pong: %w", err)
		}

	case ws.OpPong:
		// Optional: Handle pong (usually no action needed)
		log.Printf("Received Pong on FD %d", fd)

	default:
		return fmt.Errorf("unknown WebSocket opcode: %v", op)
	}

	return nil
}

func workerFunc(id int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup, eventQueue *eventQueue) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			ep.handleEvents(job.fd, job.events)
		default:
			if job, ok := eventQueue.dequeue(); ok {
				ep.handleEvents(job.fd, job.events)
			} else {
				// Small sleep to prevent busy-waiting
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func main() {
	flag.Parse()

	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		log.Fatalf("Error getting initial rlimit: %v", err) // Use log.Fatalf
	}
	log.Printf("Initial limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	// Prepare the desired limit
	desiredLimit := rlimit              // Copy struct
	desiredLimit.Cur = desiredLimit.Max // Try to set soft = hard

	log.Printf("Attempting to set limits: Soft=%d, Hard=%d\n", desiredLimit.Cur, desiredLimit.Max)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
		// THIS IS IMPORTANT! Log the error if setting fails.
		log.Printf("!!! Error setting rlimit: %v !!!", err)
		log.Printf("!!! Check user permissions (need root or CAP_SYS_RESOURCE to raise hard limit) !!!")
		// Decide if you want to exit or continue with potentially lower limits
		// os.Exit(1) // Optionally exit if setting limit is critical
	}

	// *** READ BACK THE LIMITS TO SEE WHAT ACTUALLY APPLIES ***
	var actualLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &actualLimit); err != nil {
		log.Fatalf("Error getting rlimit AFTER setting attempt: %v", err)
	}
	log.Printf(">>> Actual limits AFTER Setrlimit call: Soft=%d, Hard=%d <<<\n", actualLimit.Cur, actualLimit.Max)

	jobChan := make(chan eventJob, *workers*2)
	shutdownCtx, cancelEpollLoop := context.WithCancel(context.Background())
	epoll, err := newEpoll(jobChan, shutdownCtx)
	if err != nil {
		fmt.Println("Failed to initialize epoll", "error", err)
		os.Exit(1)
	}
	// Create eventQueue
	eventQueue := &eventQueue{}
	// Close the epoll file descriptor on shutdown
	defer func() {
		log.Printf("Closing epoll FD: %d", epoll.fd)
		if err := unix.Close(epoll.fd); err != nil {
			log.Printf("Error closing epoll FD %d: %v", epoll.fd, err)
		}
	}()

	// start worker goroutines
	workerWg := &sync.WaitGroup{}
	startWorkers(*workers, epoll, jobChan, workerWg, eventQueue)
	go epoll.wait()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHander(w, r, epoll)

	})

	fmt.Println("Listening on port ", addr)

	srv := http.Server{Addr: *addr}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	fmt.Println("Shutdown signal received", sig.String())
	shutdownCtxHTTP, cancelHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTP()
	if err := srv.Shutdown(shutdownCtxHTTP); err != nil {
		fmt.Println("WebSocket server shutdown failed", "error", err)
	} else {
		fmt.Println("WebSocket server stopped accepting new connections")
	}
	fmt.Println("signalling epoll loop and workers to shut down ...")
	cancelEpollLoop()

	// close worker channel
	close(jobChan)

	fmt.Println("Waiting for workers to finish...")
	workerWg.Wait()
	fmt.Println("All workers finished.")
	closedCount := 0
	epoll.connections.Range(func(key, value interface{}) bool {
		fd := key.(int)
		conn := value.(net.Conn)
		log.Printf("Closing connection FD %d from final cleanup.", fd)
		conn.Close() // Ignore error here
		closedCount++
		return true // Continue iteration
	})
	log.Printf("Closed %d connections during final cleanup.", closedCount)
	log.Printf("Server gracefully shut down.")

}

// // // sudo sh -c "echo 2621440 > /proc/sys/net/netfilter/nf_conntrack_max"

// // // func (ep *epoll) startConnectionHealthCheck(ctx context.Context) {
// // // 	ticker := time.NewTicker(30 * time.Second)
// // // 	defer ticker.Stop()

// // // 	for {
// // // 		select {
// // // 		case <-ctx.Done():
// // // 			return
// // // 		case <-ticker.C:
// // // 			ep.connections.Range(func(key, value interface{}) bool {
// // // 				fd := key.(int)
// // // 				conn := value.(net.Conn)

// // // 				// Try to write a ping frame
// // // 				err := wsutil.WriteClientMessage(conn, ws.OpPing, nil)
// // // 				if err != nil {
// // // 					ep.deleteAndClose(conn)
// // // 				}
// // // 				return true
// // // 			})
// // // 		}
// // // 	}
// // // }
