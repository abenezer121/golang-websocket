package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"golang.org/x/sys/unix"
)

var (
	addr             = flag.String("addr", ":8082", "WebSocket service address (e.g., :8080)")
	metricsAddr      = flag.String("addr", ":8089", "WebSocket service address (e.g., :8080)")
	workers          = flag.Int("workers", runtime.NumCPU()*2, "Number of worker goroutines")
	readBufSize      = flag.Int("readBuf", 4096, "Read buffer size per connection")
	writeBufSize     = flag.Int("writeBuf", 4096, "Write buffer size per connection")
	readTimeout      = flag.Duration("readTimeout", 60*time.Second, " Read timeout")
	writeTimeout     = flag.Duration("writeTimeout", 10*time.Second, "write timeout")
	pingInterval     = flag.Duration("pingInterval", 30*time.Second, "ping interval")
	epollWaitTimeout = flag.Int("epollWaitTimeout", 100, "epoll wait timeout")
)

type metrics struct {
	startTime                 time.Time
	upgradesSuccess           atomic.Int64
	upgradesFailed            atomic.Int64
	currentConnections        atomic.Int64
	totalConnections          atomic.Int64
	connectionsClosed         atomic.Int64
	connectionsClosedByPeer   atomic.Int64
	connectionsClosedByServer atomic.Int64
	messagesReceived          atomic.Int64
	messagesSent              atomic.Int64
	bytesReceived             atomic.Int64
	bytesSent                 atomic.Int64
	pingsSent                 atomic.Int64
	pongsReceived             atomic.Int64
	epollErrors               atomic.Int64
	readErrors                atomic.Int64
	writeErrors               atomic.Int64
	processingErrors          atomic.Int64
}

type epoll struct {
	fd          int
	connections sync.Map
	metrics     *metrics

	readTimeout  time.Duration
	writeTimeout time.Duration

	workerChan  chan<- eventJob
	shutdownCtx context.Context
	shutdownWg  sync.WaitGroup // to wait for the epoll loop

	// roga    core.Roga
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

func newEpoll(workerChan chan<- eventJob, shutdownCtx context.Context, m *metrics, rt, wt time.Duration) (*epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	log.Printf("Created epoll instance with fd: %d", fd)
	e := &epoll{
		fd:           fd,
		metrics:      m,
		workerChan:   workerChan,
		shutdownCtx:  shutdownCtx,
		readTimeout:  rt,
		writeTimeout: wt,
	}
	e.shutdownWg.Add(1)
	go e.wait()
	return e, nil
}

// getFd extracts the underlying file descriptor from a net.Conn.
// The returned file descriptor should be closed when no longer needed,
// except when obtained via syscall.Conn (where the connection maintains ownership).
func getFd(conn net.Conn) (int, error) {
	// First try the File() method approach for connections that support it
	type filer interface {
		File() (*os.File, error)
	}
	if fc, ok := conn.(filer); ok {
		f, err := fc.File()
		if err != nil {
			return -1, fmt.Errorf("failed to get file from connection: %w", err)
		}

		// Get the original fd
		fd := int(f.Fd())

		// Duplicate the fd because File() transfers ownership and closing the
		// *os.File would close the original connection's fd
		newFd, err := unix.Dup(fd)
		if err != nil {
			f.Close()
			return -1, fmt.Errorf("failed to dup fd %d: %w", fd, err)
		}

		// Close the original file - this won't affect our duplicated fd
		if err := f.Close(); err != nil {
			unix.Close(newFd)
			return -1, fmt.Errorf("failed to close original file: %w", err)
		}

		// fd to non-blocking mode for epoll
		if err := unix.SetNonblock(newFd, true); err != nil {
			unix.Close(newFd)
			return -1, fmt.Errorf("failed to set non-blocking mode: %w", err)
		}

		return newFd, nil
	}

	// Fall back to syscall.Conn for other connection types
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, errors.New("connection does not implement syscall.Conn or filer interface")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("failed to get raw connection: %w", err)
	}

	var fd int = -1
	var opErr error
	ctrlErr := rc.Control(func(fdPtr uintptr) {
		//  fd non-blocking - crucial for epoll edge-triggered mode
		err := unix.SetNonblock(int(fdPtr), true)
		if err != nil {
			opErr = fmt.Errorf("failed to set non-blocking: %w", err)
			return
		}
		fd = int(fdPtr)
	})

	if ctrlErr != nil {
		return -1, fmt.Errorf("failed to control raw connection: %w", ctrlErr)
	}
	if opErr != nil {
		return -1, fmt.Errorf("operation error during control: %w", opErr)
	}
	if fd <= 0 {
		return -1, errors.New("invalid file descriptor obtained")
	}

	// Note: For syscall.Conn obtained fds, we don't own the fd so we shouldn't close it
	return fd, nil
}

func (e *epoll) add(conn net.Conn) error {

	fd, err := getFd(conn)
	if err != nil {
		log.Printf("ERROR: Failed to get FD for connection: %v", err)
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
		//e.roga.LogInfo(core.LogArgs{
		//
		//	Message: "hello",
		//	//Actor          Actor           `json:"actor"`
		//
		//})
		log.Printf("ERROR: Failed to add FD %d to epoll: %v", fd, err)
		return fmt.Errorf("epoll_ctl add failed: %w", err)

	}

	e.connections.Store(fd, conn)
	count := e.metrics.currentConnections.Add(1)
	e.metrics.totalConnections.Add(1)
	log.Printf("New connection: Total count %d\n", count)
	if e.readTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(e.readTimeout)); err != nil {
			log.Printf("WARN: Failed to set initial read deadline for FD %d: %v", fd, err)

		}
	}

	return nil
}

func (e *epoll) delete(fd int) error {
	// EPOLL_CTL_DEL automatically removes the fd from all associated event queues.
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		// ENOENT or EBADF means it was already or closed]
		log.Printf("WARN: Epoll Ctl DEL error for FD %d: %v (may be benign if already closed)", fd, err)
		return nil
	}
	_, loaded := e.connections.LoadAndDelete(fd)
	if loaded {
		newCount := e.metrics.currentConnections.Add(-1)
		e.metrics.connectionsClosed.Add(1)
		log.Printf("FD %d removed from epoll and connection map. Remaining connections: %d\n", fd, newCount)
	} else {
		log.Printf("WARN: Attempted to delete FD %d from epoll map, but it was not found.\n", fd)

	}

	return nil

}

func (e *epoll) deleteAndClose(fd int, conn net.Conn, reason string, byPeer bool) {
	_ = e.delete(fd)
	if conn != nil {

		err := conn.Close()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("WARN: Error closing connection for FD %d: %v", fd, err)
			}
		} else {
			log.Printf("Connection closed: FD=%d, Reason=%s", fd, reason)
		}
	} else {
		log.Printf("WARN: deleteAndClose called for FD %d with nil connection.", fd)
	}

	if byPeer {
		e.metrics.connectionsClosedByPeer.Add(1)
	} else {
		e.metrics.connectionsClosedByServer.Add(1)
	}

}

func (e *epoll) wait() {
	// signal the gorouting has finished
	defer e.shutdownWg.Done()
	defer log.Println("Epoll wait loop stopped.")

	log.Println("Starting epoll wait loop...")

	events := make([]unix.EpollEvent, 128)
	eventQueue := &eventQueue{}
	for {
		select {
		case <-e.shutdownCtx.Done():
			log.Println("Shutdown signal received, stopping epoll wait loop.")
			return
		default:
		}
		n, err := unix.EpollWait(e.fd, events, *epollWaitTimeout)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			// EBADF (bad file descriptor),
			if errors.Is(err, unix.EBADF) {
				log.Println("Epoll wait returned EBADF, likely closed during shutdown.")
				return
			}
			log.Printf("ERROR: EpollWait failed: %v", err)
			e.metrics.epollErrors.Add(1)

			// Prevent busy-looping on persistent errors
			select {
			case <-time.After(100 * time.Millisecond):
			case <-e.shutdownCtx.Done():
				log.Println("Shutdown signal received after epoll error.")
				return
			}
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
				log.Println("Shutdown while dispatching epoll event, discarding job.")
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
		if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
			log.Printf("ERROR: WebSocket upgrade failed: Too many open files (EMFILE/ENFILE). Check RLIMIT_NOFILE. %v", err)
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
		} else if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset by peer") {
			log.Printf("INFO: WebSocket upgrade failed, client likely disconnected prematurely: %v", err)
		} else {
			log.Printf("ERROR: WebSocket upgrade failed: %v", err)
			http.Error(w, "Failed to upgrade connection", http.StatusInternalServerError)
		}
		ep.metrics.upgradesFailed.Add(1)
		return
	}
	if err := ep.add(conn); err != nil {
		log.Printf("ERROR: Failed to add WebSocket connection (FD potentially obtained) to epoll: %v", err)
		ep.metrics.upgradesFailed.Add(1)
		conn.Close()
		return
	}

	ep.metrics.upgradesSuccess.Add(1)
	log.Printf("WebSocket connection established and added to epoll.")
}

func startWorkers(n int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup, eventQueue *eventQueue) {
	if n <= 0 {
		n = runtime.NumCPU() * 2
	}

	log.Printf("Starting worker pool with %d goroutines", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go workerFunc(i, ep, jobChan, wg, eventQueue)
	}
}

func workerFunc(id int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup, eventQueue *eventQueue) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				log.Printf("Worker %d stopping: Job channel closed.", id)
				return // Channel closed, time to exit
			}
			ep.handleEvents(job.fd, job.events)
		case <-ep.shutdownCtx.Done():
			log.Printf("Worker %d stopping: Shutdown signal received.", id)
			return

		default:
			if job, ok := eventQueue.dequeue(); ok {
				ep.handleEvents(job.fd, job.events)
			} else {
				// Small sleep to prevent busy-waiting
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
}

func (ep *epoll) handleEvents(fd int, events uint32) {
	connVal, ok := ep.connections.Load(fd)

	if !ok {
		// Connection might have been closed and removed between event generation and processing.
		_ = ep.delete(fd)
		return
	}
	conn := connVal.(net.Conn)

	// Check for errors or hangup events first.
	// EPOLLRDHUP indicates the peer has closed their writing end or the whole connection.
	// EPOLLHUP often indicates an unexpected close or reset.
	// EPOLLERR indicates an error on the FD.
	if events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
		log.Printf("Handling EPOLLERR/EPOLLHUP for FD %d", fd)
		ep.metrics.readErrors.Add(1)                                   // Count as a read/general error
		ep.deleteAndClose(fd, conn, "Epoll error/hangup event", false) // Server closing due to error
		return
	}

	if events&unix.EPOLLRDHUP != 0 {
		log.Printf("Handling EPOLLRDHUP for FD %d (peer closed write end or connection)", fd)
		ep.deleteAndClose(fd, conn, "Peer closed connection (RDHUP)", true) // Closed by peer
		return
	}

	if events&unix.EPOLLOUT != 0 {
	}

	if events&unix.EPOLLIN != 0 {
		ep.handleRead(fd, conn)
	}

}

func (ep *epoll) handleRead(fd int, conn net.Conn) {
	// For edge-triggered mode, we must read until we get EAGAIN/EWOULDBLOCK
	// to ensure we process all available data notified by the edge trigger.
	for {
		if ep.readTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(ep.readTimeout)); err != nil {
				log.Printf("WARN: Failed to set read deadline for FD %d before read: %v", fd, err)
				ep.metrics.readErrors.Add(1)
				ep.deleteAndClose(fd, conn, "Failed to set read deadline", false)
				return
			}
		}

		msg, op, err := wsutil.ReadClientData(conn)

		if err != nil {

			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				log.Printf("DEBUG: Read on FD %d returned EAGAIN/EWOULDBLOCK (expected in ET)", fd)
				return
			}

			// Handle different error types
			var reason string
			closedByPeer := false
			if errors.Is(err, io.EOF) {
				reason = "EOF (client disconnected)"
				closedByPeer = true
			} else if errors.Is(err, net.ErrClosed) {
				reason = "Connection already closed"
				ep.delete(fd)
				return
			} else if errors.Is(err, syscall.ECONNRESET) {
				reason = "Connection reset by peer"
				ep.metrics.readErrors.Add(1)
				closedByPeer = true
			} else if errors.Is(err, syscall.EPIPE) {
				reason = "Broken pipe"
				ep.metrics.readErrors.Add(1)
				closedByPeer = true
			} else if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				reason = "Read timeout"
				ep.metrics.readErrors.Add(1)

			} else {

				reason = fmt.Sprintf("Unexpected read error: %v", err)
				log.Printf("ERROR: %s on FD %d", reason, fd)
				ep.metrics.readErrors.Add(1)

			}

			log.Printf("Closing FD %d due to read issue: %s", fd, reason)
			ep.deleteAndClose(fd, conn, reason, closedByPeer)
			return
		}
		ep.metrics.messagesReceived.Add(1)
		ep.metrics.bytesReceived.Add(int64(len(msg)))
		if err := ep.processMessage(fd, conn, msg, op); err != nil {
			log.Printf("ERROR: Failed to process message on FD %d: %v. Closing connection.", fd, err)
			ep.metrics.processingErrors.Add(1)
			ep.deleteAndClose(fd, conn, fmt.Sprintf("Message processing failed: %v", err), false)
			return
		}

	}
}

func (ep *epoll) processMessage(fd int, conn net.Conn, msg []byte, op ws.OpCode) error {

	switch op {
	case ws.OpText, ws.OpBinary:
		if ep.writeTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(ep.writeTimeout)); err != nil {

				return fmt.Errorf("failed to set write deadline before echo: %w", err)
			}
		}
		err := wsutil.WriteServerMessage(conn, op, msg)
		if err != nil {

			if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				return fmt.Errorf("write failed (likely closed connection): %w", err)
			} else if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				return fmt.Errorf("write timeout: %w", err)
			}

			return fmt.Errorf("failed to write echo response: %w", err)
		}
		ep.metrics.messagesSent.Add(1)
		ep.metrics.bytesSent.Add(int64(len(msg)))

	case ws.OpClose:
		log.Printf("Client initiated close on FD %d", fd)
		return errors.New("client initiated close")
	case ws.OpPing:
		if ep.writeTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(ep.writeTimeout)); err != nil {
				return fmt.Errorf("failed to set write deadline before pong: %w", err)
			}
		}
		if err := wsutil.WriteServerMessage(conn, ws.OpPong, nil); err != nil {
			return fmt.Errorf("failed to write pong: %w", err)
		}
		ep.metrics.messagesSent.Add(1)

	case ws.OpPong:
		ep.metrics.pongsReceived.Add(1)

	default:
		log.Printf("WARN: Received unknown WebSocket opcode %v on FD %d", op, fd)
		return fmt.Errorf("unknown WebSocket opcode: %v", op)
	}

	return nil
}

func (ep *epoll) startConnectionHealthCheck(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		log.Println("Connection health check (ping) disabled.")
		return
	}

	log.Printf("Starting connection health check (ping interval: %s)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer log.Println("Connection health check stopped.")

	for {
		select {
		case <-ctx.Done():
			return // Shutdown signal
		case <-ticker.C:
			// log.Println("DEBUG: Running periodic health check (ping)")
			ep.connections.Range(func(key, value interface{}) bool {
				select {
				case <-ctx.Done(): // Check shutdown again inside loop
					return false // Stop iteration
				default:
				}

				fd := key.(int)
				conn := value.(net.Conn)

				if ep.writeTimeout > 0 {
					deadline := time.Now().Add(ep.writeTimeout)
					if err := conn.SetWriteDeadline(deadline); err != nil {
						log.Printf("WARN: Health check: Failed to set write deadline for FD %d: %v", fd, err)

					}
				}

				err := wsutil.WriteServerMessage(conn, ws.OpPing, nil)
				if err != nil {
					log.Printf("Health check: Failed to send Ping to FD %d: %v. Closing connection.", fd, err)
					ep.metrics.writeErrors.Add(1)

					ep.deleteAndClose(fd, conn, fmt.Sprintf("Ping failed: %v", err), false) // Closed by server

					return true
				}
				ep.metrics.pingsSent.Add(1)

				return true
			})
		}
	}
}

// setupRlimit increases the open file descriptor limit if possible.
func setupRlimit(disable bool) {
	// RLIMIT_NOFILE is POSIX, but focus is Linux epoll
	if disable || runtime.GOOS != "linux" {
		log.Println("Skipping RLIMIT_NOFILE adjustment.")
		return
	}

	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		log.Printf("WARN: Failed to get initial RLIMIT_NOFILE: %v", err)
		return
	}
	log.Printf("Initial RLIMIT_NOFILE: Soft=%d, Hard=%d", rlimit.Cur, rlimit.Max)

	desiredLimit := rlimit
	desiredLimit.Cur = desiredLimit.Max

	log.Printf("Attempting to set RLIMIT_NOFILE: Soft=%d, Hard=%d", desiredLimit.Cur, desiredLimit.Max)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
		log.Printf("WARN: Failed to set RLIMIT_NOFILE: %v. Check user permissions.", err)
		if rlimit.Cur < 65536 {
			log.Printf("Attempting to set RLIMIT_NOFILE soft limit to 65536")
			desiredLimit.Cur = 65536
			if desiredLimit.Cur > rlimit.Max {
				desiredLimit.Cur = rlimit.Max
			}
			if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
				log.Printf("WARN: Failed to set RLIMIT_NOFILE soft limit to %d: %v", desiredLimit.Cur, err)
			}
		}
	}

	var actualLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &actualLimit); err != nil {
		log.Printf("ERROR: Failed to get RLIMIT_NOFILE after setting attempt: %v", err)
	} else {
		log.Printf(">>> Actual RLIMIT_NOFILE after attempt: Soft=%d, Hard=%d <<<", actualLimit.Cur, actualLimit.Max)
		if actualLimit.Cur < 1024 {
			log.Printf("WARNING: Soft limit for open files (%d) is low. Server may hit connection limits.", actualLimit.Cur)
		}
	}
}

func metricsHandler(m *metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		now := time.Now()
		uptime := now.Sub(m.startTime)

		fmt.Fprintf(w, "# Go WebSocket Server Metrics\n")
		fmt.Fprintf(w, "websocket_server_uptime_seconds %f\n", uptime.Seconds())

		fmt.Fprintf(w, "# HELP websocket_server_upgrades_success_total Total successful WebSocket upgrades.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_upgrades_success_total counter\n")
		fmt.Fprintf(w, "websocket_server_upgrades_success_total %d\n", m.upgradesSuccess.Load())

		fmt.Fprintf(w, "# HELP websocket_server_upgrades_failed_total Total failed WebSocket upgrades.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_upgrades_failed_total counter\n")
		fmt.Fprintf(w, "websocket_server_upgrades_failed_total %d\n", m.upgradesFailed.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_current Current number of active WebSocket connections.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_current gauge\n")
		fmt.Fprintf(w, "websocket_server_connections_current %d\n", m.currentConnections.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_total Total WebSocket connections handled since start.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_total %d\n", m.totalConnections.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_total Total connections closed since start.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_total %d\n", m.connectionsClosed.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_by_peer_total Connections closed by the client/peer.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_by_peer_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_by_peer_total %d\n", m.connectionsClosedByPeer.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_by_server_total Connections closed by the server (errors, timeouts, etc.).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_by_server_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_by_server_total %d\n", m.connectionsClosedByServer.Load())

		fmt.Fprintf(w, "# HELP websocket_server_messages_received_total Total WebSocket messages received.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_messages_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_messages_received_total %d\n", m.messagesReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_messages_sent_total Total WebSocket messages sent (includes control frames like pong).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_messages_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_messages_sent_total %d\n", m.messagesSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_bytes_received_total Total bytes received in WebSocket messages.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_bytes_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_bytes_received_total %d\n", m.bytesReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_bytes_sent_total Total bytes sent in WebSocket messages.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_bytes_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_bytes_sent_total %d\n", m.bytesSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_pings_sent_total Total keep-alive pings sent by the server.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_pings_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_pings_sent_total %d\n", m.pingsSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_pongs_received_total Total pongs received from clients.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_pongs_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_pongs_received_total %d\n", m.pongsReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_epoll_errors_total Total errors encountered in the epoll wait loop.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_epoll_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_epoll_errors_total %d\n", m.epollErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_read_errors_total Total errors during connection reads (excluding EOF/expected closes).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_read_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_read_errors_total %d\n", m.readErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_write_errors_total Total errors during connection writes.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_write_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_write_errors_total %d\n", m.writeErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_processing_errors_total Total errors during message processing (after read, before/during write).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_processing_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_processing_errors_total %d\n", m.processingErrors.Load())

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines that currently exist.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
		fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", memStats.Alloc)
		fmt.Fprintf(w, "# HELP go_memstats_sys_bytes Number of bytes obtained from system.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_sys_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_sys_bytes %d\n", memStats.Sys)
	}
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
	serverMetrics := &metrics{startTime: time.Now()}

	setupRlimit(false)

	numWorkers := *workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 2
	}

	jobChan := make(chan eventJob, *workers*4)

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	epollInstance, err := newEpoll(jobChan, appCtx, serverMetrics, *readTimeout, *writeTimeout)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize epoll: %v", err)
	}

	// Ensure epoll FD is closed on shutdown (after wait loop exits)
	defer func() {
		log.Printf("Closing epoll FD: %d", epollInstance.fd)
		if err := unix.Close(epollInstance.fd); err != nil {
			log.Printf("ERROR: Failed closing epoll FD %d: %v", epollInstance.fd, err)
		}
	}()

	// Start Worker Goroutines
	eventQueue := &eventQueue{}
	workerWg := &sync.WaitGroup{}
	startWorkers(numWorkers, epollInstance, jobChan, workerWg, eventQueue)

	// Start Connection Health Checker
	healthCheckCtx, cancelHealthCheck := context.WithCancel(appCtx) // Inherit from appCtx
	defer cancelHealthCheck()
	go epollInstance.startConnectionHealthCheck(healthCheckCtx, *pingInterval)

	// Configure HTTP Server for WebSocket endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHander(w, r, epollInstance)
	})

	// Configure and Start Metrics Server (on a separate port)
	if *metricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", metricsHandler(serverMetrics))
		metricsSrv := &http.Server{
			Addr:    *metricsAddr,
			Handler: metricsMux,
		}
		go func() {
			log.Printf("Starting Metrics server on %s", *metricsAddr)
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
		Addr:    *addr,
		Handler: mux,

		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Goroutine to run the main server
	go func() {
		log.Printf("Starting WebSocket server on %s", *addr)
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
	epollInstance.shutdownWg.Wait()
	log.Println("Epoll loop finished.")

	log.Println("Closing worker job channel...")
	close(jobChan)

	log.Println("Waiting for workers to finish...")
	workerWg.Wait()
	log.Println("All workers finished.")

	log.Println("Closing any remaining active connections...")
	closedCount := 0
	epollInstance.connections.Range(func(key, value interface{}) bool {
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

// // // sudo sh -c "echo 2621440 > /proc/sys/net/netfilter/nf_conntrack_max"
