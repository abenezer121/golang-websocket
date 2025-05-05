package models

import (
	"context"
	"errors"
	"fastsocket/util"
	"fmt"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

var (
	epollWaitTimeout = 100
)

type Epoll struct {
	Fd          int
	Connections sync.Map
	Metrics     *Metrics

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	WorkerChan  chan<- EventJob
	ShutdownCtx context.Context
	ShutdownWg  sync.WaitGroup // to wait for the epoll loop

	// roga    core.Roga
}

func NewEpoll(workerChan chan<- EventJob, shutdownCtx context.Context, m *Metrics, rt, wt time.Duration) (*Epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	log.Printf("Created epoll instance with fd: %d", fd)
	e := &Epoll{
		Fd:           fd,
		Metrics:      m,
		WorkerChan:   workerChan,
		ShutdownCtx:  shutdownCtx,
		ReadTimeout:  rt,
		WriteTimeout: wt,
	}
	e.ShutdownWg.Add(1)
	go e.Wait()
	return e, nil
}

func (e *Epoll) Add(conn net.Conn) error {

	fd, err := util.GetFd(conn)
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
	err = unix.EpollCtl(e.Fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
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

	e.Connections.Store(fd, conn)
	count := e.Metrics.CurrentConnections.Add(1)
	e.Metrics.TotalConnections.Add(1)
	log.Printf("New connection: Total count %d\n", count)
	if e.ReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(e.ReadTimeout)); err != nil {
			log.Printf("WARN: Failed to set initial read deadline for FD %d: %v", fd, err)

		}
	}

	return nil
}

func (e *Epoll) Delete(fd int) error {
	// EPOLL_CTL_DEL automatically removes the fd from all associated event queues.
	err := unix.EpollCtl(e.Fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		// ENOENT or EBADF means it was already or closed]
		log.Printf("WARN: Epoll Ctl DEL error for FD %d: %v (may be benign if already closed)", fd, err)
		return nil
	}
	_, loaded := e.Connections.LoadAndDelete(fd)
	if loaded {
		newCount := e.Metrics.CurrentConnections.Add(-1)
		e.Metrics.ConnectionsClosed.Add(1)
		log.Printf("FD %d removed from epoll and connection map. Remaining connections: %d\n", fd, newCount)
	} else {
		log.Printf("WARN: Attempted to delete FD %d from epoll map, but it was not found.\n", fd)

	}

	return nil

}

func (e *Epoll) DeleteAndClose(fd int, conn net.Conn, reason string, byPeer bool) {
	_ = e.Delete(fd)
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
		e.Metrics.ConnectionsClosedByPeer.Add(1)
	} else {
		e.Metrics.ConnectionsClosedByServer.Add(1)
	}

}

func (e *Epoll) Wait() {
	// signal the gorouting has finished
	defer e.ShutdownWg.Done()
	defer log.Println("Epoll wait loop stopped.")

	log.Println("Starting epoll wait loop...")

	events := make([]unix.EpollEvent, 128)
	eventQueue := &EventQueue{}
	for {
		select {
		case <-e.ShutdownCtx.Done():
			log.Println("Shutdown signal received, stopping epoll wait loop.")
			return
		default:
		}
		n, err := unix.EpollWait(e.Fd, events, epollWaitTimeout)
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
			e.Metrics.EpollErrors.Add(1)

			// Prevent busy-looping on persistent errors
			select {
			case <-time.After(100 * time.Millisecond):
			case <-e.ShutdownCtx.Done():
				log.Println("Shutdown signal received after epoll error.")
				return
			}
			continue

		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			job := EventJob{
				Fd:     int(ev.Fd),
				Events: ev.Events,
			}
			select {
			case e.WorkerChan <- job:
			case <-e.ShutdownCtx.Done():
				log.Println("Shutdown while dispatching epoll event, discarding job.")
				return
			default:
				eventQueue.enqueue(job)
				// log.Printf("WARNING: Worker channel full. Queuing event for FD %d (Events: 0x%x).", job.fd, job.events)
			}
		}
	}
}

func (ep *Epoll) HandleEvents(fd int, events uint32) {
	connVal, ok := ep.Connections.Load(fd)

	if !ok {
		// Connection might have been closed and removed between event generation and processing.
		_ = ep.Delete(fd)
		return
	}
	conn := connVal.(net.Conn)

	// Check for errors or hangup events first.
	// EPOLLRDHUP indicates the peer has closed their writing end or the whole connection.
	// EPOLLHUP often indicates an unexpected close or reset.
	// EPOLLERR indicates an error on the FD.
	if events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
		log.Printf("Handling EPOLLERR/EPOLLHUP for FD %d", fd)
		ep.Metrics.ReadErrors.Add(1)                                   // Count as a read/general error
		ep.DeleteAndClose(fd, conn, "Epoll error/hangup event", false) // Server closing due to error
		return
	}

	if events&unix.EPOLLRDHUP != 0 {
		log.Printf("Handling EPOLLRDHUP for FD %d (peer closed write end or connection)", fd)
		ep.DeleteAndClose(fd, conn, "Peer closed connection (RDHUP)", true) // Closed by peer
		return
	}

	if events&unix.EPOLLOUT != 0 {
	}

	if events&unix.EPOLLIN != 0 {
		ep.HandleRead(fd, conn)
	}

}

func (ep *Epoll) HandleRead(fd int, conn net.Conn) {
	// For edge-triggered mode, we must read until we get EAGAIN/EWOULDBLOCK
	// to ensure we process all available data notified by the edge trigger.
	for {
		if ep.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
				log.Printf("WARN: Failed to set read deadline for FD %d before read: %v", fd, err)
				ep.Metrics.ReadErrors.Add(1)
				ep.DeleteAndClose(fd, conn, "Failed to set read deadline", false)
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
				ep.Delete(fd)
				return
			} else if errors.Is(err, syscall.ECONNRESET) {
				reason = "Connection reset by peer"
				ep.Metrics.ReadErrors.Add(1)
				closedByPeer = true
			} else if errors.Is(err, syscall.EPIPE) {
				reason = "Broken pipe"
				ep.Metrics.ReadErrors.Add(1)
				closedByPeer = true
			} else if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				reason = "Read timeout"
				ep.Metrics.ReadErrors.Add(1)

			} else {

				reason = fmt.Sprintf("Unexpected read error: %v", err)
				log.Printf("ERROR: %s on FD %d", reason, fd)
				ep.Metrics.ReadErrors.Add(1)

			}

			log.Printf("Closing FD %d due to read issue: %s", fd, reason)
			ep.DeleteAndClose(fd, conn, reason, closedByPeer)
			return
		}
		ep.Metrics.MessagesReceived.Add(1)
		ep.Metrics.BytesReceived.Add(int64(len(msg)))
		if err := ep.ProcessMessage(fd, conn, msg, op); err != nil {
			log.Printf("ERROR: Failed to process message on FD %d: %v. Closing connection.", fd, err)
			ep.Metrics.ProcessingErrors.Add(1)
			ep.DeleteAndClose(fd, conn, fmt.Sprintf("Message processing failed: %v", err), false)
			return
		}

	}
}

func (ep *Epoll) ProcessMessage(fd int, conn net.Conn, msg []byte, op ws.OpCode) error {

	switch op {
	case ws.OpText, ws.OpBinary:
		if ep.WriteTimeout > 0 {

			if err := conn.SetWriteDeadline(time.Now().Add(ep.WriteTimeout)); err != nil {

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
		ep.Metrics.MessagesSent.Add(1)
		ep.Metrics.BytesSent.Add(int64(len(msg)))

	case ws.OpClose:
		log.Printf("Client initiated close on FD %d", fd)
		return errors.New("client initiated close")
	case ws.OpPing:
		if ep.WriteTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(ep.WriteTimeout)); err != nil {
				return fmt.Errorf("failed to set write deadline before pong: %w", err)
			}
		}
		if err := wsutil.WriteServerMessage(conn, ws.OpPong, nil); err != nil {
			return fmt.Errorf("failed to write pong: %w", err)
		}
		ep.Metrics.MessagesSent.Add(1)

	case ws.OpPong:
		ep.Metrics.PongsReceived.Add(1)

	default:
		log.Printf("WARN: Received unknown WebSocket opcode %v on FD %d", op, fd)
		return fmt.Errorf("unknown WebSocket opcode: %v", op)
	}

	return nil
}

func (ep *Epoll) StartConnectionHealthCheck(ctx context.Context, interval time.Duration) {
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
			ep.Connections.Range(func(key, value interface{}) bool {
				select {
				case <-ctx.Done(): // Check shutdown again inside loop
					return false // Stop iteration
				default:
				}

				fd := key.(int)
				conn := value.(net.Conn)

				if ep.WriteTimeout > 0 {
					deadline := time.Now().Add(ep.WriteTimeout)
					if err := conn.SetWriteDeadline(deadline); err != nil {
						log.Printf("WARN: Health check: Failed to set write deadline for FD %d: %v", fd, err)

					}
				}

				err := wsutil.WriteServerMessage(conn, ws.OpPing, nil)
				if err != nil {
					log.Printf("Health check: Failed to send Ping to FD %d: %v. Closing connection.", fd, err)
					ep.Metrics.WriteErrors.Add(1)

					ep.DeleteAndClose(fd, conn, fmt.Sprintf("Ping failed: %v", err), false) // Closed by server

					return true
				}
				ep.Metrics.PingsSent.Add(1)

				return true
			})
		}
	}
}
