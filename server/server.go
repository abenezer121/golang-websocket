package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

func newEpoll(workerChan chan<- eventJob, shutdownCtx context.Context) (*epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("Canot create epoll in the kernel")
	}
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
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	// Set non-blocking mode
	if err := unix.SetNonblock(fd, true); err != nil {
		return fmt.Errorf("failed to set non-blocking: %w", err)
	}

	// Add to epoll with edge-triggered mode
	err = unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP | unix.EPOLLET,
		Fd:     int32(fd),
	})
	if err != nil {
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
		return fmt.Errorf("epoll ctl del error: %w", err)
	}

	e.connections.LoadAndDelete(fd)
	newCount := e.totalSocket.Add(-1)
	fmt.Println("Client deleted", "remaining_connections", newCount)

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
	return fd, nil
}

// ([]net.Conn, error)
func (e *epoll) wait() {
	events := make([]unix.EpollEvent, 100)

	for {
		select {
		case <-e.shutdownCtx.Done():
			fmt.Println("Epoll loop shutting down")
			return
		default:
		}

		n, err := unix.EpollWait(e.fd, events, 100) // Using -1 for timeout instead of 100

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
			fd := int(events[i].Fd)
			event := events[i].Events

			// Handle errors/disconnection
			if event&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
				e.delete(fd)
				continue
			}

			job := eventJob{
				fd:     fd,
				events: event,
			}

			select {
			case e.workerChan <- job:
				// fmt.Printf("Dispatched job to worker fd=%d events=0x%x\n", job.fd, job.events)
			case <-e.shutdownCtx.Done():
				return
			}
		}
	}
}
func wsHander(w http.ResponseWriter, r *http.Request, ep *epoll) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		if errors.Is(err, syscall.EMFILE) {
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
			return
		}
		fmt.Printf("Upgrade error: %v\n", err)
		return
	}
	if err := ep.add(conn); err != nil {
		fmt.Println("Failed to add connection to epoll:", err)
		conn.Close()
		return
	}
}

func startWorkers(n int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup) {
	fmt.Println("Starting worker pool ", " count ", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go workerFunc(i, ep, jobChan, wg)
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
	if events&unix.EPOLLERR != 0 || events&unix.EPOLLHUP != 0 || events&unix.EPOLLRDHUP != 0 {
		fmt.Println("Epoll error/hangup event received", "events", fmt.Sprintf("0x%x", events))
		ep.deleteAndClose(conn.(net.Conn))
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
		ep.handleRead(conn.(net.Conn))
		// if c.closed.Load() {
		// 	return
		// }
	}

}

func (ep *epoll) handleRead(conn net.Conn) {
	_, _, err := wsutil.ReadClientData(conn)

	if err != nil {
		// handle error
	}
	// fmt.Println("Msg: ", msg)
}

func workerFunc(id int, ep *epoll, jobChan <-chan eventJob, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("worker ", id, " started ")
	for job := range jobChan {
		ep.handleEvents(job.fd, job.events)

	}
	fmt.Println("Worker stopped")
}

func main() {
	flag.Parse()

	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		fmt.Println("Error getting rlimit:", err)
		return
	}
	fmt.Printf("Current limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	rlimit.Cur = rlimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		fmt.Println("Error setting rlimit:", err)
		return
	}
	fmt.Printf("New limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	jobChan := make(chan eventJob, *workers*2)
	shutdownCtx, cancelEpollLoop := context.WithCancel(context.Background())
	epoll, err := newEpoll(jobChan, shutdownCtx)
	if err != nil {
		fmt.Println("Failed to initialize epoll", "error", err)
		os.Exit(1)
	}

	workerWg := &sync.WaitGroup{}
	startWorkers(*workers, epoll, jobChan, workerWg)
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

}

// sudo sh -c "echo 2621440 > /proc/sys/net/netfilter/nf_conntrack_max"
