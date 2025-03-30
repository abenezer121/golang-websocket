package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"golang.org/x/sys/unix"
)

type epoll struct {
	fd          int
	connections sync.Map
	connCount   int
	countLock   sync.Mutex
}

func newEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, fmt.Errorf("failed to create epoll: %w", err)
	}
	return &epoll{fd: fd}, nil
}

func (e *epoll) add(conn net.Conn) error {
	fd, err := getFd(conn)
	if err != nil {
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	err = unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.POLLIN | unix.POLLHUP,
		Fd:     int32(fd),
	})
	if err != nil {
		return fmt.Errorf("failed to add to epoll: %w", err)
	}

	e.connections.Store(fd, conn)
	e.incrementConnCount()
	fmt.Printf("New connection (total: %d)\n", e.getConnectionCount())
	return nil
}

func (e *epoll) delete(conn net.Conn) error {
	fd, err := getFd(conn)
	if err != nil {
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	if err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove from epoll: %w", err)
	}

	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	e.connections.Delete(fd)
	e.decrementConnCount()
	fmt.Printf("Connection closed (total: %d)\n", e.getConnectionCount())
	return nil
}

func (e *epoll) wait() ([]net.Conn, error) {
	events := make([]unix.EpollEvent, 100)

	for {
		n, err := unix.EpollWait(e.fd, events, 100)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {

				fmt.Println("EpollWait interrupted, retrying...")
				continue
			}
			return nil, fmt.Errorf("epoll wait error: %w", err)
		}

		var connections []net.Conn
		for i := 0; i < n; i++ {
			fd := int(events[i].Fd)
			if conn, ok := e.connections.Load(fd); ok {
				connections = append(connections, conn.(net.Conn))
			}
		}
		return connections, nil
	}
}

func (e *epoll) incrementConnCount() {
	e.countLock.Lock()
	defer e.countLock.Unlock()
	e.connCount++
}

func (e *epoll) decrementConnCount() {
	e.countLock.Lock()
	defer e.countLock.Unlock()
	e.connCount--
}

func (e *epoll) getConnectionCount() int {
	e.countLock.Lock()
	defer e.countLock.Unlock()
	return e.connCount
}

func getFd(conn net.Conn) (int, error) {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, fmt.Errorf("failed to get syscall.Conn")
	}

	rawConn, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("failed to get raw connection: %w", err)
	}

	var fd int
	rawConn.Control(func(fdPtr uintptr) {
		fd = int(fdPtr)
	})
	return fd, nil
}

func processMessages(ep *epoll, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down message processor...")
			return
		default:
			connections, err := ep.wait()
			if err != nil {
				fmt.Println("Epoll wait error:", err)
				continue
			}

			for _, conn := range connections {
				if conn == nil {
					break
				}

				_, _, err := wsutil.ReadClientData(conn)
				if err != nil {
					if err := ep.delete(conn); err != nil {
						log.Printf("Failed to remove connection: %v", err)
					}
					conn.Close()
				} else {
					// fmt.Println(string(msg))
				}
			}
		}
	}
}

func handleWs(w http.ResponseWriter, r *http.Request, ep *epoll) {
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

func setNetworkParameters() error {
	settings := map[string]string{
		"net.ipv4.ip_local_port_range": "1024 65535",
		"net.ipv4.tcp_mem":             "386534 515379 773068",
		"net.ipv4.tcp_rmem":            "4096 87380 6291456",
		"net.ipv4.tcp_wmem":            "4096 87380 6291456",
		"net.core.somaxconn":           "65535",
		"net.ipv4.tcp_max_syn_backlog": "65535",
	}

	for setting, value := range settings {

		cmd := exec.Command("sysctl", "-w", fmt.Sprintf("%s=%s", setting, value))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to set %s: %v", setting, err)
		}
		fmt.Printf("Successfully set %s = %s\n", setting, value)
	}
	return nil
}

func main() {
	setNetworkParameters()

	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Current limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	rlimit.Cur = rlimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("New limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	ep, err := newEpoll()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer unix.Close(ep.fd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go processMessages(ep, ctx)

	http.HandleFunc("/ping-location", func(w http.ResponseWriter, r *http.Request) {
		handleWs(w, r, ep)
	})

	srv := &http.Server{Addr: ":8080"}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down server...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
}
