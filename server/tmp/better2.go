package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"syscall"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"golang.org/x/sys/unix"
)

type epoll struct {
	fd          int
	connections map[int]net.Conn
	lock        *sync.RWMutex
}

func newEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}

	return &epoll{
		fd:          fd,
		lock:        &sync.RWMutex{},
		connections: make(map[int]net.Conn),
	}, err
}

func (e *epoll) add(conn net.Conn) error {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	fd := int(pfdVal.FieldByName("Sysfd").Int())

	// add to epoll
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.POLLIN | unix.POLLHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	e.connections[fd] = conn

	fmt.Printf("New connection (total: %d)\n", len(e.connections))
	return nil
}

func (e *epoll) delete(conn net.Conn) error {

	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	fd := int(pfdVal.FieldByName("Sysfd").Int())

	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.connections, fd)

	fmt.Printf("Connection closed (total: %d)\n", len(e.connections))
	return nil
}

func (e *epoll) wait() ([]net.Conn, error) {
	events := make([]unix.EpollEvent, 32768)
	n, err := unix.EpollWait(e.fd, events, -1)
	if err != nil {
		return nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	var connections []net.Conn
	for i := 0; i < n; i++ {
		conn := e.connections[int(events[i].Fd)]
		connections = append(connections, conn)

	}
	return connections, nil
}

func processMessages(ep *epoll) {

	for {
		connections, err := ep.wait()
		if err != nil {

			fmt.Println("Epoll wait error:", err)
			return
		}

		for _, conn := range connections {
			if conn == nil {
				break
			}

			msg, _, err := wsutil.ReadClientData(conn)
			if err != nil {
				if err := ep.delete(conn); err != nil {
					log.Printf("Failed to remove %v", err)
				}

				conn.Close()
			} else {
				fmt.Println(msg)
			}
		}
	}
}

func handleWs(w http.ResponseWriter, r *http.Request, ep *epoll) {

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		if err.Error() == "too many open files" {
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
			return
		}
		fmt.Printf("Upgrade error: %v\n", err)
		return
	}

	err = ep.add(conn)
	if err != nil {
		fmt.Println("failed to add connection to epoll", err)
		conn.Close()

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
		// Use sysctl command-line tool
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

	// if err := setNetworkParameters(); err != nil {
	// 	log.Printf("Warning: Failed to set network parameters: %v", err)
	// 	// Continue execution as some parameters might require root privileges
	// }

	// increase the number of open file descriptors
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
	go processMessages(ep)
	http.HandleFunc("/ping-location", func(w http.ResponseWriter, r *http.Request) {
		handleWs(w, r, ep)
	})

	fmt.Println("Websocket server started :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

// // original 262144
// //echo 2621440 | sudo tee /proc/sys/net/netfilter/nf_conntrack_max

// // // ps aux | grep "go run server.go" | grep -v "grep" | awk '{print $6/1024 " MB"}'
// // sudo pkill -f "./server"
// // sudo  ./server
// // ps aux | grep "./server"
// //  ps -p 20009 -o %mem,rss | awk 'NR==2 {printf "Memory Usage: %.2f%% (%.2f MB)\n", $1, $2/1024}'
// // ps -p 20010 -o %mem,rss | awk 'NR==2 {printf "Memory Usage: %.2f%% (%.2f MB)\n", $1, $2/1024}'
// // ps -p 20011 -o %mem,rss | awk 'NR==2 {printf "Memory Usage: %.2f%% (%.2f MB)\n", $1, $2/1024}'
// // ps -p 20073 -o %mem,rss | awk 'NR==2 {printf "Memory Usage: %.2f%% (%.2f MB)\n", $1, $2/1024}'

// export PATH=$PATH:$(go env GOPATH)/bin
// protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ping.proto
