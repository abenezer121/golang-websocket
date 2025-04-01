package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ip          = flag.String("ip", "127.0.0.1", "server IP")
	connections = flag.Int("conn", 1, "number of websocket connections")
)

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, `Websockets client generator Example usage: ./client -ip=172.17.0.1 -conn=10`)
		flag.PrintDefaults()
	}
	flag.Parse()

	// Set file descriptor limits (same as original)
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		log.Fatalf("Error getting initial rlimit: %v", err)
	}
	log.Printf("Initial limits: Soft=%d, Hard=%d\n", rlimit.Cur, rlimit.Max)

	desiredLimit := rlimit
	desiredLimit.Cur = desiredLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
		log.Printf("!!! Error setting rlimit: %v !!!", err)
	}

	var actualLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &actualLimit); err != nil {
		log.Fatalf("Error getting rlimit AFTER setting attempt: %v", err)
	}
	log.Printf(">>> Actual limits AFTER Setrlimit call: Soft=%d, Hard=%d <<<\n", actualLimit.Cur, actualLimit.Max)

	u := url.URL{Scheme: "ws", Host: *ip + ":8080", Path: "/ws"}
	log.Printf("Connecting to %s", u.String())

	var conns []*websocket.Conn
	var wg sync.WaitGroup

	for i := 0; i < *connections; i++ {
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			fmt.Println("Failed to connect", i, err)
			break
		}
		conns = append(conns, c)
		defer func(conn *websocket.Conn) {
			conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
			time.Sleep(time.Second)
			conn.Close()
		}(c)
	}

	log.Printf("Finished initializing %d connections", len(conns))

	for {
		wg.Add(len(conns))

		startTime := time.Now()
		for i, conn := range conns {
			go func(conn *websocket.Conn, idx int) {
				defer wg.Done()

				log.Printf("Conn %d sending message", idx)

				if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Hello from conn %v", idx))); err != nil {
					fmt.Printf("Failed to send message on conn %d: %v", idx, err)
				}
			}(conn, i)
		}

		wg.Wait()
		log.Printf("All connections completed their sends in %v", time.Since(startTime))

		time.Sleep(time.Second * 2)
	}
}

// 9299
