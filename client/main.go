package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	var connections int
	var serverHost string
	flag.IntVar(&connections, "connections", 50000, "Number of WebSocket connections")
	flag.StringVar(&serverHost, "host", "host.docker.internal", "WebSocket server host")
	flag.Parse()

	var conns []*websocket.Conn
	wsURL := fmt.Sprintf("ws://%s:8080/ping-location", serverHost)
	fmt.Println(connections)
	for i := 0; i < connections; i++ {
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			fmt.Println("Failed to connect", i, err)
			break
		}
		conns = append(conns, c)
		defer func() {
			c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
			time.Sleep(time.Second)
			c.Close()
		}()
	}

	log.Printf("Finished initializing %d connections", len(conns))

	for {
		for i := 0; i < len(conns); i++ {
			time.Sleep(time.Millisecond * 5)
			conn := conns[i]
			log.Printf("Conn %d sending message", i)
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second*5)); err != nil {
				fmt.Printf("Failed to receive pong: %v", err)
			}
			conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Hello from conn %v", i)))
		}
	}
}
