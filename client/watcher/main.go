package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ip       = flag.String("ip", "127.0.0.1", "server IP")
	port     = flag.String("port", "8082", "server port")
	path     = flag.String("path", "/activity", "websocket path for watcher commands")
	command  = flag.String("command", "track-driver", "watcher command: track-driver|get-drivers|get-bbox")
	driverID = flag.String("driver-id", "driver_1", "driver ID for track-driver")
	page     = flag.Int("page", 1, "page number for get-drivers")
	minLat   = flag.Float64("min-lat", 9.30, "minimum latitude for get-bbox")
	minLng   = flag.Float64("min-lng", 38.20, "minimum longitude for get-bbox")
	maxLat   = flag.Float64("max-lat", 9.60, "maximum latitude for get-bbox")
	maxLng   = flag.Float64("max-lng", 39.30, "maximum longitude for get-bbox")
)

func buildWatcherMessage() map[string]interface{} {
	switch *command {
	case "get-drivers":
		return map[string]interface{}{
			"command_type": "get-drivers",
			"page":         *page,
		}
	case "get-bbox":
		return map[string]interface{}{
			"command_type": "get-bbox",
			"min_lat":      *minLat,
			"min_lng":      *minLng,
			"max_lat":      *maxLat,
			"max_lng":      *maxLng,
		}
	default:
		return map[string]interface{}{
			"command_type": "track-driver",
			"driver_id":    *driverID,
		}
	}
}

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, "Watcher simulator example usage:\n")
		io.WriteString(os.Stderr, "  go run ./watcher -ip=127.0.0.1 -path=/activity -command=track-driver -driver-id=driver_1\n")
		io.WriteString(os.Stderr, "  go run ./watcher -ip=127.0.0.1 -path=/activity -command=get-drivers -page=1\n")
		io.WriteString(os.Stderr, "  go run ./watcher -ip=127.0.0.1 -path=/activity -command=get-bbox -min-lat=9.3 -min-lng=38.2 -max-lat=9.6 -max-lng=39.3\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *ip + ":" + *port, Path: *path}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Watcher failed to connect: %v", err)
	}
	defer conn.Close()

	msg := buildWatcherMessage()
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Watcher failed to marshal command: %v", err)
	}

	if err := conn.WriteJSON(msg); err != nil {
		log.Fatalf("Watcher failed to send command: %v", err)
	}

	log.Printf("Connected to %s and sent payload: %s", u.String(), string(msgJSON))

	if *command != "track-driver" {
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			log.Fatalf("Watcher failed to set read deadline: %v", err)
		}
	}

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("Watcher lost connection: %v", err)
		}

		log.Printf("Received message type=%d payload=%s", messageType, string(payload))

		if *command != "track-driver" {
			return
		}
	}
}
