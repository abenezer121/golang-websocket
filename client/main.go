package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ip  = flag.String("ip", "127.0.0.1", "server IP")
	lat = flag.Float64("lat", 9.34234, "starting lat")
	lng = flag.Float64("lng", 38.234234, "starting lng")
	id  = flag.String("id", "0", "server ID")
)

type connectionState struct {
	conn        *websocket.Conn
	currentStep int
}

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, `Websockets client generator Example usage: ./client -ip=127.0.0.1`)
		flag.PrintDefaults()
	}
	flag.Parse()

	// Set file descriptor limits
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

	u := url.URL{Scheme: "ws", Host: *ip + ":8082", Path: "/ws"}
	log.Printf("Connecting to %s", u.String())

	// Create single connection
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func() {
		c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		time.Sleep(time.Second)
		c.Close()
	}()

	state := &connectionState{
		conn:        c,
		currentStep: 0,
	}

	log.Printf("Connection established successfully")

	// Add message reader goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v", err)
				return
			}
			log.Printf("Received: %s", message)
		}
	}()

	startLat, startLng := *lat, *lng
	endLat, endLng := 9.5124, 39.2288 // Example destination coordinates
	totalSteps := 100

	for {
		log.Printf("Sending message (step %d)", state.currentStep)
		msg := struct {
			Id  string  `json:"id"`
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		}{
			Id:  *id,
			Lat: startLat + (endLat-startLat)*float64(state.currentStep)/float64(totalSteps),
			Lng: startLng + (endLng-startLng)*float64(state.currentStep)/float64(totalSteps),
		}

		// Increment step and reset if needed
		state.currentStep++
		if state.currentStep > totalSteps {
			state.currentStep = 0
		}

		// Marshal to JSON properly
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Failed to marshal message: %v", err)
			continue
		}

		if err := state.conn.WriteMessage(websocket.TextMessage, jsonMsg); err != nil {
			log.Printf("Failed to send message: %v", err)
			return
		} else {
			log.Printf("Sent: %s", jsonMsg)
		}

		time.Sleep(time.Second * 2)
	}
}
