// New code with the drivers simulation running in one container 
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync" 
	"syscall"
	"time"

	"fastsocket/grpc/trackingpb"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ip           = flag.String("ip", "127.0.0.1", "server IP")
	lat          = flag.Float64("lat", 9.34234, "starting lat")
	lng          = flag.Float64("lng", 38.234234, "starting lng")
	numClients   = flag.Int("n", 1000, "number of clients to simulate") // New flag for scaling
	mode         = flag.String("mode", "ws", "client mode: ws or grpc") // New flag to choose between WebSocket and gRPC the default is ws if u didn't specify the mode
)

func startDriver(driverID string, serverIP string, startLat, startLng float64, wg *sync.WaitGroup) {
	defer wg.Done()

	u := url.URL{Scheme: "ws", Host: serverIP + ":8082", Path: "/ws"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("Driver %s failed to connect: %v", driverID, err)
		return
	}
	defer c.Close()

	currentStep := 0
	totalSteps := 100
	endLat, endLng := 9.5124, 39.2288

	for {
		msg := struct {
			Id        string  `json:"id"`
			Lat       float64 `json:"lat"`
			Lng       float64 `json:"lng"`
			CompanyId string  `json:"company_id"`
		}{
			Id:        driverID,
			Lat:       startLat + (endLat-startLat)*float64(currentStep)/float64(totalSteps),
			Lng:       startLng + (endLng-startLng)*float64(currentStep)/float64(totalSteps),
			CompanyId: "beu",
		}

		jsonMsg, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, jsonMsg); err != nil {
			log.Printf("Driver %s lost connection", driverID)
			return
		}

		currentStep = (currentStep + 1) % totalSteps
		time.Sleep(time.Second * 2)
	}
}

func startGrpcDriver(driverID string, serverIP string, startLat, startLng float64, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := grpc.Dial(serverIP+":8090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Driver %s failed to connect: %v", driverID, err)
		return
	}
	defer conn.Close()

	client := trackingpb.NewDriverTrackerClient(conn)

	currentStep := 0
	totalSteps := 100
	endLat, endLng := 9.5124, 39.2288

	for {
		lat := startLat + (endLat-startLat)*float64(currentStep)/float64(totalSteps)
		lng := startLng + (endLng-startLng)*float64(currentStep)/float64(totalSteps)

		req := &trackingpb.DriverLocation{
			Id:        driverID,
			Lat:       lat,
			Lng:       lng,
			CompanyId: "beu",
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		_, err := client.PublishLocation(ctx, req)
		cancel()

		if err != nil {
			log.Printf("Driver %s lost connection: %v", driverID, err)
			return
		}

		currentStep = (currentStep + 1) % totalSteps
		time.Sleep(time.Second * 2)
	}
}

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, `Websockets client generator Example usage: ./client -ip=127.0.0.1 -n=1000`)
		flag.PrintDefaults()
	}
	flag.Parse()

	var rlimit syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	rlimit.Cur = rlimit.Max
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)

	log.Printf("Starting simulation for %d clients...", *numClients)

	var wg sync.WaitGroup

	
	for i := 1; i <= *numClients; i++ {
		wg.Add(1)
		driverID := fmt.Sprintf("driver_%d", i)
		if *mode == "grpc" {
			go startGrpcDriver(driverID, *ip, *lat, *lng, &wg)
		} else {
			go startDriver(driverID, *ip, *lat, *lng, &wg)
		}
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
}