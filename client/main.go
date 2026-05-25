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
	"sync/atomic"
	"syscall"
	"time"

	"fastsocket/grpc/trackingpb"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	ip           = flag.String("ip", "127.0.0.1", "server IP")
	lat          = flag.Float64("lat", 9.34234, "starting lat")
	lng          = flag.Float64("lng", 38.234234, "starting lng")
	numClients   = flag.Int("n", 1000, "number of clients to simulate") // New flag for scaling
	mode         = flag.String("mode", "ws", "client mode: ws or grpc") // New flag to choose between WebSocket and gRPC the default is ws if u didn't specify the mode
	poolSize     = flag.Int("pool", 50, "number of gRPC connections in the pool") // added
)

type connPool struct {
	conns  []*grpc.ClientConn
	cursor atomic.Uint64
}

func newConnPool(addr string, size int) (*connPool, error) {
	
	if size <= 0 {
		return nil, fmt.Errorf("pool size must be > 0")
	}
	pool := &connPool{conns: make([]*grpc.ClientConn, size)}

	for i := range pool.conns {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
			grpc.WithInitialWindowSize(1<<20),
			grpc.WithInitialConnWindowSize(1<<22),
		)
		if err != nil {
			for _, c := range pool.conns[:i] {
				if c != nil {
					_ = c.Close()
				}
			}
			return nil, fmt.Errorf("pool conn %d: %w", i, err)
		}
		pool.conns[i] = conn
	}
	return pool, nil
}

func (p *connPool) get() *grpc.ClientConn {
	idx := p.cursor.Add(1) % uint64(len(p.conns))
	return p.conns[idx]
}

func (p *connPool) close() {
	for _, c := range p.conns {
		c.Close()
	}
}

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

func startGrpcDriver(
	ctx context.Context,
	driverID string,
	startLat, startLng float64,
	pool *connPool,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	client := trackingpb.NewDriverTrackerClient(pool.get())

	stream, err := client.PublishLocationStream(ctx)
	if err != nil {
		log.Printf("Driver %s failed to open stream: %v", driverID, err)
		return
	}

	currentStep := 0
	totalSteps := 100
	endLat, endLng := 9.5124, 39.2288
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_, _ = stream.CloseAndRecv()
			return

		case <-ticker.C:
			lat := startLat + (endLat-startLat)*float64(currentStep)/float64(totalSteps)
			lng := startLng + (endLng-startLng)*float64(currentStep)/float64(totalSteps)

			err := stream.Send(&trackingpb.DriverLocation{
				Id:        driverID,
				Lat:       &lat,
				Lng:       &lng,
				CompanyId: "beu",
			})

			if err != nil {
				log.Printf("Driver %s send error: %v", driverID, err)
				return
			}

			currentStep = (currentStep + 1) % totalSteps
		}
	}
}

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, `Websockets client generator Example usage: ./client -ip=127.0.0.1 -n=1000`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *mode != "ws" && *mode != "websocket" && *mode != "grpc" {
		log.Fatalf("Unsupported mode: %q. Allowed modes are: 'ws', 'websocket', 'grpc'", *mode)
	}

	var rlimit syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	rlimit.Cur = rlimit.Max
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)

	log.Printf("Starting simulation for %d clients...", *numClients)

	var wg sync.WaitGroup

	// create pool ONLY if grpc mode
	var pool *connPool
	var ctx context.Context
	var cancel context.CancelFunc

	if *mode == "grpc" {
		var err error
		pool, err = newConnPool(fmt.Sprintf("%s:8090", *ip), *poolSize)
		if err != nil {
			log.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.close()

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
	}

	for i := 1; i <= *numClients; i++ {
		wg.Add(1)
		driverID := fmt.Sprintf("driver_%d", i)
		if *mode == "grpc" {
			go startGrpcDriver(ctx, driverID, *lat, *lng, pool, &wg)
		} else {
			go startDriver(driverID, *ip, *lat, *lng, &wg)
		}
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
}