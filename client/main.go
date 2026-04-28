package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ip         = flag.String("ip", "127.0.0.1", "server IP")
	lat        = flag.Float64("lat", 9.05380, "starting lat for non-bbox mode")
	lng        = flag.Float64("lng", 38.76127, "starting lng for non-bbox mode")
	numClients = flag.Int("n", 1000, "number of clients to simulate")

	companyID = flag.String("company-id", "beu", "company id sent by simulated drivers")

	useBBox = flag.Bool("use-bbox", true, "spread simulated drivers across the configured bbox")
	minLat  = flag.Float64("min-lat", 9.035798, "minimum latitude for bbox placement")
	minLng  = flag.Float64("min-lng", 38.743267, "minimum longitude for bbox placement")
	maxLat  = flag.Float64("max-lat", 9.071798, "maximum latitude for bbox placement")
	maxLng  = flag.Float64("max-lng", 38.779267, "maximum longitude for bbox placement")
)

type movementBounds struct {
	minLat float64
	minLng float64
	maxLat float64
	maxLng float64
}

func startDriver(driverID string, serverIP string, startLat, startLng float64, bounds movementBounds, wg *sync.WaitGroup) {
	defer wg.Done()

	u := url.URL{Scheme: "ws", Host: serverIP + ":8082", Path: "/ws"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("Driver %s failed to connect: %v", driverID, err)
		return
	}
	defer c.Close()

	latAmplitude := (bounds.maxLat - bounds.minLat) / 6
	lngAmplitude := (bounds.maxLng - bounds.minLng) / 6
	if latAmplitude == 0 {
		latAmplitude = 0.002
	}
	if lngAmplitude == 0 {
		lngAmplitude = 0.002
	}

	step := 0
	for {
		phase := float64(step) / 8
		nextLat := clamp(startLat+math.Sin(phase)*latAmplitude, bounds.minLat, bounds.maxLat)
		nextLng := clamp(startLng+math.Cos(phase*0.8)*lngAmplitude, bounds.minLng, bounds.maxLng)

		msg := struct {
			Id        string  `json:"id"`
			Lat       float64 `json:"lat"`
			Lng       float64 `json:"lng"`
			CompanyId string  `json:"company_id"`
		}{
			Id:        driverID,
			Lat:       nextLat,
			Lng:       nextLng,
			CompanyId: *companyID,
		}

		jsonMsg, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, jsonMsg); err != nil {
			log.Printf("Driver %s lost connection", driverID)
			return
		}

		step++
		time.Sleep(2 * time.Second)
	}
}

func main() {
	flag.Usage = func() {
		io.WriteString(os.Stderr, "WebSocket client generator example usage:\n")
		io.WriteString(os.Stderr, "  go run . -ip=127.0.0.1 -n=50\n")
		io.WriteString(os.Stderr, "  go run . -ip=127.0.0.1 -n=50 -use-bbox=true -min-lat=9.035798 -min-lng=38.743267 -max-lat=9.071798 -max-lng=38.779267\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	var rlimit syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit)
	rlimit.Cur = rlimit.Max
	syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit)

	log.Printf("Starting simulation for %d clients...", *numClients)

	bounds := movementBounds{
		minLat: *lat - 0.02,
		minLng: *lng - 0.02,
		maxLat: *lat + 0.02,
		maxLng: *lng + 0.02,
	}
	if *useBBox {
		bounds = movementBounds{
			minLat: *minLat,
			minLng: *minLng,
			maxLat: *maxLat,
			maxLng: *maxLng,
		}
	}

	var wg sync.WaitGroup
	for i := 1; i <= *numClients; i++ {
		wg.Add(1)
		driverID := fmt.Sprintf("driver_%d", i)
		startLat, startLng := driverStartPosition(i-1, *numClients, bounds, *lat, *lng, *useBBox)
		go startDriver(driverID, *ip, startLat, startLng, bounds, &wg)
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
}

func driverStartPosition(index, total int, bounds movementBounds, fallbackLat, fallbackLng float64, spreadInBBox bool) (float64, float64) {
	if !spreadInBBox {
		return fallbackLat, fallbackLng
	}

	if total <= 1 {
		return (bounds.minLat + bounds.maxLat) / 2, (bounds.minLng + bounds.maxLng) / 2
	}

	cols := int(math.Ceil(math.Sqrt(float64(total))))
	rows := int(math.Ceil(float64(total) / float64(cols)))
	col := index % cols
	row := index / cols

	latStep := (bounds.maxLat - bounds.minLat) / float64(max(rows, 1))
	lngStep := (bounds.maxLng - bounds.minLng) / float64(max(cols, 1))

	startLat := bounds.minLat + latStep*(float64(row)+0.5)
	startLng := bounds.minLng + lngStep*(float64(col)+0.5)
	return clamp(startLat, bounds.minLat, bounds.maxLat), clamp(startLng, bounds.minLng, bounds.maxLng)
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}