package models

import (
	"flag"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	Addr             = flag.String("addr", ":8082", "WebSocket service address (e.g., :8080)")
	GRPCAddr         = flag.String("grpcAddr", ":8090", "gRPC service address (e.g., :8090)")
	MetricsAddr      = flag.String("metaddr", ":8089", "WebSocket service address (e.g., :8080)")
	Workers          = flag.Int("workers", runtime.NumCPU()*2, "Number of worker goroutines")
	ReadBufSize      = flag.Int("readBuf", 4096, "Read buffer size per connection")
	WriteBufSize     = flag.Int("writeBuf", 4096, "Write buffer size per connection")
	ReadTimeout      = flag.Duration("readTimeout", 60*time.Second, " Read timeout")
	WriteTimeout     = flag.Duration("writeTimeout", 10*time.Second, "write timeout")
	PingInterval     = flag.Duration("pingInterval", 30*time.Second, "ping interval")
	EpollWaitTimeout = 100
)

type EventJob struct {
	Fd     int
	Events uint32
}


type Metrics struct {
	StartTime                 time.Time
	UpgradesSuccess           atomic.Int64
	UpgradesFailed            atomic.Int64
	CurrentConnections        atomic.Int64
	TotalConnections          atomic.Int64
	ConnectionsClosed         atomic.Int64
	ConnectionsClosedByPeer   atomic.Int64
	ConnectionsClosedByServer atomic.Int64
	MessagesReceived          atomic.Int64
	MessagesSent              atomic.Int64
	BytesReceived             atomic.Int64
	BytesSent                 atomic.Int64
	PingsSent                 atomic.Int64
	PongsReceived             atomic.Int64
	EpollErrors               atomic.Int64
	ReadErrors                atomic.Int64
	WriteErrors               atomic.Int64
	ProcessingErrors          atomic.Int64
}

