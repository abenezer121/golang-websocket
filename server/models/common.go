package models

import (
	"sync"
	"sync/atomic"
	"time"
)

type EventJob struct {
	Fd     int
	Events uint32
}

type EventQueue struct {
	Mu    sync.Mutex
	Queue []EventJob
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

func (eq *EventQueue) enqueue(job EventJob) {
	eq.Mu.Lock()
	defer eq.Mu.Unlock()
	eq.Queue = append(eq.Queue, job)
}

func (eq *EventQueue) Dequeue() (EventJob, bool) {
	eq.Mu.Lock()
	defer eq.Mu.Unlock()
	if len(eq.Queue) == 0 {
		return EventJob{}, false
	}
	job := eq.Queue[0]
	eq.Queue = eq.Queue[1:]
	return job, true
}
