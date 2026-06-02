package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/models"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const defaultQueueSize = 64

type queuedFrame struct {
	isComment bool
	payload   []byte
	comment   string
}

type SSESubscriber struct {
	id      string
	W       http.ResponseWriter
	Flusher http.Flusher
	mu      sync.Mutex
	closed  bool
	done    chan struct{}
	queue   chan queuedFrame
	OnClose func()
}

func NewSSESubscriber(id string, w http.ResponseWriter, flusher http.Flusher) *SSESubscriber {
	return &SSESubscriber{
		id:      id,
		W:       w,
		Flusher: flusher,
		done:    make(chan struct{}),
		queue:   make(chan queuedFrame, defaultQueueSize),
	}
}

func (s *SSESubscriber) ID() string {
	return s.id
}

func (s *SSESubscriber) Send(payload models.WatcherResponse) error {
	if s == nil {
		return errors.New("subscriber closed")
	}

	var message string
	if payload.Command == "error" {
		message = payload.Error
	} else {
		message = payload.Message
	}

	sseResp := struct {
		Command    string                 `json:"command"`
		Status     string                 `json:"status,omitempty"`
		Message    string                 `json:"message,omitempty"`
		DriverIDs  []string               `json:"driver_ids,omitempty"`
		Paginated  []models.Command       `json:"paginated,omitempty"`
		DriverData *models.LocationUpdate `json:"driver_data,omitempty"`
	}{
		Command:    payload.Command,
		Status:     payload.Status,
		Message:    message,
		DriverIDs:  payload.DriverIDs,
		Paginated:  payload.Drivers,
		DriverData: payload.DriverUpdate,
	}

	msg, err := json.Marshal(sseResp)
	if err != nil {
		return err
	}

	return s.enqueue(queuedFrame{payload: msg})
}

func (s *SSESubscriber) SendComment(comment string) error {
	if s == nil {
		return errors.New("subscriber closed")
	}

	return s.enqueue(queuedFrame{isComment: true, comment: comment})
}

func (s *SSESubscriber) Start(ctx context.Context, heartbeat time.Duration) {
	if s == nil {
		return
	}

	go func() {
		var ticker *time.Ticker
		var heartbeatC <-chan time.Time
		if heartbeat > 0 {
			ticker = time.NewTicker(heartbeat)
			heartbeatC = ticker.C
			defer ticker.Stop()
		}

		for {
			select {
			case <-ctx.Done():
				_ = s.Close()
				return
			case <-s.done:
				return
			case frame := <-s.queue:
				if err := s.writeFrame(frame); err != nil {
					_ = s.Close()
					return
				}
			case <-heartbeatC:
				if err := s.writeFrame(queuedFrame{isComment: true, comment: "heartbeat"}); err != nil {
					_ = s.Close()
					return
				}
			}
		}
	}()
}

func (s *SSESubscriber) enqueue(frame queuedFrame) error {
	s.mu.Lock()
	closed := s.closed
	done := s.done
	queue := s.queue
	s.mu.Unlock()

	if closed || done == nil || queue == nil {
		return errors.New("subscriber closed")
	}

	select {
	case <-done:
		return errors.New("subscriber closed")
	case queue <- frame:
		return nil
	default:
		return errors.New("subscriber send buffer full")
	}
}

func (s *SSESubscriber) writeFrame(frame queuedFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.W == nil || s.Flusher == nil {
		return errors.New("subscriber closed")
	}

	if frame.isComment {
		if _, err := fmt.Fprintf(s.W, ": %s\n\n", frame.comment); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(s.W, "data: %s\n\n", frame.payload); err != nil {
			return err
		}
	}
	s.Flusher.Flush()
	return nil
}

func (s *SSESubscriber) Close() error {
	if s == nil {
		return nil
	}

	var cleanup func()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true
	cleanup = s.OnClose
	s.OnClose = nil
	if s.done != nil {
		close(s.done)
	}
	s.W = nil
	s.Flusher = nil
	s.mu.Unlock()

	if cleanup != nil {
		cleanup()
	}

	return nil
}

var _ ClientConnection = (*SSESubscriber)(nil)
