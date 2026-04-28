package sse

import (
	"context"
	"errors"
	"fastsocket/core"
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
	W       http.ResponseWriter
	Flusher http.Flusher
	mu      sync.Mutex
	closed  bool
	done    chan struct{}
	queue   chan queuedFrame
	OnClose func()
}

func NewSSESubscriber(w http.ResponseWriter, flusher http.Flusher) *SSESubscriber {
	return &SSESubscriber{
		W:       w,
		Flusher: flusher,
		done:    make(chan struct{}),
		queue:   make(chan queuedFrame, defaultQueueSize),
	}
}

func (s *SSESubscriber) Send(msg []byte) error {
	if s == nil {
		return errors.New("subscriber closed")
	}

	copyMsg := append([]byte(nil), msg...)
	return s.enqueue(queuedFrame{payload: copyMsg})
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

var _ core.Subscriber = (*SSESubscriber)(nil)
