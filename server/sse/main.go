package sse

import (
	"errors"
	"fastsocket/core"
	"fmt"
	"net/http"
	"sync"
)

type SSESubscriber struct {
	W       http.ResponseWriter
	Flusher http.Flusher
	mu      sync.Mutex
	closed  bool
	OnClose func()
}

func (s *SSESubscriber) Send(msg []byte) error {
	if s == nil {
		return errors.New("subscriber closed")
	}

	return s.writeFrame("data: %s\n\n", msg)
}

func (s *SSESubscriber) SendComment(comment string) error {
	if s == nil {
		return errors.New("subscriber closed")
	}

	return s.writeFrame(": %s\n\n", comment)
}

func (s *SSESubscriber) writeFrame(format string, payload interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.W == nil || s.Flusher == nil {
		return errors.New("subscriber closed")
	}

	if _, err := fmt.Fprintf(s.W, format, payload); err != nil {
		return err
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
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	cleanup = s.OnClose
	s.OnClose = nil
	s.W = nil
	s.Flusher = nil

	if cleanup != nil {
		s.mu.Unlock()
		cleanup()
		s.mu.Lock()
	}

	return nil
}

var _ core.Subscriber = (*SSESubscriber)(nil)
