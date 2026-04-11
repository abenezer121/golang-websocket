package handlers

import (
	"encoding/json"
	"fastsocket/epoll"
	"fastsocket/models"
	"fastsocket/sse"
	"log"
	"net/http"
	"time"
)

func SSEHandler(ep *epoll.Epoll) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		driverID := r.URL.Query().Get("driver_id")
		if driverID == "" {
			http.Error(w, "missing driver_id", http.StatusBadRequest)
			return
		}

		sub := &sse.SSESubscriber{
			W:       w,
			Flusher: flusher,
		}
		sub.OnClose = func() {
			ep.NotifyMapMutex.Lock()
			defer ep.NotifyMapMutex.Unlock()

			subscribers := ep.NotifyMap[driverID]
			for i, subscriber := range subscribers {
				if subscriber == sub {
					ep.NotifyMap[driverID] = append(subscribers[:i], subscribers[i+1:]...)
					break
				}
			}

			if len(ep.NotifyMap[driverID]) == 0 {
				delete(ep.NotifyMap, driverID)
			}

			log.Println("SSE cleaned up for:", driverID)
		}

		ep.NotifyMapMutex.Lock()
		ep.NotifyMap[driverID] = append(ep.NotifyMap[driverID], sub)
		ep.NotifyMapMutex.Unlock()

		log.Println("SSE subscriber added:", driverID)

		connectedMsg, err := json.Marshal(models.SocketResponse{Command: "connected"})
		if err != nil {
			http.Error(w, "failed to initialize stream", http.StatusInternalServerError)
			_ = sub.Close()
			return
		}
		if err := sub.Send(connectedMsg); err != nil {
			_ = sub.Close()
			return
		}

		go func() {
			ticker := time.NewTicker(20 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					if err := sub.SendComment("heartbeat"); err != nil {
						return
					}
				}
			}
		}()

		<-r.Context().Done()
		_ = sub.Close()
	}
}
