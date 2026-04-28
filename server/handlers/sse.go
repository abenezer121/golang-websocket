package handlers

import (
	"encoding/json"
	"fastsocket/epoll"
	"fastsocket/models"
	"fastsocket/sse"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const sseHeartbeatInterval = 20 * time.Second

func SSEHandler(ep *epoll.Epoll) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		companyID := r.URL.Query().Get("company_id")
		if companyID == "" {
			http.Error(w, "missing company_id", http.StatusBadRequest)
			return
		}

		driverIDs := parseSSEDriverIDs(r)
		bbox, err := parseSSEBBox(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if bbox == nil && len(driverIDs) == 0 {
			http.Error(w, "provide either bbox params or at least one driver_id", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		sub := sse.NewSSESubscriber(w, flusher)

		registration, err := ep.RegisterSSESubscription(sub, companyID, bbox, driverIDs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		sub.OnClose = func() {
			ep.UnregisterSubscription(registration.ID)
			log.Printf("SSE cleaned up for subscription %s", registration.ID)
		}

		sub.Start(r.Context(), sseHeartbeatInterval)

		connectedMsg, err := json.Marshal(models.SocketResponse{
			Command: "connected",
			Status:  "ok",
			Message: "SSE connection established",
		})
		if err != nil {
			http.Error(w, "failed to initialize stream", http.StatusInternalServerError)
			_ = sub.Close()
			return
		}
		if err := sub.Send(connectedMsg); err != nil {
			_ = sub.Close()
			return
		}

		if err := ep.SendInitialSnapshot(registration); err != nil {
			_ = sendSSEError(sub, "failed to load initial snapshot")
			_ = sub.Close()
			return
		}

		<-r.Context().Done()
		_ = sub.Close()
	}
}

func sendSSEError(sub *sse.SSESubscriber, message string) error {
	payload, err := json.Marshal(models.SocketResponse{
		Command: "error",
		Status:  "error",
		Message: message,
	})
	if err != nil {
		return err
	}
	return sub.Send(payload)
}

func parseSSEDriverIDs(r *http.Request) []string {
	driverSet := make(map[string]struct{})
	for _, raw := range r.URL.Query()["driver_id"] {
		for _, part := range strings.Split(raw, ",") {
			driverID := strings.TrimSpace(part)
			if driverID == "" {
				continue
			}
			driverSet[driverID] = struct{}{}
		}
	}

	driverIDs := make([]string, 0, len(driverSet))
	for driverID := range driverSet {
		driverIDs = append(driverIDs, driverID)
	}
	return driverIDs
}

func parseSSEBBox(r *http.Request) (*epoll.BoundingBox, error) {
	query := r.URL.Query()
	minLatRaw := query.Get("min_lat")
	minLngRaw := query.Get("min_lng")
	maxLatRaw := query.Get("max_lat")
	maxLngRaw := query.Get("max_lng")

	if minLatRaw == "" && minLngRaw == "" && maxLatRaw == "" && maxLngRaw == "" {
		return nil, nil
	}

	minLat, err := parseFloatQuery(minLatRaw, "min_lat")
	if err != nil {
		return nil, err
	}
	minLng, err := parseFloatQuery(minLngRaw, "min_lng")
	if err != nil {
		return nil, err
	}
	maxLat, err := parseFloatQuery(maxLatRaw, "max_lat")
	if err != nil {
		return nil, err
	}
	maxLng, err := parseFloatQuery(maxLngRaw, "max_lng")
	if err != nil {
		return nil, err
	}

	return &epoll.BoundingBox{
		MinLat: minLat,
		MinLng: minLng,
		MaxLat: maxLat,
		MaxLng: maxLng,
	}, nil
}

func parseFloatQuery(raw, name string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}
