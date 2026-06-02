package handlers

import (
	"fastsocket/models"
	"fastsocket/tracker"
	"fastsocket/transport"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const sseHeartbeatInterval = 20 * time.Second

var nextSSEID atomic.Uint64

func SSEHandler(trackerSvc *tracker.Service) http.HandlerFunc {
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

		sseConnID := fmt.Sprintf("sse:%d", nextSSEID.Add(1))
		sub := transport.NewSSESubscriber(sseConnID, w, flusher)

		registration, err := trackerSvc.RegisterSubscription(sub, companyID, bbox, driverIDs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		sub.OnClose = func() {
			trackerSvc.RemoveSubscription(registration.ID)
			log.Printf("SSE cleaned up for subscription %s", registration.ID)
		}

		sub.Start(r.Context(), sseHeartbeatInterval)

		err = sub.Send(models.WatcherResponse{
			Command: "connected",
			Status:  "ok",
			Message: "SSE connection established",
		})
		if err != nil {
			_ = sub.Close()
			return
		}

		if err := trackerSvc.SendInitialSnapshot(registration); err != nil {
			_ = sub.Send(models.WatcherResponse{
				Command: "error",
				Status:  "error",
				Error:   "failed to load initial snapshot",
			})
			_ = sub.Close()
			return
		}

		<-r.Context().Done()
		_ = sub.Close()
	}
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

func parseSSEBBox(r *http.Request) (*tracker.BoundingBox, error) {
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

	return &tracker.BoundingBox{
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
