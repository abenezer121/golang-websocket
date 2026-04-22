package transport

import (
	"encoding/json"
	"fastsocket/models"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWebSocketConnectionSendEncodesWatcherResponse(t *testing.T) {
	var payload []byte
	conn := NewWebSocketConnection("ws:test", nil, func(_ *websocket.Conn, msg []byte) error {
		payload = append([]byte(nil), msg...)
		return nil
	})

	lat := 9.5
	lng := 38.9
	if err := conn.Send(models.WatcherResponse{
		Command: "get-bbox",
		Drivers: []models.Command{
			{
				Id:  "driver_1",
				Lat: &lat,
				Lng: &lng,
			},
		},
	}); err != nil {
		t.Fatalf("send watcher response: %v", err)
	}

	var decoded struct {
		Command   string           `json:"command"`
		Paginated []models.Command `json:"paginated"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode websocket payload: %v", err)
	}

	if decoded.Command != "get-bbox" {
		t.Fatalf("expected command get-bbox, got %q", decoded.Command)
	}
	if len(decoded.Paginated) != 1 || decoded.Paginated[0].Id != "driver_1" {
		t.Fatalf("unexpected paginated payload: %+v", decoded.Paginated)
	}
}
