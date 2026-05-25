package transport

import (
	"encoding/json"
	"fastsocket/models"
	"fmt"
	"github.com/gorilla/websocket"
)

type WebSocketConnection struct {
	id     string
	conn   *websocket.Conn
	writer func(*websocket.Conn, []byte) error
}

func NewWebSocketConnection(id string, conn *websocket.Conn, writer func(*websocket.Conn, []byte) error) *WebSocketConnection {
	return &WebSocketConnection{
		id:     id,
		conn:   conn,
		writer: writer,
	}
}

func WebSocketConnectionID(fd int) string {
	return fmt.Sprintf("ws:%d", fd)
}

type websocketResponse struct {
	Command    string                 `json:"command,omitempty"`
	Paginated  []models.Command       `json:"paginated,omitempty"`
	DriverData *models.LocationUpdate `json:"driver_data,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Status     string                 `json:"status,omitempty"`
}

func (c *WebSocketConnection) Send(payload models.WatcherResponse) error {
	return c.sendJSON(websocketResponse{
		Command:    payload.Command,
		Paginated:  payload.Drivers,
		DriverData: payload.DriverUpdate,
		Error:      payload.Error,
		Status:     payload.Status,
	})
}

func (c *WebSocketConnection) sendJSON(payload any) error {
	msg, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return c.writer(c.conn, msg)
}

func (c *WebSocketConnection) Close() error {
	return c.conn.Close()
}

func (c *WebSocketConnection) ID() string {
	return c.id
}
