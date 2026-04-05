package transport

import (
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

func (c *WebSocketConnection) Send(payload []byte) error {
	return c.writer(c.conn, payload)
}

func (c *WebSocketConnection) Close() error {
	return c.conn.Close()
}

func (c *WebSocketConnection) ID() string {
	return c.id
}
