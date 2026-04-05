package transport

type ClientConnection interface {
	Send([]byte) error
	Close() error
	ID() string
}
