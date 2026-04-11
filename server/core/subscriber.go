package core

type Subscriber interface {
    Send([]byte) error
    Close() error
}