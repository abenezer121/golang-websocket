package transport
import "fastsocket/models"

type ClientConnection interface {
	Send(models.WatcherResponse) error
	Close() error
	ID() string
}
