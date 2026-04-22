package tracker

import (
	"fastsocket/models"
	"testing"
)

type fakeConn struct {
	id       string
	messages []models.WatcherResponse
}

func (f *fakeConn) Send(payload models.WatcherResponse) error {
	f.messages = append(f.messages, payload)
	return nil
}

func (f *fakeConn) Close() error {
	return nil
}

func (f *fakeConn) ID() string {
	return f.id
}

func TestTrackDriverReplacesPreviousSubscription(t *testing.T) {
	service := NewService(nil)
	conn := &fakeConn{id: "watcher-1"}

	if err := service.TrackDriver(conn, "driver-a"); err != nil {
		t.Fatalf("track driver-a: %v", err)
	}
	if err := service.TrackDriver(conn, "driver-b"); err != nil {
		t.Fatalf("track driver-b: %v", err)
	}

	if got := service.driverTrackMap[conn.ID()]; got != "driver-b" {
		t.Fatalf("expected watcher to track driver-b, got %q", got)
	}
	if _, ok := service.notifyMap["driver-a"][conn.ID()]; ok {
		t.Fatalf("watcher should have been removed from previous driver subscription")
	}
	if _, ok := service.notifyMap["driver-b"][conn.ID()]; !ok {
		t.Fatalf("watcher should be present in the new driver subscription")
	}
}

func TestHandleWatcherCommandTrackDriverSendsAck(t *testing.T) {
	service := NewService(nil)
	conn := &fakeConn{id: "watcher-2"}
	commandType := "track-driver"
	driverID := "driver-42"

	err := service.HandleWatcherCommand(models.Command{
		CommandType: &commandType,
		DriverId:    &driverID,
	}, conn)
	if err != nil {
		t.Fatalf("handle watcher command: %v", err)
	}
	if len(conn.messages) != 1 {
		t.Fatalf("expected 1 ack message, got %d", len(conn.messages))
	}

	if got := conn.messages[0].Status; got != "now tracking driver_id driver-42" {
		t.Fatalf("unexpected ack status %q", got)
	}
}
