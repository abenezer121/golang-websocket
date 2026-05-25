package transport

import (
	"fastsocket/models"
	"testing"

	"fastsocket/grpc/trackingpb"
)

type fakeGRPCWatcherStream struct {
	events []*trackingpb.WatcherEvent
}

func (f *fakeGRPCWatcherStream) Send(event *trackingpb.WatcherEvent) error {
	f.events = append(f.events, event)
	return nil
}

func TestGRPCWatcherConnectionSendPayloads(t *testing.T) {
	stream := &fakeGRPCWatcherStream{}
	conn := NewGRPCWatcherConnection(stream)

	if err := conn.Send(models.WatcherResponse{
		Command: "track",
		DriverUpdate: &models.LocationUpdate{
			WorkerID:  "driver_1",
			Latitude:  9.5,
			Longitude: 38.9,
			Timestamp: "t2",
			CompanyId: "beu",
			UnixTime:  "2",
		},
	}); err != nil {
		t.Fatalf("send watcher response: %v", err)
	}

	if len(stream.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(stream.events))
	}

	event := stream.events[0]
	if event.GetCommand() != "track" {
		t.Errorf("expected command %q, got %q", "track", event.GetCommand())
	}

	driverData := event.GetDriverData()
	if driverData == nil {
		t.Fatal("expected driver data to be non-nil")
	}

	if driverData.GetWorkerId() != "driver_1" {
		t.Errorf("expected worker id %q, got %q", "driver_1", driverData.GetWorkerId())
	}
	if driverData.GetLat() != 9.5 {
		t.Errorf("expected latitude %f, got %f", 9.5, driverData.GetLat())
	}
	if driverData.GetLng() != 38.9 {
		t.Errorf("expected longitude %f, got %f", 38.9, driverData.GetLng())
	}
	if driverData.GetTimestamp() != "t2" {
		t.Errorf("expected timestamp %q, got %q", "t2", driverData.GetTimestamp())
	}
	if driverData.GetCompanyId() != "beu" {
		t.Errorf("expected company id %q, got %q", "beu", driverData.GetCompanyId())
	}
	if driverData.GetUnixTime() != "2" {
		t.Errorf("expected unix time %q, got %q", "2", driverData.GetUnixTime())
	}
}
