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

	if stream.events[0].GetDriverData().GetWorkerId() != "driver_1" {
		t.Fatalf("expected first event worker id driver_1, got %q", stream.events[0].GetDriverData().GetWorkerId())
	}
}
