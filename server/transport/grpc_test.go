package transport

import (
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



	canonicalPayload := []byte(`{"command":"track","driver_data":{"worker_id":"driver_1","lat":9.5,"lng":38.9,"timestamp":"t2","company_id":"beu","unix_time":"2"}}`)
	if err := conn.Send(canonicalPayload); err != nil {
		t.Fatalf("send canonical payload: %v", err)
	}

	if len(stream.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(stream.events))
	}

	
	if stream.events[0].GetDriverData().GetWorkerId() != "driver_1" {
		t.Fatalf("expected first event worker id driver_1, got %q", stream.events[0].GetDriverData().GetWorkerId())
	}
}
