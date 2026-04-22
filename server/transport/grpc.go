package transport

import (
	"errors"
	"fastsocket/grpc/trackingpb"
	"fastsocket/models"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var grpcConnectionCounter atomic.Uint64

type GRPCWatcherStream interface {
	Send(*trackingpb.WatcherEvent) error
}

type GRPCWatcherConnection struct {
	id     string
	stream GRPCWatcherStream
	mu     sync.Mutex
}

func NewGRPCWatcherConnection(stream GRPCWatcherStream) *GRPCWatcherConnection {
	id := fmt.Sprintf("grpc:%d", grpcConnectionCounter.Add(1))
	return &GRPCWatcherConnection{
		id:     id,
		stream: stream,
	}
}

func (c *GRPCWatcherConnection) Send(payload models.WatcherResponse) error {
	event := &trackingpb.WatcherEvent{
		Command: payload.Command,
		Drivers: DriversToProto(payload.Drivers),
		Error:   payload.Error,
		Status:  payload.Status,
	}
	if payload.DriverUpdate != nil {
		event.DriverData = &trackingpb.DriverLocationUpdate{
			WorkerId:  payload.DriverUpdate.WorkerID,
			Lat:       payload.DriverUpdate.Latitude,
			Lng:       payload.DriverUpdate.Longitude,
			Timestamp: payload.DriverUpdate.Timestamp,
			CompanyId: payload.DriverUpdate.CompanyId,
			UnixTime:  payload.DriverUpdate.UnixTime,
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream.Send(event)
}

func (c *GRPCWatcherConnection) Close() error {
	return nil
}

func (c *GRPCWatcherConnection) ID() string {
	return c.id
}

func DriversToProto(drivers []models.Command) []*trackingpb.Driver {
	out := make([]*trackingpb.Driver, 0, len(drivers))
	for _, driver := range drivers {
		out = append(out, DriverToProto(driver))
	}
	return out
}

func DriverToProto(driver models.Command) *trackingpb.Driver {
	item := &trackingpb.Driver{
		Id:        driver.Id,
		From:      derefString(driver.From),
		Session:   derefString(driver.Session),
		CreatedAt: derefString(driver.CreatedAt),
		UpdatedAt: derefString(driver.UpdatedAt),
		LastSeen:  "",
		Active:    derefBool(driver.Active),
		CompanyId: driver.CompanyId,
	}
	if driver.Lat != nil {
		item.Lat = *driver.Lat
	}
	if driver.Lng != nil {
		item.Lng = *driver.Lng
	}
	if driver.LastSeen != nil {
		item.LastSeen = driver.LastSeen.Format(time.RFC3339)
	}
	if driver.DriverType != nil {
		item.DriverType = int32(*driver.DriverType)
	}
	return item
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func derefBool(value *bool) bool {
	if value == nil {
		return false
	}
	return *value
}

func ValidateBBox(minLat, minLng, maxLat, maxLng float64) error {
	if minLat >= maxLat || minLng >= maxLng {
		return errors.New("invalid bounding box coordinates")
	}
	return nil
}
