package tracker

import (
	"fastsocket/transport"
	"fmt"
	"sync"
	"sync/atomic"
)

type BoundingBox struct {
	MinLat float64
	MinLng float64
	MaxLat float64
	MaxLng float64
}

func (b BoundingBox) Contains(lat, lng float64) bool {
	return lat >= b.MinLat && lat <= b.MaxLat && lng >= b.MinLng && lng <= b.MaxLng
}

type ClientSubscription struct {
	ID                   string
	Connection           transport.ClientConnection
	CompanyID            string
	BBox                 *BoundingBox
	ExplicitDriverIDs    map[string]struct{}
	CurrentBBoxDriverIDs map[string]struct{}
	mu                   sync.RWMutex
}

var subscriptionSeq atomic.Uint64

func nextSubscriptionID() string {
	return fmt.Sprintf("sub-%d", subscriptionSeq.Add(1))
}

func NewClientSubscription(conn transport.ClientConnection) *ClientSubscription {
	return &ClientSubscription{
		ID:                   nextSubscriptionID(),
		Connection:           conn,
		ExplicitDriverIDs:    make(map[string]struct{}),
		CurrentBBoxDriverIDs: make(map[string]struct{}),
	}
}

func (cs *ClientSubscription) AddExplicitDriver(driverID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.ExplicitDriverIDs[driverID] = struct{}{}
}

func (cs *ClientSubscription) RemoveExplicitDriver(driverID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.ExplicitDriverIDs, driverID)
}

func (cs *ClientSubscription) SetBBox(companyID string, bbox *BoundingBox) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.CompanyID = companyID
	cs.BBox = bbox
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) ClearBBox() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.BBox = nil
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) ClearAll() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.BBox = nil
	cs.CompanyID = ""
	cs.ExplicitDriverIDs = make(map[string]struct{})
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) HasExplicitDriver(driverID string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	_, ok := cs.ExplicitDriverIDs[driverID]
	return ok
}

func (cs *ClientSubscription) SnapshotDriverIDs() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	driverIDs := make([]string, 0, len(cs.ExplicitDriverIDs))
	for driverID := range cs.ExplicitDriverIDs {
		driverIDs = append(driverIDs, driverID)
	}
	return driverIDs
}

func (cs *ClientSubscription) Company() string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.CompanyID
}

func (cs *ClientSubscription) MarkBBoxMembership(driverID string, inside bool) (wasInside bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	_, wasInside = cs.CurrentBBoxDriverIDs[driverID]
	if inside {
		cs.CurrentBBoxDriverIDs[driverID] = struct{}{}
	} else {
		delete(cs.CurrentBBoxDriverIDs, driverID)
	}
	return wasInside
}
