package epoll

import (
	"encoding/json"
	"fastsocket/core"
	"fastsocket/models"
	"fmt"
	"github.com/gorilla/websocket"
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
	Subscriber           core.Subscriber
	Conn                 *websocket.Conn
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

func newClientSubscription(subscriber core.Subscriber, conn *websocket.Conn) *ClientSubscription {
	return &ClientSubscription{
		ID:                   nextSubscriptionID(),
		Subscriber:           subscriber,
		Conn:                 conn,
		ExplicitDriverIDs:    make(map[string]struct{}),
		CurrentBBoxDriverIDs: make(map[string]struct{}),
	}
}

func (cs *ClientSubscription) addExplicitDriver(driverID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.ExplicitDriverIDs[driverID] = struct{}{}
}

func (cs *ClientSubscription) removeExplicitDriver(driverID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.ExplicitDriverIDs, driverID)
}

func (cs *ClientSubscription) setBBox(companyID string, bbox *BoundingBox) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.CompanyID = companyID
	cs.BBox = bbox
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) clearBBox() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.BBox = nil
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) clearAll() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.BBox = nil
	cs.CompanyID = ""
	cs.ExplicitDriverIDs = make(map[string]struct{})
	cs.CurrentBBoxDriverIDs = make(map[string]struct{})
}

func (cs *ClientSubscription) hasExplicitDriver(driverID string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	_, ok := cs.ExplicitDriverIDs[driverID]
	return ok
}

func (cs *ClientSubscription) snapshotDriverIDs() []string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	driverIDs := make([]string, 0, len(cs.ExplicitDriverIDs))
	for driverID := range cs.ExplicitDriverIDs {
		driverIDs = append(driverIDs, driverID)
	}
	return driverIDs
}

func (cs *ClientSubscription) companyID() string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.CompanyID
}

func (cs *ClientSubscription) markBBoxMembership(driverID string, inside bool) (wasInside bool) {
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

func sendJSON(sub core.Subscriber, payload interface{}) error {
	msg, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return sub.Send(msg)
}

func ackResponse(command, status, message string, driverIDs []string) models.SocketResponse {
	return models.SocketResponse{
		Command:   command,
		Status:    status,
		Message:   message,
		DriverIDs: driverIDs,
	}
}

func (ep *Epoll) UnregisterAllSSESubscribers() {
	ep.subscriptionsMu.Lock()
	toClose := make([]*ClientSubscription, 0)
	for id, sub := range ep.Subscriptions {
		if sub == nil || sub.Conn != nil {
			continue
		}
		toClose = append(toClose, sub)
		delete(ep.Subscriptions, id)

		sub.mu.RLock()
		companyID := sub.CompanyID
		driverIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
		for driverID := range sub.ExplicitDriverIDs {
			driverIDs = append(driverIDs, driverID)
		}
		sub.mu.RUnlock()

		for _, driverID := range driverIDs {
			subIDs, ok := ep.ExplicitDriverSubs[driverID]
			if !ok {
				continue
			}
			delete(subIDs, id)
			if len(subIDs) == 0 {
				delete(ep.ExplicitDriverSubs, driverID)
			}
		}
		if companyID != "" {
			ep.removeBBoxIndexLocked(companyID, id)
		}
	}
	ep.subscriptionsMu.Unlock()

	for _, sub := range toClose {
		_ = sub.Subscriber.Close()
	}
}
