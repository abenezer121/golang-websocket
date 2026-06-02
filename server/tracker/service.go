package tracker

import (
	"context"
	"encoding/json"
	"errors"
	redisstore "fastsocket/external/redis"
	"fastsocket/models"
	"fastsocket/transport"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"fastsocket/config"
)

type Service struct {
	redis *redis.Client

	mu             sync.RWMutex
	notifyMap      map[string]map[string]transport.ClientConnection
	driverTrackMap map[string]string

	subscriptionsMu       sync.RWMutex
	subscriptions         map[string]*ClientSubscription
	connSubscriptionIDs   map[string]string
	explicitDriverSubs    map[string]map[string]struct{}
	bboxSubscriptionsByCo map[string]map[string]struct{}
}

func NewService(rd *redis.Client) *Service {
	return &Service{
		redis:                 rd,
		notifyMap:             make(map[string]map[string]transport.ClientConnection),
		driverTrackMap:        make(map[string]string),
		subscriptions:         make(map[string]*ClientSubscription),
		connSubscriptionIDs:   make(map[string]string),
		explicitDriverSubs:    make(map[string]map[string]struct{}),
		bboxSubscriptionsByCo: make(map[string]map[string]struct{}),
	}
}

func (s *Service) ProcessDriverUpdate(cmd models.Command) error {
	if cmd.Id == "" {
		return errors.New("driver update requires id")
	}
	if cmd.Lat == nil || cmd.Lng == nil {
		return errors.New("driver update requires lat and lng")
	}

	return s.UpdateWorkerLocation(cmd.Id, *cmd.Lat, *cmd.Lng, cmd.CompanyId)
}

func (s *Service) UpdateWorkerLocation(workerID string, lat, lng float64, companyID string) error {
	ctx := context.Background()

	now := time.Now()
	nowStr := now.Format(time.RFC3339)
	nowUnixStr := strconv.FormatInt(now.Unix(), 10)

	pipe := s.redis.Pipeline()
	pipe.GeoAdd(ctx, config.WorkerLocationSet, &redis.GeoLocation{
		Name:      workerID,
		Latitude:  lat,
		Longitude: lng,
	})

	var workerToStore models.Command
	existingData, err := s.redis.HGet(ctx, config.WorkerDetailsHash, workerID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("Error fetching existing worker details for %s: %v\n", workerID, err)
		return fmt.Errorf("failed to fetch existing worker details: %w", err)
	}

	if errors.Is(err, redis.Nil) {
		active := true
		workerToStore = models.Command{
			Id:        workerID,
			Lat:       &lat,
			Lng:       &lng,
			CompanyId: companyID,
			CreatedAt: &nowStr,
			UpdatedAt: &nowStr,
			Active:    &active,
			LastSeen:  &now,
		}
	} else {
		var existingWorker models.Command
		if err := json.Unmarshal([]byte(existingData), &existingWorker); err != nil {
			log.Printf("Error unmarshalling existing worker data for %s: %v\n", workerID, err)
			active := true
			workerToStore = models.Command{
				Id:        workerID,
				Lat:       &lat,
				Lng:       &lng,
				CompanyId: companyID,
				CreatedAt: &nowStr,
				UpdatedAt: &nowStr,
				Active:    &active,
				LastSeen:  &now,
			}
		} else {
			active := true
			workerToStore = existingWorker
			workerToStore.Lat = &lat
			workerToStore.Lng = &lng
			if companyID != "" {
				workerToStore.CompanyId = companyID
			}
			workerToStore.UpdatedAt = &nowStr
			workerToStore.Active = &active
			workerToStore.LastSeen = &now
		}
	}

	workerJSON, err := json.Marshal(workerToStore)
	if err != nil {
		log.Printf("Error marshalling worker details for %s: %v\n", workerID, err)
		return fmt.Errorf("failed to marshal worker details: %w", err)
	}

	pipe.HSet(ctx, config.WorkerDetailsHash, workerID, string(workerJSON))

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Error executing Redis pipeline for UpdateWorkerLocation (Worker: %s): %v\n", workerID, err)
		return fmt.Errorf("redis pipeline execution failed: %w", err)
	}

	subscribers := s.subscribersFor(workerID)
	update := models.LocationUpdate{
		WorkerID:  workerID,
		Latitude:  lat,
		Longitude: lng,
		Timestamp: nowStr,
		UnixTime:  nowUnixStr,
		CompanyId: workerToStore.CompanyId,
	}

	if len(subscribers) > 0 {
		response := models.WatcherResponse{
			Command:      "track",
			DriverUpdate: &update,
		}

		brokenIDs := make([]string, 0)
		for _, conn := range subscribers {
			if err := conn.Send(response); err != nil {
				log.Printf("Failed to write to connection for worker %s: %v", workerID, err)
				_ = conn.Close()
				brokenIDs = append(brokenIDs, conn.ID())
			}
		}

		if len(brokenIDs) > 0 {
			s.removeSubscribers(workerID, brokenIDs)
		}
	}

	s.dispatchToSubscriptions(update)

	return nil
}

func (s *Service) HandleWatcherCommand(decodedMsg models.Command, conn transport.ClientConnection) error {
	if decodedMsg.CommandType == nil {
		return errors.New("missing command_type")
	}

	switch *decodedMsg.CommandType {
	case "get-bbox":
		if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil {
			return s.sendError(conn, "get-bbox command requires min_lat, min_lng, max_lat, max_lng")
		}

		if err := transport.ValidateBBox(*decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng); err != nil {
			return s.sendError(conn, err.Error())
		}

		var response models.WatcherResponse
		var err error
		if decodedMsg.CompanyId != "" {
			var drivers []models.Command
			drivers, err = redisstore.FindWorkersInBBoxByCompany(s.redis, *decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng, decodedMsg.CompanyId)
			response = models.WatcherResponse{
				Command: "get-bbox",
				Drivers: drivers,
			}
		} else {
			response, err = s.bboxResponse(*decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng)
		}
		if err != nil {
			log.Printf("ERROR: 'get-bbox' failed: %v", err)
			return s.sendError(conn, "Failed to retrieve data for bounding box")
		}

		return s.send(conn, response)

	case "get-drivers":
		if decodedMsg.Page == nil {
			return s.sendError(conn, "get-drivers command requires a page number")
		}

		var response models.WatcherResponse
		var err error
		if decodedMsg.CompanyId != "" {
			var drivers []models.Command
			drivers, _, err = redisstore.GetAllWorkersPaginatedByCompany(s.redis, *decodedMsg.Page, 100, decodedMsg.CompanyId)
			response = models.WatcherResponse{
				Command: "get-drivers",
				Drivers: drivers,
			}
		} else {
			response, err = s.driversResponse(*decodedMsg.Page, 100)
		}
		if err != nil {
			log.Printf("ERROR: 'get-drivers' failed: %v", err)
			return s.sendError(conn, "Failed to retrieve drivers list")
		}

		return s.send(conn, response)

	case "track-driver":
		if decodedMsg.DriverId == nil || *decodedMsg.DriverId == "" {
			return s.sendError(conn, "track-driver command requires a valid driver_id")
		}

		return s.TrackDriver(conn, *decodedMsg.DriverId)

	case "subscribe-driver":
		driverIDs := normalizeDriverIDs(decodedMsg)
		if len(driverIDs) == 0 || decodedMsg.CompanyId == "" {
			return s.sendError(conn, "subscribe-driver requires company_id and at least one driver_id")
		}
		sub := s.getOrCreateSubscription(conn, decodedMsg.CompanyId)
		if sub.Company() != decodedMsg.CompanyId {
			return s.sendError(conn, fmt.Sprintf("subscription is already scoped to company_id %s", sub.Company()))
		}
		s.AddExplicitDrivers(sub, driverIDs)
		if err := s.sendExplicitDriverSnapshot(sub, driverIDs); err != nil {
			log.Printf("ERROR: failed to send driver snapshot: %v", err)
			return err
		}
		return s.send(conn, models.WatcherResponse{
			Command:   "subscribe-driver",
			Status:    "ok",
			Message:   "driver subscription updated",
			DriverIDs: driverIDs,
		})

	case "untrack-driver", "unsubscribe-driver":
		driverIDs := normalizeDriverIDs(decodedMsg)
		if len(driverIDs) == 0 {
			return s.sendError(conn, "untrack-driver command requires at least one driver_id")
		}
		s.subscriptionsMu.Lock()
		subID, exists := s.connSubscriptionIDs[conn.ID()]
		s.subscriptionsMu.Unlock()
		if exists {
			s.subscriptionsMu.RLock()
			sub := s.subscriptions[subID]
			s.subscriptionsMu.RUnlock()
			if sub != nil {
				s.RemoveExplicitDrivers(sub, driverIDs)
			}
		}
		return s.send(conn, models.WatcherResponse{
			Command:   *decodedMsg.CommandType,
			Status:    "ok",
			Message:   "driver subscription removed",
			DriverIDs: driverIDs,
		})

	case "subscribe-bbox", "update-bbox":
		if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil || decodedMsg.CompanyId == "" {
			return s.sendError(conn, "subscribe-bbox command requires company_id, min_lat, min_lng, max_lat, max_lng")
		}
		bbox := &BoundingBox{
			MinLat: *decodedMsg.MinLat,
			MinLng: *decodedMsg.MinLng,
			MaxLat: *decodedMsg.MaxLat,
			MaxLng: *decodedMsg.MaxLng,
		}
		sub := s.getOrCreateSubscription(conn, decodedMsg.CompanyId)
		if err := s.setBBoxSubscription(sub, decodedMsg.CompanyId, bbox); err != nil {
			return s.sendError(conn, err.Error())
		}
		if err := s.SendInitialSnapshot(sub); err != nil {
			log.Printf("ERROR: failed to send bbox snapshot: %v", err)
			return err
		}
		return s.send(conn, models.WatcherResponse{
			Command: *decodedMsg.CommandType,
			Status:  "ok",
			Message: "bbox subscription updated",
		})

	case "clear-subscriptions":
		s.subscriptionsMu.Lock()
		subID, exists := s.connSubscriptionIDs[conn.ID()]
		s.subscriptionsMu.Unlock()
		if exists {
			s.subscriptionsMu.RLock()
			sub := s.subscriptions[subID]
			s.subscriptionsMu.RUnlock()
			if sub != nil {
				s.clearSubscription(sub)
			}
		}
		return s.send(conn, models.WatcherResponse{
			Command: "clear-subscriptions",
			Status:  "ok",
			Message: "all subscriptions cleared",
		})

	default:
		return s.sendError(conn, fmt.Sprintf("Unknown command_type: %s", *decodedMsg.CommandType))
	}
}

func (s *Service) GetDrivers(page, pageSize int) ([]models.Command, int, error) {
	return redisstore.GetAllWorkersPaginated(s.redis, page, pageSize)
}

func (s *Service) GetDriversInBBox(minLat, minLng, maxLat, maxLng float64) ([]models.Command, error) {
	return redisstore.FindWorkersInBBox(s.redis, minLat, minLng, maxLat, maxLng)
}

func (s *Service) bboxResponse(minLat, minLng, maxLat, maxLng float64) (models.WatcherResponse, error) {
	drivers, err := s.GetDriversInBBox(minLat, minLng, maxLat, maxLng)
	if err != nil {
		return models.WatcherResponse{}, err
	}

	return models.WatcherResponse{
		Command: "get-bbox",
		Drivers: drivers,
	}, nil
}

func (s *Service) driversResponse(page, pageSize int) (models.WatcherResponse, error) {
	drivers, _, err := s.GetDrivers(page, pageSize)
	if err != nil {
		return models.WatcherResponse{}, err
	}

	return models.WatcherResponse{
		Command: "get-drivers",
		Drivers: drivers,
	}, nil
}

func (s *Service) TrackDriver(conn transport.ClientConnection, driverID string) error {
	if conn == nil {
		return errors.New("nil connection")
	}
	if driverID == "" {
		return errors.New("driver_id is required")
	}

	s.mu.Lock()
	if oldDriverID, tracked := s.driverTrackMap[conn.ID()]; tracked && oldDriverID != driverID {
		if watchers := s.notifyMap[oldDriverID]; watchers != nil {
			delete(watchers, conn.ID())
			if len(watchers) == 0 {
				delete(s.notifyMap, oldDriverID)
			}
		}
	}

	if s.notifyMap[driverID] == nil {
		s.notifyMap[driverID] = make(map[string]transport.ClientConnection)
	}
	s.notifyMap[driverID][conn.ID()] = conn
	s.driverTrackMap[conn.ID()] = driverID
	s.mu.Unlock()

	return s.sendStatus(conn, fmt.Sprintf("now tracking driver_id %s", driverID))
}

func (s *Service) RemoveConnection(connID string) {
	s.mu.Lock()
	driverID, ok := s.driverTrackMap[connID]
	if ok {
		delete(s.driverTrackMap, connID)
		if watchers := s.notifyMap[driverID]; watchers != nil {
			delete(watchers, connID)
			if len(watchers) == 0 {
				delete(s.notifyMap, driverID)
			}
		}
	}

	for trackedDriverID, watchers := range s.notifyMap {
		if _, exists := watchers[connID]; !exists {
			continue
		}
		delete(watchers, connID)
		if len(watchers) == 0 {
			delete(s.notifyMap, trackedDriverID)
		}
	}
	s.mu.Unlock()

	s.subscriptionsMu.Lock()
	subID, exists := s.connSubscriptionIDs[connID]
	s.subscriptionsMu.Unlock()
	if exists {
		s.RemoveSubscription(subID)
	}
}

func (s *Service) subscribersFor(driverID string) []transport.ClientConnection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	watchers := s.notifyMap[driverID]
	if len(watchers) == 0 {
		return nil
	}

	out := make([]transport.ClientConnection, 0, len(watchers))
	for _, conn := range watchers {
		out = append(out, conn)
	}
	return out
}

func (s *Service) removeSubscribers(driverID string, connIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	watchers := s.notifyMap[driverID]
	if len(watchers) == 0 {
		return
	}

	for _, connID := range connIDs {
		delete(watchers, connID)
		delete(s.driverTrackMap, connID)
	}

	if len(watchers) == 0 {
		delete(s.notifyMap, driverID)
	}
}

func (s *Service) send(conn transport.ClientConnection, payload models.WatcherResponse) error {
	if err := conn.Send(payload); err != nil {
		s.RemoveConnection(conn.ID())
		_ = conn.Close()
		return err
	}

	return nil
}

func (s *Service) sendError(conn transport.ClientConnection, message string) error {
	return s.send(conn, models.WatcherResponse{
		Command: "error",
		Status:  "error",
		Error:   message,
	})
}

func (s *Service) sendStatus(conn transport.ClientConnection, message string) error {
	return s.send(conn, models.WatcherResponse{Status: message})
}

func (s *Service) getOrCreateSubscription(conn transport.ClientConnection, companyID string) *ClientSubscription {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	if subID, ok := s.connSubscriptionIDs[conn.ID()]; ok {
		return s.subscriptions[subID]
	}

	sub := NewClientSubscription(conn)
	sub.CompanyID = companyID
	s.subscriptions[sub.ID] = sub
	s.connSubscriptionIDs[conn.ID()] = sub.ID
	return sub
}

func (s *Service) RegisterSubscription(conn transport.ClientConnection, companyID string, bbox *BoundingBox, driverIDs []string) (*ClientSubscription, error) {
	if companyID == "" {
		return nil, errors.New("company_id is required")
	}

	sub := NewClientSubscription(conn)
	sub.CompanyID = companyID

	s.subscriptionsMu.Lock()
	s.subscriptions[sub.ID] = sub
	s.connSubscriptionIDs[conn.ID()] = sub.ID
	if bbox != nil {
		sub.BBox = bbox
		s.addBBoxIndexLocked(companyID, sub.ID)
	}
	s.subscriptionsMu.Unlock()

	if len(driverIDs) > 0 {
		s.AddExplicitDrivers(sub, driverIDs)
	}

	return sub, nil
}

func (s *Service) RemoveSubscription(id string) {
	sub := s.removeSubscriptionLocked(id)
	if sub != nil {
		_ = sub.Connection.Close()
	}
}

func (s *Service) removeSubscriptionLocked(id string) *ClientSubscription {
	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()

	sub, ok := s.subscriptions[id]
	if !ok {
		return nil
	}

	delete(s.subscriptions, id)
	delete(s.connSubscriptionIDs, sub.Connection.ID())

	sub.mu.RLock()
	companyID := sub.CompanyID
	driverIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
	for driverID := range sub.ExplicitDriverIDs {
		driverIDs = append(driverIDs, driverID)
	}
	sub.mu.RUnlock()

	for _, driverID := range driverIDs {
		subIDs, ok := s.explicitDriverSubs[driverID]
		if !ok {
			continue
		}
		delete(subIDs, id)
		if len(subIDs) == 0 {
			delete(s.explicitDriverSubs, driverID)
		}
	}

	if companyID != "" {
		s.removeBBoxIndexLocked(companyID, id)
	}

	return sub
}

func (s *Service) addBBoxIndexLocked(companyID, subID string) {
	if _, ok := s.bboxSubscriptionsByCo[companyID]; !ok {
		s.bboxSubscriptionsByCo[companyID] = make(map[string]struct{})
	}
	s.bboxSubscriptionsByCo[companyID][subID] = struct{}{}
}

func (s *Service) removeBBoxIndexLocked(companyID, subID string) {
	subIDs, ok := s.bboxSubscriptionsByCo[companyID]
	if !ok {
		return
	}
	delete(subIDs, subID)
	if len(subIDs) == 0 {
		delete(s.bboxSubscriptionsByCo, companyID)
	}
}

func (s *Service) AddExplicitDrivers(sub *ClientSubscription, driverIDs []string) {
	for _, driverID := range driverIDs {
		sub.AddExplicitDriver(driverID)
	}

	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()
	for _, driverID := range driverIDs {
		if _, ok := s.explicitDriverSubs[driverID]; !ok {
			s.explicitDriverSubs[driverID] = make(map[string]struct{})
		}
		s.explicitDriverSubs[driverID][sub.ID] = struct{}{}
	}
}

func (s *Service) RemoveExplicitDrivers(sub *ClientSubscription, driverIDs []string) {
	for _, driverID := range driverIDs {
		sub.RemoveExplicitDriver(driverID)
	}

	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()
	for _, driverID := range driverIDs {
		subIDs, ok := s.explicitDriverSubs[driverID]
		if !ok {
			continue
		}
		delete(subIDs, sub.ID)
		if len(subIDs) == 0 {
			delete(s.explicitDriverSubs, driverID)
		}
	}
}

func (s *Service) SendInitialSnapshot(sub *ClientSubscription) error {
	if sub == nil {
		return errors.New("subscription is nil")
	}

	driverMap := make(map[string]models.Command)

	sub.mu.RLock()
	companyID := sub.CompanyID
	bbox := sub.BBox
	explicitIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
	for driverID := range sub.ExplicitDriverIDs {
		explicitIDs = append(explicitIDs, driverID)
	}
	sub.mu.RUnlock()

	if companyID == "" {
		return errors.New("company_id is required")
	}

	if bbox != nil {
		workers, err := redisstore.FindWorkersInBBoxByCompany(s.redis, bbox.MinLat, bbox.MinLng, bbox.MaxLat, bbox.MaxLng, companyID)
		if err != nil {
			return err
		}

		sub.mu.Lock()
		sub.CurrentBBoxDriverIDs = make(map[string]struct{}, len(workers))
		for _, worker := range workers {
			sub.CurrentBBoxDriverIDs[worker.Id] = struct{}{}
			driverMap[worker.Id] = worker
		}
		sub.mu.Unlock()
	}

	for _, driverID := range explicitIDs {
		worker, found, err := redisstore.GetWorkerByID(s.redis, driverID)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if worker.CompanyId != companyID {
			continue
		}
		driverMap[worker.Id] = worker
	}

	drivers := make([]models.Command, 0, len(driverMap))
	for _, driver := range driverMap {
		drivers = append(drivers, driver)
	}

	return sub.Connection.Send(models.WatcherResponse{
		Command: "snapshot",
		Drivers: drivers,
	})
}

func (s *Service) UnregisterAllSSESubscriptions() {
	s.subscriptionsMu.Lock()
	toClose := make([]*ClientSubscription, 0)
	for id, sub := range s.subscriptions {
		if sub == nil || !strings.HasPrefix(sub.Connection.ID(), "sse:") {
			continue
		}
		toClose = append(toClose, sub)
		delete(s.subscriptions, id)
		delete(s.connSubscriptionIDs, sub.Connection.ID())

		sub.mu.RLock()
		companyID := sub.CompanyID
		driverIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
		for driverID := range sub.ExplicitDriverIDs {
			driverIDs = append(driverIDs, driverID)
		}
		sub.mu.RUnlock()

		for _, driverID := range driverIDs {
			subIDs, ok := s.explicitDriverSubs[driverID]
			if !ok {
				continue
			}
			delete(subIDs, id)
			if len(subIDs) == 0 {
				delete(s.explicitDriverSubs, driverID)
			}
		}
		if companyID != "" {
			s.removeBBoxIndexLocked(companyID, id)
		}
	}
	s.subscriptionsMu.Unlock()

	for _, sub := range toClose {
		_ = sub.Connection.Close()
	}
}

func (s *Service) dispatchToSubscriptions(update models.LocationUpdate) {
	s.subscriptionsMu.RLock()
	explicitIDs := s.copySubIDsLocked(s.explicitDriverSubs[update.WorkerID])
	bboxIDs := s.copySubIDsLocked(s.bboxSubscriptionsByCo[update.CompanyId])

	subscriptions := make(map[string]*ClientSubscription, len(explicitIDs)+len(bboxIDs))
	for _, subID := range explicitIDs {
		if sub, ok := s.subscriptions[subID]; ok {
			subscriptions[subID] = sub
		}
	}
	for _, subID := range bboxIDs {
		if sub, ok := s.subscriptions[subID]; ok {
			subscriptions[subID] = sub
		}
	}
	s.subscriptionsMu.RUnlock()

	if len(subscriptions) == 0 {
		return
	}

	eventBySubID := make(map[string]string, len(subscriptions))
	for _, subID := range explicitIDs {
		sub, ok := subscriptions[subID]
		if !ok {
			continue
		}
		if sub.Company() == update.CompanyId {
			eventBySubID[subID] = "track"
		}
	}

	for _, subID := range bboxIDs {
		sub, ok := subscriptions[subID]
		if !ok {
			continue
		}

		sub.mu.RLock()
		bbox := sub.BBox
		sub.mu.RUnlock()
		if bbox == nil {
			continue
		}

		inside := bbox.Contains(update.Latitude, update.Longitude)
		wasInside := sub.MarkBBoxMembership(update.WorkerID, inside)

		switch {
		case inside:
			eventBySubID[subID] = "track"
		case wasInside:
			if _, exists := eventBySubID[subID]; !exists {
				eventBySubID[subID] = "driver-left"
			}
		}
	}

	broken := make([]string, 0)
	for subID, command := range eventBySubID {
		sub := subscriptions[subID]
		if sub == nil {
			continue
		}

		response := models.WatcherResponse{
			Command:      command,
			DriverUpdate: &update,
		}
		if err := sub.Connection.Send(response); err != nil {
			log.Printf("Failed to write update for worker %s to subscription %s: %v", update.WorkerID, subID, err)
			broken = append(broken, subID)
		}
	}

	for _, subID := range broken {
		s.RemoveSubscription(subID)
	}
}

func (s *Service) copySubIDsLocked(subIDs map[string]struct{}) []string {
	if len(subIDs) == 0 {
		return nil
	}

	ids := make([]string, 0, len(subIDs))
	for subID := range subIDs {
		ids = append(ids, subID)
	}
	return ids
}

func (s *Service) sendExplicitDriverSnapshot(sub *ClientSubscription, driverIDs []string) error {
	sub.mu.RLock()
	companyID := sub.CompanyID
	sub.mu.RUnlock()

	drivers := make([]models.Command, 0, len(driverIDs))
	for _, driverID := range driverIDs {
		worker, found, err := redisstore.GetWorkerByID(s.redis, driverID)
		if err != nil {
			return err
		}
		if !found || worker.CompanyId != companyID {
			continue
		}
		drivers = append(drivers, worker)
	}

	if len(drivers) == 0 {
		return nil
	}

	return sub.Connection.Send(models.WatcherResponse{
		Command: "snapshot-driver",
		Drivers: drivers,
	})
}

func (s *Service) setBBoxSubscription(sub *ClientSubscription, companyID string, bbox *BoundingBox) error {
	if err := s.ensureCompany(sub, companyID); err != nil {
		return err
	}

	sub.mu.Lock()
	oldCompanyID := sub.CompanyID
	sub.BBox = bbox
	sub.CurrentBBoxDriverIDs = make(map[string]struct{})
	sub.mu.Unlock()

	s.subscriptionsMu.Lock()
	defer s.subscriptionsMu.Unlock()
	if oldCompanyID != "" {
		s.removeBBoxIndexLocked(oldCompanyID, sub.ID)
	}
	s.addBBoxIndexLocked(companyID, sub.ID)
	return nil
}

func (s *Service) ensureCompany(sub *ClientSubscription, companyID string) error {
	if companyID == "" {
		return errors.New("company_id is required")
	}
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if sub.CompanyID == "" {
		sub.CompanyID = companyID
		return nil
	}
	if sub.CompanyID != companyID {
		return fmt.Errorf("subscription is already scoped to company_id %s", sub.CompanyID)
	}
	return nil
}

func (s *Service) clearSubscription(sub *ClientSubscription) {
	driverIDs := sub.SnapshotDriverIDs()
	if len(driverIDs) > 0 {
		s.RemoveExplicitDrivers(sub, driverIDs)
	}

	sub.mu.Lock()
	companyID := sub.CompanyID
	sub.BBox = nil
	sub.CurrentBBoxDriverIDs = make(map[string]struct{})
	sub.CompanyID = ""
	sub.mu.Unlock()

	if companyID != "" {
		s.subscriptionsMu.Lock()
		s.removeBBoxIndexLocked(companyID, sub.ID)
		s.subscriptionsMu.Unlock()
	}
}

func normalizeDriverIDs(cmd models.Command) []string {
	if cmd.DriverId != nil && *cmd.DriverId != "" {
		return []string{*cmd.DriverId}
	}
	return cmd.DriverIDs
}
