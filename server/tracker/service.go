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
	"sync"
	"time"

	"fastsocket/config"
)

type Service struct {
	redis *redis.Client

	mu             sync.RWMutex
	notifyMap      map[string]map[string]transport.ClientConnection
	driverTrackMap map[string]string
}

func NewService(rd *redis.Client) *Service {
	return &Service{
		redis:          rd,
		notifyMap:      make(map[string]map[string]transport.ClientConnection),
		driverTrackMap: make(map[string]string),
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
	if len(subscribers) > 0 {
		update := models.LocationUpdate{
			WorkerID:  workerID,
			Latitude:  lat,
			Longitude: lng,
			Timestamp: nowStr,
			UnixTime:  nowUnixStr,
			CompanyId: workerToStore.CompanyId,
		}

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

		response, err := s.bboxResponse(*decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng)
		if err != nil {
			log.Printf("ERROR: 'get-bbox' failed: %v", err)
			return s.sendError(conn, "Failed to retrieve data for bounding box")
		}

		return s.send(conn, response)

	case "get-drivers":
		if decodedMsg.Page == nil {
			return s.sendError(conn, "get-drivers command requires a page number")
		}

		response, err := s.driversResponse(*decodedMsg.Page, 100)
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
	defer s.mu.Unlock()

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
	return s.send(conn, models.WatcherResponse{Error: message})
}

func (s *Service) sendStatus(conn transport.ClientConnection, message string) error {
	return s.send(conn, models.WatcherResponse{Status: message})
}
