package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/config"
	"fastsocket/models"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"
)

func FindWorkersInBBox(rd *redis.Client, minLat, minLng, maxLat, maxLng float64) ([]models.Command, error) {
	ctx := context.Background()

	if minLat >= maxLat || minLng >= maxLng {
		return nil, errors.New("invalid bounding box coordinates")
	}

	centerLat := (minLat + maxLat) / 2
	centerLng := (minLng + maxLng) / 2
	heightKm := (maxLat - minLat) * 110.574
	widthKm := (maxLng - minLng) * (111.320 * math.Cos(centerLat*math.Pi/180))

	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: centerLng,
			Latitude:  centerLat,
			BoxWidth:  widthKm,
			BoxHeight: heightKm,
			BoxUnit:   "km",

			Count: 1000,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := rd.GeoSearchLocation(ctx, config.WorkerLocationSet, searchQuery).Result()
	if err != nil {

		if errors.Is(err, redis.Nil) {
			return []models.Command{}, nil
		}
		log.Printf("Error executing GeoSearchLocation: %v\n", err)
		return nil, fmt.Errorf("failed to search worker locations: %w", err)
	}

	if len(locations) == 0 {
		return []models.Command{}, nil // No workers found in the bbox
	}

	workerIdsInBox := make([]string, 0, len(locations))
	for _, loc := range locations {
		workerIdsInBox = append(workerIdsInBox, loc.Name)
	}

	detailsData, err := rd.HMGet(ctx, config.WorkerDetailsHash, workerIdsInBox...).Result()
	if err != nil {
		log.Printf("Error fetching worker details with HMGet: %v\n", err)
		return nil, fmt.Errorf("failed to fetch worker details: %w", err)
	}

	workers := make([]models.Command, 0, len(locations))
	for i, data := range detailsData {
		if data == nil {

			log.Printf("Worker %s found in GeoSet but missing details in %s\n", workerIdsInBox[i], config.WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", workerIdsInBox[i], data)
			continue
		}

		var worker models.Command
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", workerIdsInBox[i], err)
			continue
		}
		workers = append(workers, worker)
	}

	return workers, nil
}

func GetAllWorkersPaginated(rd *redis.Client, page, pageSize int) ([]models.Command, int, error) {
	ctx := context.Background()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	allWorkerIds, err := rd.HKeys(ctx, config.WorkerDetailsHash).Result()
	if err != nil {
		log.Printf("Error fetching all worker keys from %s: %v\n", config.WorkerDetailsHash, err)
		return nil, 0, fmt.Errorf("failed to get worker keys: %w", err)
	}

	totalWorkers := len(allWorkerIds)
	if totalWorkers == 0 {
		return []models.Command{}, 0, nil
	}

	sort.SliceStable(allWorkerIds, func(i, j int) bool {
		return compareWorkerIDs(allWorkerIds[i], allWorkerIds[j]) < 0
	})

	start := (page - 1) * pageSize
	if start >= totalWorkers {
		return []models.Command{}, totalWorkers, nil
	}

	end := start + pageSize
	if end > totalWorkers {
		end = totalWorkers
	}

	paginatedWorkerIds := allWorkerIds[start:end]

	if len(paginatedWorkerIds) == 0 {
		return []models.Command{}, totalWorkers, nil
	}

	detailsData, err := rd.HMGet(ctx, config.WorkerDetailsHash, paginatedWorkerIds...).Result()
	if err != nil {
		log.Printf("Error fetching paginated worker details with HMGet: %v\n", err)
		return nil, totalWorkers, fmt.Errorf("failed to fetch worker details for page %d: %w", page, err)
	}

	workers := make([]models.Command, 0, len(paginatedWorkerIds))
	now := time.Now()
	oneMinuteAgo := now.Add(-1 * time.Minute)
	needsUpdate := make(map[string]models.Command)

	for i, data := range detailsData {
		if data == nil {
			log.Printf("Details not found for paginated worker %s in %s\n", paginatedWorkerIds[i], config.WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", paginatedWorkerIds[i], data)
			continue
		}

		var worker models.Command
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", paginatedWorkerIds[i], err)
			continue
		}

		// Parse the UpdatedAt string into a time.Time object
		if worker.UpdatedAt == nil || worker.Active == nil {
			workers = append(workers, worker)
			continue
		}

		updatedAt, err := time.Parse(time.RFC3339, *worker.UpdatedAt)
		if err != nil {
			log.Printf("Error parsing UpdatedAt for worker %s: %v\n", paginatedWorkerIds[i], err)
			continue
		}

		// Check if the worker was updated more than 1 minute ago and is still marked as active
		active := false
		_updatedAt := fmt.Sprintf("%s", now.Format(time.RFC3339))
		if updatedAt.Before(oneMinuteAgo) && *worker.Active {
			worker.Active = &active
			worker.UpdatedAt = &_updatedAt
			needsUpdate[paginatedWorkerIds[i]] = worker
		}

		workers = append(workers, worker)
	}

	if len(needsUpdate) > 0 {
		err := updateInactiveWorkersInRedis(ctx, rd, needsUpdate)
		if err != nil {
			log.Printf("Error updating inactive workers in Redis: %v\n", err)

		}
	}

	return workers, totalWorkers, nil
}

var workerIDSuffixPattern = regexp.MustCompile(`^(.+?)_(\d+)$`)

func compareWorkerIDs(left, right string) int {
	leftPrefix, leftNumeric, leftIsNumeric := parseWorkerID(left)
	rightPrefix, rightNumeric, rightIsNumeric := parseWorkerID(right)

	if leftPrefix != rightPrefix {
		if leftPrefix < rightPrefix {
			return -1
		}
		return 1
	}

	if leftIsNumeric && rightIsNumeric {
		if leftNumeric < rightNumeric {
			return -1
		}
		if leftNumeric > rightNumeric {
			return 1
		}
	}

	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func parseWorkerID(value string) (prefix string, numeric int, isNumeric bool) {
	matches := workerIDSuffixPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return value, 0, false
	}

	numeric, err := strconv.Atoi(matches[2])
	if err != nil {
		return value, 0, false
	}
	return matches[1], numeric, true
}

func updateInactiveWorkersInRedis(ctx context.Context, rd *redis.Client, workers map[string]models.Command) error {
	pipe := rd.Pipeline()

	for id, worker := range workers {
		workerData, err := json.Marshal(worker)
		if err != nil {
			log.Printf("Error marshalling worker %s data: %v\n", id, err)
			continue
		}
		pipe.HSet(ctx, config.WorkerDetailsHash, id, workerData)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline for inactive workers update: %w", err)
	}

	return nil
}

func GetWorkerByID(rd *redis.Client, workerID string) (models.Command, bool, error) {
	ctx := context.Background()

	detailStr, err := rd.HGet(ctx, config.WorkerDetailsHash, workerID).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return models.Command{}, false, nil
		}
		return models.Command{}, false, fmt.Errorf("failed to fetch worker details for %s: %w", workerID, err)
	}

	var worker models.Command
	if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
		return models.Command{}, false, fmt.Errorf("failed to unmarshal worker details for %s: %w", workerID, err)
	}

	return worker, true, nil
}

func FindWorkersInBBoxByCompany(rd *redis.Client, minLat, minLng, maxLat, maxLng float64, companyID string) ([]models.Command, error) {
	ctx := context.Background()

	if minLat >= maxLat || minLng >= maxLng {
		return nil, errors.New("invalid bounding box coordinates")
	}

	centerLat := (minLat + maxLat) / 2
	centerLng := (minLng + maxLng) / 2
	heightKm := (maxLat - minLat) * 110.574
	widthKm := (maxLng - minLng) * (111.320 * math.Cos(centerLat*math.Pi/180))

	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: centerLng,
			Latitude:  centerLat,
			BoxWidth:  widthKm,
			BoxHeight: heightKm,
			BoxUnit:   "km",
			Count:     1000,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := rd.GeoSearchLocation(ctx, config.WorkerLocationSet, searchQuery).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return []models.Command{}, nil
		}
		log.Printf("Error executing GeoSearchLocation: %v\n", err)
		return nil, fmt.Errorf("failed to search worker locations: %w", err)
	}

	if len(locations) == 0 {
		return []models.Command{}, nil
	}

	workerIdsInBox := make([]string, 0, len(locations))
	for _, loc := range locations {
		workerIdsInBox = append(workerIdsInBox, loc.Name)
	}

	detailsData, err := rd.HMGet(ctx, config.WorkerDetailsHash, workerIdsInBox...).Result()
	if err != nil {
		log.Printf("Error fetching worker details with HMGet: %v\n", err)
		return nil, fmt.Errorf("failed to fetch worker details: %w", err)
	}

	workers := make([]models.Command, 0, len(locations))
	for i, data := range detailsData {
		if data == nil {
			log.Printf("Worker %s found in GeoSet but missing details\n", workerIdsInBox[i])
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			continue
		}

		var worker models.Command
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			continue
		}

		if worker.Active == nil || !*worker.Active {
			continue
		}

		if companyID != "" && worker.CompanyId != companyID {
			continue
		}

		workers = append(workers, worker)
	}

	return workers, nil
}

func GetAllWorkersPaginatedByCompany(rd *redis.Client, page, pageSize int, companyID string) ([]models.Command, int, error) {
	ctx := context.Background()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	allWorkerIds, err := rd.HKeys(ctx, config.WorkerDetailsHash).Result()
	if err != nil {
		log.Printf("Error fetching all worker keys from %s: %v\n", config.WorkerDetailsHash, err)
		return nil, 0, fmt.Errorf("failed to get worker keys: %w", err)
	}

	if len(allWorkerIds) == 0 {
		return []models.Command{}, 0, nil
	}

	sort.SliceStable(allWorkerIds, func(i, j int) bool {
		return compareWorkerIDs(allWorkerIds[i], allWorkerIds[j]) < 0
	})

	start := (page - 1) * pageSize
	chunkSize := pageSize
	if chunkSize < 100 {
		chunkSize = 100
	}

	pageWorkers := make([]models.Command, 0, pageSize)
	totalWorkers := 0
	now := time.Now()
	oneMinuteAgo := now.Add(-1 * time.Minute)
	needsUpdate := make(map[string]models.Command)

	for chunkStart := 0; chunkStart < len(allWorkerIds); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(allWorkerIds) {
			chunkEnd = len(allWorkerIds)
		}

		chunkIDs := allWorkerIds[chunkStart:chunkEnd]
		detailsData, err := rd.HMGet(ctx, config.WorkerDetailsHash, chunkIDs...).Result()
		if err != nil {
			log.Printf("Error fetching paginated worker details with HMGet: %v\n", err)
			return nil, 0, fmt.Errorf("failed to fetch worker details: %w", err)
		}

		for i, data := range detailsData {
			if data == nil {
				log.Printf("Details not found for worker %s\n", chunkIDs[i])
				continue
			}

			detailStr, ok := data.(string)
			if !ok {
				continue
			}

			var worker models.Command
			if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
				continue
			}

			if worker.UpdatedAt == nil || worker.Active == nil {
				continue
			}

			updatedAt, err := time.Parse(time.RFC3339, *worker.UpdatedAt)
			if err != nil {
				continue
			}

			active := false
			updatedAtNow := now.Format(time.RFC3339)
			if updatedAt.Before(oneMinuteAgo) && *worker.Active {
				worker.Active = &active
				worker.UpdatedAt = &updatedAtNow
				needsUpdate[chunkIDs[i]] = worker
			}

			if companyID != "" && worker.CompanyId != companyID {
				continue
			}

			if totalWorkers >= start && len(pageWorkers) < pageSize {
				pageWorkers = append(pageWorkers, worker)
			}
			totalWorkers++
		}
	}

	if len(needsUpdate) > 0 {
		if err := updateInactiveWorkersInRedis(ctx, rd, needsUpdate); err != nil {
			log.Printf("Error updating inactive workers in Redis: %v\n", err)
		}
	}

	if start >= totalWorkers {
		return []models.Command{}, totalWorkers, nil
	}

	return pageWorkers, totalWorkers, nil
}

