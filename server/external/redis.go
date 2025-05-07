package external

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/models"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"sort"
	"time"
)

func FindWorkersInBBox(rd *redis.Client, minLat, minLng, maxLat, maxLng float64) ([]models.WorkersToTrack, error) {
	ctx := context.Background()

	if minLat >= maxLat || minLng >= maxLng {
		return nil, errors.New("invalid bounding box coordinates")
	}

	centerLat := (minLat + maxLat) / 2
	centerLng := (minLng + maxLng) / 2
	height := maxLat - minLat
	width := maxLng - minLng

	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: centerLng,
			Latitude:  centerLat,
			BoxWidth:  width,
			BoxHeight: height,

			Count: 1000,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := rd.GeoSearchLocation(ctx, models.WorkerLocationSet, searchQuery).Result()
	if err != nil {

		if errors.Is(err, redis.Nil) {
			return []models.WorkersToTrack{}, nil
		}
		log.Printf("Error executing GeoSearchLocation: %v\n", err)
		return nil, fmt.Errorf("failed to search worker locations: %w", err)
	}

	if len(locations) == 0 {
		return []models.WorkersToTrack{}, nil // No workers found in the bbox
	}

	workerIdsInBox := make([]string, 0, len(locations))
	for _, loc := range locations {
		workerIdsInBox = append(workerIdsInBox, loc.Name)
	}

	detailsData, err := rd.HMGet(ctx, models.WorkerDetailsHash, workerIdsInBox...).Result()
	if err != nil {
		log.Printf("Error fetching worker details with HMGet: %v\n", err)
		return nil, fmt.Errorf("failed to fetch worker details: %w", err)
	}

	workers := make([]models.WorkersToTrack, 0, len(locations))
	for i, data := range detailsData {
		if data == nil {

			log.Printf("Worker %s found in GeoSet but missing details in %s\n", workerIdsInBox[i], models.WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", workerIdsInBox[i], data)
			continue
		}

		var worker models.WorkersToTrack
		if err := json.Unmarshal([]byte(detailStr), worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", workerIdsInBox[i], err)
			continue
		}
		if worker.Active {

			workers = append(workers, worker)
		}
	}

	return workers, nil
}

func GetAllWorkersPaginated(rd *redis.Client, page, pageSize int) ([]models.WorkersToTrack, int, error) {
	ctx := context.Background()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	allWorkerIds, err := rd.HKeys(ctx, models.WorkerDetailsHash).Result()
	if err != nil {
		log.Printf("Error fetching all worker keys from %s: %v\n", models.WorkerDetailsHash, err)
		return nil, 0, fmt.Errorf("failed to get worker keys: %w", err)
	}

	totalWorkers := len(allWorkerIds)
	if totalWorkers == 0 {
		return []models.WorkersToTrack{}, 0, nil
	}

	sort.Strings(allWorkerIds)

	start := (page - 1) * pageSize
	if start >= totalWorkers {
		return []models.WorkersToTrack{}, totalWorkers, nil
	}

	end := start + pageSize
	if end > totalWorkers {
		end = totalWorkers
	}

	paginatedWorkerIds := allWorkerIds[start:end]

	if len(paginatedWorkerIds) == 0 {
		return []models.WorkersToTrack{}, totalWorkers, nil
	}

	detailsData, err := rd.HMGet(ctx, models.WorkerDetailsHash, paginatedWorkerIds...).Result()
	if err != nil {
		log.Printf("Error fetching paginated worker details with HMGet: %v\n", err)
		return nil, totalWorkers, fmt.Errorf("failed to fetch worker details for page %d: %w", page, err)
	}

	workers := make([]models.WorkersToTrack, 0, len(paginatedWorkerIds))
	now := time.Now()
	oneMinuteAgo := now.Add(-1 * time.Minute)
	needsUpdate := make(map[string]models.WorkersToTrack)

	for i, data := range detailsData {
		if data == nil {
			log.Printf("Details not found for paginated worker %s in %s\n", paginatedWorkerIds[i], models.WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", paginatedWorkerIds[i], data)
			continue
		}

		var worker models.WorkersToTrack
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", paginatedWorkerIds[i], err)
			continue
		}

		// Parse the UpdatedAt string into a time.Time object
		updatedAt, err := time.Parse(time.RFC3339, worker.UpdatedAt) // Adjust layout based on your actual format
		if err != nil {
			log.Printf("Error parsing UpdatedAt for worker %s: %v\n", paginatedWorkerIds[i], err)
			continue
		}

		// Check if the worker was updated more than 1 minute ago and is still marked as active
		if updatedAt.Before(oneMinuteAgo) && worker.Active {
			worker.Active = false
			worker.UpdatedAt = now.Format(time.RFC3339) // Format back to string using same format
			needsUpdate[paginatedWorkerIds[i]] = worker
		}

		workers = append(workers, worker)
	}

	// Update workers that need to be marked as inactive in Redis
	if len(needsUpdate) > 0 {
		err := updateInactiveWorkersInRedis(ctx, rd, needsUpdate)
		if err != nil {
			log.Printf("Error updating inactive workers in Redis: %v\n", err)
			// Continue to return the workers even if Redis update failed
		}
	}

	return workers, totalWorkers, nil
}

func updateInactiveWorkersInRedis(ctx context.Context, rd *redis.Client, workers map[string]models.WorkersToTrack) error {
	pipe := rd.Pipeline()

	for id, worker := range workers {
		workerData, err := json.Marshal(worker)
		if err != nil {
			log.Printf("Error marshalling worker %s data: %v\n", id, err)
			continue
		}
		pipe.HSet(ctx, models.WorkerDetailsHash, id, workerData)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline for inactive workers update: %w", err)
	}

	return nil
}
