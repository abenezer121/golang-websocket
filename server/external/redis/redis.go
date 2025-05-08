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
	"sort"
)

func FindWorkersInBBox(rd *redis.Client, minLat, minLng, maxLat, maxLng float64) ([]models.Command, error) {
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
		if err := json.Unmarshal([]byte(detailStr), worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", workerIdsInBox[i], err)
			continue
		}
		if *worker.Active {

			workers = append(workers, worker)
		}
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

	sort.Strings(allWorkerIds)

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

		workers = append(workers, worker)
	}

	return workers, totalWorkers, nil
}
