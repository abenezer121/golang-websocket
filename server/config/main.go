package config

import (
	"flag"
	"log"
)

const (
	WorkerLocationSet = "workers:locations" // Geo set for worker locations
	WorkerDetailsHash = "workers:details"   // Hash storing worker details
	WorkerHistorySet  = "workers:history:"
)

var (
	ResponseFormat string
)

func ParseFlags() {
	flag.StringVar(&ResponseFormat, "response_format", "proto", "Format of API responses (e.g., json or proto)")
	flag.Parse()

	log.Printf("Using response format: %s", ResponseFormat)
}
