package models

import "time"

type WorkersToTrack struct {
	Id        string    `json:"id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	From      string    `json:"from"`
	CreatedAt string    `json:"created_at"`
	UpdatedAt string    `json:"updated_at"`
	LastSeen  time.Time `json:"last_seen"`
	Active    bool      `json:"active"`
}
