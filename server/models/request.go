package models

import "time"

type Command struct {
	//Id          string          `json:"id,omitempty"`
	CommandType *string    `json:"command_type,omitempty"`
	DriverId    *string    `json:"driver_id,omitempty"`
	MinLat      *float64   `json:"min_lat,omitempty"`
	MinLng      *float64   `json:"min_lng,omitempty"`
	MaxLat      *float64   `json:"max_lat,omitempty"`
	MaxLng      *float64   `json:"max_lng,omitempty"`
	Page        *int       `json:"page,omitempty"`
	Lat         *float64   `json:"lat"`
	Lng         *float64   `json:"lng"`
	From        *string    `json:"from"`
	Session     *string    `json:"session"`
	At          *time.Time `json:"at"`
	Active      *bool      `json:"active"`
}
