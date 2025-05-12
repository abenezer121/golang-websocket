package models

import "time"

type Command struct {
	//Id          string          `json:"id,omitempty"`
	ClientId    *string    `json:"client_id,omitempty"`
	CommandType *string    `json:"command_type,omitempty"`
	DriverId    *string    `json:"driver_id,omitempty"`
	MinLat      *float64   `json:"min_lat,omitempty"`
	MinLng      *float64   `json:"min_lng,omitempty"`
	MaxLat      *float64   `json:"max_lat,omitempty"`
	MaxLng      *float64   `json:"max_lng,omitempty"`
	Page        *int       `json:"page,omitempty"`
	Lat         *float64   `json:"lat,omitempty"`
	Lng         *float64   `json:"lng,omitempty"`
	From        *string    `json:"from,omitempty"`
	Session     *string    `json:"session,omitempty"`
	At          *time.Time `json:"at,omitempty"`
	Active      *bool      `json:"active,omitempty"`
}
