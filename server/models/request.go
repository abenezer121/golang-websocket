package models

type Command struct {
	CommandType string   `json:"command_type"`
	DriverId    *string  `json:"driver_id,omitempty"`
	MinLat      *float64 `json:"min_lat,omitempty"`
	MinLng      *float64 `json:"min_lng,omitempty"`
	MaxLat      *float64 `json:"max_lat,omitempty"`
	MaxLng      *float64 `json:"max_lng,omitempty"`
	Page        *int     `json:"page,omitempty"`
}
