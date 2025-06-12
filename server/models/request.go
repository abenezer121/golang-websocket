package models

import "time"

type DriverTypeEnum int

const (
	DriverTypeInactive DriverTypeEnum = iota // 0
	DriverTypeActive                         // 1
	DriverTypeAll                            // 2
)

type Command struct {
	Id          string          `json:"id,omitempty"`
	CommandType *string         `json:"command_type,omitempty"`
	DriverId    *string         `json:"driver_id,omitempty"`
	MinLat      *float64        `json:"min_lat,omitempty"`
	MinLng      *float64        `json:"min_lng,omitempty"`
	MaxLat      *float64        `json:"max_lat,omitempty"`
	MaxLng      *float64        `json:"max_lng,omitempty"`
	Page        *int            `json:"page,omitempty"`
	DriverType  *DriverTypeEnum `json:"driver_type,omitempty"` // 0 inactive 1 active 2 all
	Lat         *float64        `json:"lat"`
	Lng         *float64        `json:"lng"`
	From        *string         `json:"from"`
	Session     *string         `json:"session"`
	CreatedAt   *string         `json:"created_at"`
	UpdatedAt   *string         `json:"updated_at"`
	LastSeen    *time.Time      `json:"last_seen"`
	Active      *bool           `json:"active"`
	CompanyId   string          `json:"company_id"`
}
