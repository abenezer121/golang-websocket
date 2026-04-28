package models

type LocationUpdate struct {
	WorkerID  string  `json:"worker_id"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
	CompanyId string  `json:"company_id"`
	UnixTime  string  `json:"unix_time"`
}

type SocketResponse struct {
	Command    string          `json:"command"`
	Status     string          `json:"status,omitempty"`
	Message    string          `json:"message,omitempty"`
	DriverIDs  []string        `json:"driver_ids,omitempty"`
	Paginated  []Command       `json:"paginated,omitempty"`
	DriverData *LocationUpdate `json:"driver_data,omitempty"`
}
