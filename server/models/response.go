package models

type LocationUpdate struct {
	WorkerID  string  `json:"worker_id"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
	UnixTime  string  `json:"unix_time"`
}
type SocketResponse struct {
	Command    string
	Paginated  []Command
	DriverData LocationUpdate
}
