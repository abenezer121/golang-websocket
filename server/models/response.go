package models

type LocationUpdate struct {
	WorkerID  string  `json:"worker_id"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
	CompanyId string  `json:"company_id"`
	UnixTime  string  `json:"unix_time"`
}

/* WatcherResponse is the transport independent response shape emitted by the service layer.
and Each transport is responsible for encoding it into its own wire format (protobuf and json). */
type WatcherResponse struct {
	Command      string
	Drivers      []Command
	DriverUpdate *LocationUpdate
	Error        string
	Status       string
}
