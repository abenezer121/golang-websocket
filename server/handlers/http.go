package handlers

import (
	"encoding/json"
	"errors"
	"fastsocket/epoll"
	"io"
	"net/http"
	"strings"
)

const maxDriverUpdateBodyBytes = 1 << 20

type driverUpdateRequest struct {
	ID        string   `json:"id"`
	Lat       *float64 `json:"lat"`
	Lng       *float64 `json:"lng"`
	CompanyID string   `json:"company_id"`
}

type apiResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func HandleDriverUpdateHTTP(ep *epoll.Epoll) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, apiResponse{
				Status:  "error",
				Message: "method not allowed; use POST",
			})
			return
		}

		if !isJSONRequest(r) {
			writeJSON(w, http.StatusUnsupportedMediaType, apiResponse{
				Status:  "error",
				Message: "content type must be application/json",
			})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxDriverUpdateBodyBytes)
		defer r.Body.Close()

		var req driverUpdateRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&req); err != nil {
			writeDecodeError(w, err)
			return
		}

		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, apiResponse{
				Status:  "error",
				Message: "request body must contain a single JSON object",
			})
			return
		}

		if req.ID == "" {
			writeJSON(w, http.StatusBadRequest, apiResponse{
				Status:  "error",
				Message: "id is required",
			})
			return
		}
		if req.Lat == nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{
				Status:  "error",
				Message: "lat is required",
			})
			return
		}
		if req.Lng == nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{
				Status:  "error",
				Message: "lng is required",
			})
			return
		}
		if req.CompanyID == "" {
			writeJSON(w, http.StatusBadRequest, apiResponse{
				Status:  "error",
				Message: "company_id is required",
			})
			return
		}

		if err := ep.UpdateWorkersLocation(req.ID, *req.Lat, *req.Lng, req.CompanyID); err != nil {
			writeJSON(w, http.StatusInternalServerError, apiResponse{
				Status:  "error",
				Message: "failed to process driver update",
			})
			return
		}

		writeJSON(w, http.StatusOK, apiResponse{Status: "ok"})
	}
}

func isJSONRequest(r *http.Request) bool {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		return false
	}

	mediaType := strings.TrimSpace(strings.Split(contentType, ";")[0])
	return mediaType == "application/json"
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var syntaxErr *json.SyntaxError
	var unmarshalTypeErr *json.UnmarshalTypeError

	switch {
	case errors.As(err, &syntaxErr):
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Status:  "error",
			Message: "request body contains malformed JSON",
		})
	case errors.Is(err, io.EOF):
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Status:  "error",
			Message: "request body must not be empty",
		})
	case errors.As(err, &unmarshalTypeErr):
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Status:  "error",
			Message: "request body contains an invalid value type",
		})
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Status:  "error",
			Message: err.Error(),
		})
	case strings.Contains(err.Error(), "http: request body too large"):
		writeJSON(w, http.StatusRequestEntityTooLarge, apiResponse{
			Status:  "error",
			Message: "request body is too large",
		})
	default:
		writeJSON(w, http.StatusBadRequest, apiResponse{
			Status:  "error",
			Message: "invalid JSON payload",
		})
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
