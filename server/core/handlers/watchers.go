package handlers

import (
	"errors"
	"fastsocket/core/epoll"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"syscall"
)

func ControlHandler(upgrader websocket.Upgrader, w http.ResponseWriter, r *http.Request, ep *epoll.Epoll) {
	// Upgrade the HTTP connection to a WebSocket connection. This should happen ONCE.
	log.Printf("hello htere")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
			log.Printf("ERROR: WebSocket upgrade failed: Too many open files (EMFILE/ENFILE). Check RLIMIT_NOFILE. %v", err)
			http.Error(w, "Server at connection limit", http.StatusServiceUnavailable)
		} else if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset by peer") {
			log.Printf("INFO: WebSocket upgrade failed, client likely disconnected prematurely: %v", err)
		} else {
			log.Printf("ERROR: WebSocket upgrade failed: %v", err)
			http.Error(w, "Failed to upgrade connection", http.StatusInternalServerError)
		}
		ep.Metrics.UpgradesFailed.Add(1)
		return
	}

	if err := ep.Add(conn); err != nil {
		log.Printf("ERROR: Failed to add WebSocket connection (FD potentially obtained) to epoll: %v", err)
		ep.Metrics.UpgradesFailed.Add(1)
		conn.Close()
		return
	}

	ep.Metrics.UpgradesSuccess.Add(1)

}

/*

func ControlHandler(upgrader websocket.Upgrader, w http.ResponseWriter, r *http.Request, notifyMap map[string][]*websocket.Conn, notifyMapMutex *sync.RWMutex, rd *redis.Client) {
	// Upgrade the HTTP connection to a WebSocket connection. This should happen ONCE.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Handle various upgrade errors
		if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) {
			log.Printf("ERROR: WebSocket upgrade failed for %s: Too many open files (EMFILE/ENFILE). Check RLIMIT_NOFILE. %v", r.RemoteAddr, err)
			http.Error(w, "Server at connection limit, please try again later.", http.StatusServiceUnavailable)
		} else if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
			strings.Contains(err.Error(), "connection reset by peer") ||
			strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("INFO: WebSocket upgrade failed for %s, client likely disconnected prematurely: %v", r.RemoteAddr, err)
			// Don't write http.Error as the connection is likely already gone or response writer is unusable
		} else if strings.Contains(err.Error(), "websocket: bad handshake") {
			log.Printf("ERROR: WebSocket upgrade failed for %s due to bad handshake: %v", r.RemoteAddr, err)
			http.Error(w, "Failed to upgrade connection: bad handshake", http.StatusBadRequest)
		} else {
			log.Printf("ERROR: WebSocket upgrade failed for %s: %v", r.RemoteAddr, err)
			http.Error(w, "Failed to upgrade connection to WebSocket.", http.StatusInternalServerError)
		}
		return // Essential to stop further processing if upgrade fails
	}

	// Log successful connection and ensure it's closed when the handler exits.
	log.Printf("INFO: Control client connected: %s", conn.RemoteAddr())

	// trackedDriverID stores the ID of the driver this connection is currently tracking.
	// This is used for cleanup in notifyMap when the connection closes or changes tracking.
	var trackedDriverID string

	defer func() {
		log.Printf("INFO: Closing connection for %s", conn.RemoteAddr())
		// Clean up this connection from the notifyMap if it was tracking a driver.
		if trackedDriverID != "" {
			notifyMapMutex.Lock()
			if conns, ok := notifyMap[trackedDriverID]; ok {
				// Remove the current connection (conn) from the slice of connections for the trackedDriverID
				updatedConns := []*websocket.Conn{}
				for _, c := range conns {
					if c != conn {
						updatedConns = append(updatedConns, c)
					}
				}
				if len(updatedConns) == 0 {
					delete(notifyMap, trackedDriverID) // No more trackers for this driver
					log.Printf("INFO: Removed last tracker for driver_id: %s (client: %s)", trackedDriverID, conn.RemoteAddr())
				} else {
					notifyMap[trackedDriverID] = updatedConns
					log.Printf("INFO: Removed client %s from tracking driver_id: %s. Remaining trackers: %d", conn.RemoteAddr(), trackedDriverID, len(updatedConns))
				}
			}
			notifyMapMutex.Unlock()
		}
		conn.Close() // Close the WebSocket connection.
	}()

	// This is the message read loop for the successfully established WebSocket connection.
	for {
		messageType, msg, err := conn.ReadMessage()
		if err != nil {
			// Handle read errors (client disconnected, network issues, etc.)
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("INFO: Client %s disconnected normally.", conn.RemoteAddr())
			} else if errors.Is(err, io.EOF) {
				log.Printf("INFO: Client %s disconnected (EOF).", conn.RemoteAddr())
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("INFO: Client %s connection closed: %v", conn.RemoteAddr(), err)
			} else {
				log.Printf("ERROR: Read error from client %s: %v", conn.RemoteAddr(), err)
			}
			break // Exit the read loop; defer will handle cleanup and closing.
		}

		// We only process text messages.
		if messageType != websocket.TextMessage {
			log.Printf("WARN: Received non-text message type %d from %s. Ignoring.", messageType, conn.RemoteAddr())
			continue
		}

		// Attempt to unmarshal the JSON message into our Command struct.
		var decodedMsg models.Command
		if err := json.Unmarshal(msg, &decodedMsg); err != nil {
			log.Printf("ERROR: Failed to unmarshal JSON from %s: %v. Message: %s", conn.RemoteAddr(), err, string(msg))
			errMsg := []byte(`{"error": "Invalid JSON format"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send JSON format error message to %s: %v", conn.RemoteAddr(), writeErr)
				break // If we can't write, assume connection is dead.
			}
			continue
		}

		// Handle commands based on CommandType.
		switch *decodedMsg.CommandType {
		case "get-bbox":
			log.Printf("INFO: Handling 'get-bbox' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
			// Validate required fields for get-bbox.
			if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil {
				log.Printf("WARN: 'get-bbox' command from %s missing required coordinates. Message: %s", conn.RemoteAddr(), string(msg))
				errMsg := []byte(`{"error": "get-bbox command requires min_lat, min_lng, max_lat, max_lng"}`)
				if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
					log.Printf("ERROR: Failed to send coordinate error message to %s: %v", conn.RemoteAddr(), writeErr)
					break
				}
				continue
			}

			// Call your external function to find workers in the bounding box.
			// Note: Corrected the last parameter from *decodedMsg.MinLng to *decodedMsg.MaxLng.
			paginated, err := external.FindWorkersInBBox(rd, *decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng)
			if err != nil {
				log.Printf("ERROR: 'get-bbox' failed to find workers for %s: %v", conn.RemoteAddr(), err)
				errMsg := []byte(`{"error": "Failed to retrieve data for bounding box"}`)
				if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
					log.Printf("ERROR: Failed to send bbox data retrieval error message to %s: %v", conn.RemoteAddr(), writeErr)
					break
				}
				continue
			}

			// Prepare and send the response.
			newSocketResponse := models.SocketResponse{Command: "get-bbox", Paginated: paginated}
			responseBytes, marshalErr := json.Marshal(newSocketResponse)
			if marshalErr != nil {
				log.Printf("ERROR: Failed to marshal 'get-bbox' response for %s: %v", conn.RemoteAddr(), marshalErr)
				continue // Or send an internal server error message
			}
			if writeErr := conn.WriteMessage(websocket.TextMessage, responseBytes); writeErr != nil {
				log.Printf("ERROR: Failed to send 'get-bbox' response to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}

		case "get-drivers":
			log.Printf("INFO: Handling 'get-drivers' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
			// Validate required fields for get-drivers.
			if decodedMsg.Page == nil {
				log.Printf("WARN: 'get-drivers' command from %s missing page number. Message: %s", conn.RemoteAddr(), string(msg))
				errMsg := []byte(`{"error": "get-drivers command requires a page number"}`)
				if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
					log.Printf("ERROR: Failed to send page number error message to %s: %v", conn.RemoteAddr(), writeErr)
					break
				}
				continue
			}

			// Call your external function to get all workers paginated.
			paginated, _, err := external.GetAllWorkersPaginated(rd, *decodedMsg.Page, 100) // Assuming page size of 100
			if err != nil {
				log.Printf("ERROR: 'get-drivers' failed to get all workers for %s: %v", conn.RemoteAddr(), err)
				errMsg := []byte(`{"error": "Failed to retrieve drivers list"}`)
				if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
					log.Printf("ERROR: Failed to send get-drivers data retrieval error message to %s: %v", conn.RemoteAddr(), writeErr)
					break
				}
				continue
			}

			// Prepare and send the response.
			newSocketResponse := models.SocketResponse{Command: "get-drivers", Paginated: paginated}
			responseBytes, marshalErr := json.Marshal(newSocketResponse)
			if marshalErr != nil {
				log.Printf("ERROR: Failed to marshal 'get-drivers' response for %s: %v", conn.RemoteAddr(), marshalErr)
				continue
			}
			if writeErr := conn.WriteMessage(websocket.TextMessage, responseBytes); writeErr != nil {
				log.Printf("ERROR: Failed to send 'get-drivers' response to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}

		case "track-driver":
			log.Printf("INFO: Handling 'track-driver' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
			// Validate required fields for track-driver.
			if decodedMsg.DriverId == nil || *decodedMsg.DriverId == "" {
				log.Printf("WARN: 'track-driver' command from %s missing valid driver_id. Message: %s", conn.RemoteAddr(), string(msg))
				errMsg := []byte(`{"error": "track-driver command requires a valid driver_id"}`)
				if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
					log.Printf("ERROR: Failed to send driver_id validation error message to %s: %v", conn.RemoteAddr(), writeErr)
					break
				}
				continue
			}

			newDriverToTrack := *decodedMsg.DriverId
			notifyMapMutex.Lock() // Lock before modifying notifyMap or trackedDriverID

			// If the connection was previously tracking a different driver, remove it from the old driver's list.
			if trackedDriverID != "" && trackedDriverID != newDriverToTrack {
				if conns, ok := notifyMap[trackedDriverID]; ok {
					updatedConns := []*websocket.Conn{}
					for _, c := range conns {
						if c != conn { // Don't add the current connection back to the old list
							updatedConns = append(updatedConns, c)
						}
					}
					if len(updatedConns) == 0 {
						delete(notifyMap, trackedDriverID)
						log.Printf("INFO: Client %s stopped tracking old driver_id: %s (removed last tracker).", conn.RemoteAddr(), trackedDriverID)
					} else {
						notifyMap[trackedDriverID] = updatedConns
						log.Printf("INFO: Client %s stopped tracking old driver_id: %s. Remaining trackers for old ID: %d", conn.RemoteAddr(), trackedDriverID, len(updatedConns))
					}
				}
			}

			// Add this connection to the list of trackers for the newDriverToTrack.
			// Prevent duplicate additions if client sends command multiple times.
			isAlreadyTracking := false
			for _, existingConn := range notifyMap[newDriverToTrack] {
				if existingConn == conn {
					isAlreadyTracking = true
					break
				}
			}

			if !isAlreadyTracking {
				notifyMap[newDriverToTrack] = append(notifyMap[newDriverToTrack], conn)
				log.Printf("INFO: Client %s is NOW tracking driver_id: %s. Total trackers for this driver: %d", conn.RemoteAddr(), newDriverToTrack, len(notifyMap[newDriverToTrack]))
			} else {
				log.Printf("INFO: Client %s is ALREADY tracking driver_id: %s.", conn.RemoteAddr(), newDriverToTrack)
			}
			trackedDriverID = newDriverToTrack // Update the driver ID this connection is tracking.
			notifyMapMutex.Unlock()            // Unlock after modifications

			// Send an acknowledgment message.
			ackMsg := []byte(fmt.Sprintf(`{"status": "now tracking driver_id %s"}`, newDriverToTrack))
			if err = conn.WriteMessage(websocket.TextMessage, ackMsg); err != nil {
				log.Printf("ERROR: Failed to send tracking acknowledgment to %s: %v", conn.RemoteAddr(), err)
				break // If ack fails, connection might be broken.
			}

		default:
			log.Printf("WARN: Unknown command_type '%s' from %s. Message: %s", decodedMsg.CommandType, conn.RemoteAddr(), string(msg))
			errMsg := []byte(fmt.Sprintf(`{"error": "Unknown command_type: %s"}`, decodedMsg.CommandType))
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send unknown command error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
		}
	}
	// The deferred function (conn.Close() and notifyMap cleanup) will execute when this loop breaks.
}

*/
