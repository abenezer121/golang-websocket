package epoll

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/config"
	redis2 "fastsocket/external/redis"
	"fastsocket/models"
	"fastsocket/util"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Epoll struct {
	Fd           int
	Connections  sync.Map
	Metrics      *models.Metrics
	redis        *redis.Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	WorkerChan  chan<- models.EventJob
	ShutdownCtx context.Context
	ShutdownWg  sync.WaitGroup // to wait for the epoll loop
	// roga    core.Roga

	// notify map
	NotifyMap      map[string][]*websocket.Conn
	NotifyMapMutex *sync.RWMutex

	// to track drivers
	DriverTrackMap      map[*websocket.Conn][]string
	DriverTrackMapMutex *sync.RWMutex
}

func NewEpoll(workerChan chan<- models.EventJob, shutdownCtx context.Context, m *models.Metrics, rt, wt time.Duration, notifyMap map[string][]*websocket.Conn, notifyMapMutex *sync.RWMutex, rd *redis.Client, driverTrackMap map[*websocket.Conn][]string, driverTrackMapMutex *sync.RWMutex) (*Epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	log.Printf("Created epoll instance with fd: %d", fd)
	e := &Epoll{
		Fd:                  fd,
		Metrics:             m,
		WorkerChan:          workerChan,
		ShutdownCtx:         shutdownCtx,
		ReadTimeout:         rt,
		WriteTimeout:        wt,
		redis:               rd,
		NotifyMap:           notifyMap,
		NotifyMapMutex:      notifyMapMutex,
		DriverTrackMap:      driverTrackMap,
		DriverTrackMapMutex: driverTrackMapMutex,
	}
	e.ShutdownWg.Add(1)
	go e.Wait()
	return e, nil
}

// Todo not to do just warning dont use pipe for the long term location store
func (ep *Epoll) UpdateWorkersLocation(conn *websocket.Conn, lat, lng float64, at time.Time) error {

	workerId, ok := ep.DriverTrackMap[conn]
	if !ok {
		return fmt.Errorf("driver track not found")
	}

	if len(workerId) < 2 {
		return fmt.Errorf("driver track id too short")
	}

	ctx := context.Background()
	now := time.Now()
	nowStr := now.Format(time.RFC3339)
	nowUnixStr := strconv.FormatInt(now.Unix(), 10)

	pipe := ep.redis.Pipeline()
	ep.NotifyMapMutex.RLock()
	conns, ok := ep.NotifyMap[workerId[0]]
	ep.NotifyMapMutex.RUnlock()

	// response for tracking
	if ok {
		broken := []int{} // collect indexes of dead connections
		update := models.LocationUpdate{
			WorkerID:  workerId[0],
			Latitude:  lat,
			Longitude: lng,
			Timestamp: nowStr,
			Session:   workerId[1],
			UnixTime:  nowUnixStr,
		}

		response := models.SocketResponse{
			Command:    "track",
			DriverData: update,
		}

		msg, err := json.Marshal(response)
		if err != nil {
			return fmt.Errorf("json marshal error: %w", err)
		}

		for i, conn := range conns {
			err := conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Printf("Failed to write to connection for worker %s: %v", workerId, err)
				broken = append(broken, i)
			}
		}
		// clean up broken connections
		if len(broken) > 0 {
			ep.NotifyMapMutex.Lock()
			alive := make([]*websocket.Conn, 0, len(conns)-len(broken))
			for i, conn := range conns {
				skip := false
				for _, badIndex := range broken {
					if i == badIndex {
						skip = true
						break
					}
				}
				if !skip {
					alive = append(alive, conn)
				}
			}

			if len(alive) > 0 {
				ep.NotifyMap[workerId[0]] = alive
			} else {
				delete(ep.NotifyMap, workerId[0])
			}
			ep.NotifyMapMutex.Unlock()
		}
	}

	// update workers geo location
	pipe.GeoAdd(ctx, config.WorkerLocationSet, &redis.GeoLocation{
		Name:      workerId[0],
		Latitude:  lat,
		Longitude: lng,
	})

	var workerToStore models.Command
	existingData, err := ep.redis.HGet(ctx, config.WorkerDetailsHash, workerId[0]).Result()
	if err != nil && !errors.Is(err, redis.Nil) {

		log.Printf("Error fetching existing worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to fetch existing worker details: %w", err)
	}

	if errors.Is(err, redis.Nil) {
		active := true
		workerToStore = models.Command{
			DriverId: &workerId[0],
			Lat:      &lat,
			Lng:      &lng,
			At:       &at,
			Active:   &active,
			Session:  &workerId[1],
		}
	} else {
		var existingWorker models.Command
		if err := json.Unmarshal([]byte(existingData), &existingWorker); err != nil {
			log.Printf("Error unmarshalling existing worker data for %s: %v\n", workerId, err)
			active := true
			workerToStore = models.Command{
				DriverId: &workerId[0],
				Lat:      &lat,
				Lng:      &lng,
				At:       &at,
				Active:   &active,
				Session:  &workerId[1],
			}
		} else {
			active := true
			workerToStore = existingWorker
			workerToStore.Lat = &lat
			workerToStore.Lng = &lng
			workerToStore.At = &at
			workerToStore.Active = &active
			workerToStore.Session = &workerId[1]
		}
	}

	// Store worker details
	workerJSON, err := json.Marshal(workerToStore)
	if err != nil {
		log.Printf("Error marshalling worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to marshal worker details: %w", err)
	}

	pipe.HSet(ctx, config.WorkerDetailsHash, workerId[0], string(workerJSON))
	pipe.Expire(ctx, config.WorkerDetailsHash, 5*time.Second)
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error executing Redis pipeline for UpdateWorkersLocation (Worker: %s): %v\n", workerId, err)
		fmt.Println(err)
		return fmt.Errorf("redis pipeline execution failed: %w", err)
	}
	return err

}

func (ep *Epoll) Add(conn *websocket.Conn) error {

	fd, err := util.GetFd(conn)
	if err != nil {
		log.Printf("ERROR: Failed to get FD for connection: %v", err)
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	// Add connection FD to epoll with edge-triggered monitoring for `read`, write readiness, and peer close.
	// EPOLLET (Edge Triggered): Notify only on changes. Requires careful reading/writing all available data.
	// EPOLLIN: Monitor for read readiness.
	// EPOLLOUT: Monitor for write readiness (useful for handling backpressure, currently write handling is basic).
	// EPOLLRDHUP: Monitor for peer closing connection or half-closing write side. Robust way to detect closure.
	// EPOLLERR: Monitor for errors (implicitly monitored, but good to include).
	// EPOLLHUP: Monitor for hangup (implicitly monitored).

	// Add to epoll with edge-triggered mode
	err = unix.EpollCtl(ep.Fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP | unix.EPOLLET | unix.EPOLLERR | unix.EPOLLHUP,
		Fd:     int32(fd),
	})
	if err != nil {

		log.Printf("ERROR: Failed to add FD %d to epoll: %v", fd, err)
		return fmt.Errorf("epoll_ctl add failed: %w", err)

	}

	ep.Connections.Store(fd, conn)
	count := ep.Metrics.CurrentConnections.Add(1)
	ep.Metrics.TotalConnections.Add(1)
	log.Printf("New connection: Total count %d\n", count)
	if ep.ReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
			log.Printf("WARN: Failed to set initial read deadline for FD %d: %v", fd, err)

		}
	}

	return nil
}

func (ep *Epoll) Delete(fd int) error {
	// EPOLL_CTL_DEL automatically removes the fd from all associated event queues.
	err := unix.EpollCtl(ep.Fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		// ENOENT or EBADF means it was already or closed]
		log.Printf("WARN: Epoll Ctl DEL error for FD %d: %v (may be benign if already closed)", fd, err)
		return nil
	}
	_, loaded := ep.Connections.LoadAndDelete(fd)
	if loaded {
		newCount := ep.Metrics.CurrentConnections.Add(-1)
		ep.Metrics.ConnectionsClosed.Add(1)
		log.Printf("FD %d removed from epoll and connection map. Remaining connections: %d\n", fd, newCount)
	} else {
		log.Printf("WARN: Attempted to delete FD %d from epoll map, but it was not found.\n", fd)

	}

	return nil

}

func (ep *Epoll) DeleteAndClose(fd int, conn *websocket.Conn, reason string, byPeer bool) {
	_ = ep.Delete(fd)
	if conn != nil {

		err := conn.Close()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("WARN: Error closing connection for FD %d: %v", fd, err)
			}
		} else {
			log.Printf("Connection closed: FD=%d, Reason=%s", fd, reason)
		}
	} else {
		log.Printf("WARN: deleteAndClose called for FD %d with nil connection.", fd)
	}

	if byPeer {
		ep.Metrics.ConnectionsClosedByPeer.Add(1)
	} else {
		ep.Metrics.ConnectionsClosedByServer.Add(1)
	}

}

func (ep *Epoll) Wait() {
	defer ep.ShutdownWg.Done()
	defer log.Println("Epoll wait loop stopped.")
	log.Println("Starting epoll wait loop for direct event processing...")

	events := make([]unix.EpollEvent, 128) // Buffer for epoll events

	for {
		select {
		case <-ep.ShutdownCtx.Done():
			log.Println("Shutdown signal received, stopping epoll wait loop.")
			return
		default:
		}

		n, err := unix.EpollWait(ep.Fd, events, models.EpollWaitTimeout)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if errors.Is(err, unix.EBADF) {
				log.Println("ERROR: EpollWait returned EBADF, epoll FD likely closed. Stopping loop.")
				return
			}
			log.Printf("ERROR: EpollWait failed: %v", err)
			ep.Metrics.EpollErrors.Add(1)

			select {
			case <-time.After(100 * time.Millisecond):
			case <-ep.ShutdownCtx.Done():
				return
			}
			continue
		}

		// Process events concurrently for better handling of multiple clients
		for i := 0; i < n; i++ {
			ev := &events[i]
			fd := int(ev.Fd)
			eventFlags := ev.Events

			select {
			case <-ep.ShutdownCtx.Done():
				return
			default:
				// make this go routing pool
				go func(fd int, flags uint32) {
					ep.HandleEvents(fd, flags)
				}(fd, eventFlags)
			}
		}
	}
}
func (ep *Epoll) HandleEvents(fd int, events uint32) {

	connVal, ok := ep.Connections.Load(fd)
	if !ok {
		log.Printf("WARN: HandleEvents: No connection found for FD %d. Events: 0x%x. Possibly already closed/removed.", fd, events)
		_ = ep.Delete(fd)
		return
	}
	conn := connVal.(*websocket.Conn)

	if events&unix.EPOLLIN != 0 {
		ep.HandleRead(fd, conn)
		return

	}

	if events&unix.EPOLLHUP != 0 || events&unix.EPOLLERR != 0 {

		log.Printf("Client disconnected (fd: %d)", fd)
		err := ep.Delete(fd)
		if err != nil {
		}
		return
	}
}

// In Edge-Triggered (EPOLLET) mode, we must read until EAGAIN/EWOULDBLOCK.
func (ep *Epoll) HandleRead(fd int, conn *websocket.Conn) {
	for {
		if _, stillConnected := ep.Connections.Load(fd); !stillConnected {
			log.Printf("DEBUG: HandleRead: Connection for FD %d (%s) no longer in map, exiting read loop.", fd, conn.RemoteAddr())
			return
		}

		// Set read deadline for this specific read operation.
		if ep.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
				log.Printf("WARN: HandleRead: Failed to set read deadline for FD %d (%s): %v. Closing.", fd, conn.RemoteAddr(), err)
				ep.Metrics.ReadErrors.Add(1)
				ep.DeleteAndClose(fd, conn, fmt.Sprintf("SetReadDeadline failed: %v", err), false)
				return // Exit read loop and HandleRead
			}
		}

		log.Printf("DEBUG: HandleRead FD %d (%s): Attempting ReadMessage", fd, conn.RemoteAddr())
		msgType, msg, err := conn.ReadMessage()

		if err != nil {

			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				log.Printf("DEBUG: Read on FD %d (%s) returned EAGAIN/EWOULDBLOCK (expected in ET if all data read)", fd, conn.RemoteAddr())
				return
			}

			// Check for common client-initiated close errors
			isPeerClose := websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
				websocket.CloseNoStatusReceived,
			) || errors.Is(err, io.EOF)

			errMsg := fmt.Sprintf("ReadMessage error: %v", err)
			if isPeerClose {

				errMsg = fmt.Sprintf("Peer closed connection during read: %v", err)
			} else if errors.Is(err, net.ErrClosed) {
				errMsg = "Connection already closed" // This might happen if closed by another goroutine (e.g. health check)
			}

			log.Printf("INFO: Closing connection for FD %d (%s) due to: %s", fd, conn.RemoteAddr(), errMsg)
			ep.Metrics.ReadErrors.Add(1)
			ep.DeleteAndClose(fd, conn, errMsg, isPeerClose)
			return
		}

		ep.Metrics.MessagesReceived.Add(1)
		ep.Metrics.BytesReceived.Add(int64(len(msg)))

		switch msgType {
		case websocket.TextMessage, websocket.BinaryMessage:
			var worker models.Command
			jsonErr := json.Unmarshal(msg, &worker)
			if jsonErr != nil {
				log.Printf("ERROR: Failed to unmarshal message on FD %d (%s): %v. Content: %s. Closing.", fd, conn.RemoteAddr(), jsonErr, string(msg))
				ep.Metrics.ProcessingErrors.Add(1)
				ep.DeleteAndClose(fd, conn, fmt.Sprintf("Message unmarshal failed: %v", jsonErr), false)
				return
			}

			fmt.Printf("Received worker data from FD %d (%s): %+v\n", fd, conn.RemoteAddr(), worker)

			if worker.CommandType != nil && *worker.CommandType == "driver-register" { // register driver for tracking
				if worker.DriverId == nil || worker.Session == nil {
					return
				}

				ep.DriverTrackMapMutex.Lock()
				ep.DriverTrackMap[conn] = []string{*worker.DriverId, *worker.Session}
				ep.DriverTrackMapMutex.Unlock()
			} else if worker.CommandType != nil {
				ep.HandleWatcherMessage(worker, conn)
			} else {

				err = ep.UpdateWorkersLocation(conn, *worker.Lat, *worker.Lng, *worker.At)
			}
		case websocket.CloseMessage:

			log.Printf("INFO: Received WebSocket Close frame from FD %d (%s). Closing connection.", fd, conn.RemoteAddr())

			ep.DeleteAndClose(fd, conn, "Received WebSocket close frame", true)
			return

		case websocket.PingMessage:

			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil { // Reset deadline after control frame
				log.Printf("WARN: HandleRead: Failed to reset read deadline after Ping for FD %d: %v. Closing.", fd, err)
				ep.DeleteAndClose(fd, conn, "Failed to set read deadline post-ping", false)
				return
			}
			continue

		case websocket.PongMessage:

			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil { // Reset deadline
				log.Printf("WARN: HandleRead: Failed to reset read deadline after Pong for FD %d: %v. Closing.", fd, err)
				ep.DeleteAndClose(fd, conn, "Failed to set read deadline post-pong", false)
				return
			}
			continue

		default:
			log.Printf("WARN: Received unknown message type %d from FD %d (%s).", msgType, fd, conn.RemoteAddr())
		}

	}
}

func (ep *Epoll) HandleWatcherMessage(decodedMsg models.Command, conn *websocket.Conn) {

	//defer func() {
	//	log.Printf("INFO: Closing connection for %s", conn.RemoteAddr())
	//	// Clean up this connection from the notifyMap if it was tracking a driver.
	//	if trackedDriverID != "" {
	//		notifyMapMutex.Lock()
	//		if conns, ok := notifyMap[trackedDriverID]; ok {
	//			// Remove the current connection (conn) from the slice of connections for the trackedDriverID
	//			updatedConns := []*websocket.Conn{}
	//			for _, c := range conns {
	//				if c != conn {
	//					updatedConns = append(updatedConns, c)
	//				}
	//			}
	//			if len(updatedConns) == 0 {
	//				delete(notifyMap, trackedDriverID) // No more trackers for this driver
	//				log.Printf("INFO: Removed last tracker for driver_id: %s (client: %s)", trackedDriverID, conn.RemoteAddr())
	//			} else {
	//				notifyMap[trackedDriverID] = updatedConns
	//				log.Printf("INFO: Removed client %s from tracking driver_id: %s. Remaining trackers: %d", conn.RemoteAddr(), trackedDriverID, len(updatedConns))
	//			}
	//		}
	//		notifyMapMutex.Unlock()
	//	}
	//	conn.Close() // Close the WebSocket connection.
	//}()

	// This is the message read loop for the successfully established WebSocket connection.

	// We only process text messages.

	// Attempt to unmarshal the JSON message into our Command struct.

	// Handle commands based on CommandType.
	switch *decodedMsg.CommandType {
	case "get-bbox":
		log.Printf("INFO: Handling 'get-bbox' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
		// Validate required fields for get-bbox.
		if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil {
			log.Printf("WARN: 'get-bbox' command from %s missing required coordinates. Message: %s", conn.RemoteAddr(), decodedMsg)
			errMsg := []byte(`{"error": "get-bbox command requires min_lat, min_lng, max_lat, max_lng"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send coordinate error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
			return
		}

		// Call your external function to find workers in the bounding box.
		paginated, err := redis2.FindWorkersInBBox(ep.redis, *decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng)
		if err != nil {
			log.Printf("ERROR: 'get-bbox' failed to find workers for %s: %v", conn.RemoteAddr(), err)
			errMsg := []byte(`{"error": "Failed to retrieve data for bounding box"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send bbox data retrieval error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
			return
		}

		// Prepare and send the response.
		newSocketResponse := models.SocketResponse{Command: "get-bbox", Paginated: paginated}
		responseBytes, marshalErr := json.Marshal(newSocketResponse)
		if marshalErr != nil {
			log.Printf("ERROR: Failed to marshal 'get-bbox' response for %s: %v", conn.RemoteAddr(), marshalErr)
			//Todo Or send an internal server error message
			return
		}
		if writeErr := conn.WriteMessage(websocket.TextMessage, responseBytes); writeErr != nil {
			log.Printf("ERROR: Failed to send 'get-bbox' response to %s: %v", conn.RemoteAddr(), writeErr)
			break
		}

	case "get-drivers":
		log.Printf("INFO: Handling 'get-drivers' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
		// Validate required fields for get-drivers.
		if decodedMsg.Page == nil {
			log.Printf("WARN: 'get-drivers' command from %s missing page number. Message: %s", conn.RemoteAddr(), decodedMsg)
			errMsg := []byte(`{"error": "get-drivers command requires a page number"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send page number error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
			return
		}

		// Call your external function to get all workers paginated.
		paginated, _, err := redis2.GetAllWorkersPaginated(ep.redis, *decodedMsg.Page, 100) // Assuming page size of 100
		if err != nil {
			log.Printf("ERROR: 'get-drivers' failed to get all workers for %s: %v", conn.RemoteAddr(), err)
			errMsg := []byte(`{"error": "Failed to retrieve drivers list"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send get-drivers data retrieval error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
			return
		}

		// Prepare and send the response.
		newSocketResponse := models.SocketResponse{Command: "get-drivers", Paginated: paginated}
		responseBytes, marshalErr := json.Marshal(newSocketResponse)
		if marshalErr != nil {
			log.Printf("ERROR: Failed to marshal 'get-drivers' response for %s: %v", conn.RemoteAddr(), marshalErr)
			return
		}
		if writeErr := conn.WriteMessage(websocket.TextMessage, responseBytes); writeErr != nil {
			log.Printf("ERROR: Failed to send 'get-drivers' response to %s: %v", conn.RemoteAddr(), writeErr)
			break
		}

	case "track-driver":
		log.Printf("INFO: Handling 'track-driver' from %s. Data: %+v", conn.RemoteAddr(), decodedMsg)
		// Validate required fields for track-driver.
		if decodedMsg.DriverId == nil || *decodedMsg.DriverId == "" {
			log.Printf("WARN: 'track-driver' command from %s missing valid driver_id. Message: %s", conn.RemoteAddr(), decodedMsg)
			errMsg := []byte(`{"error": "track-driver command requires a valid driver_id"}`)
			if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
				log.Printf("ERROR: Failed to send driver_id validation error message to %s: %v", conn.RemoteAddr(), writeErr)
				break
			}
			return
		}

		newDriverToTrack := *decodedMsg.DriverId
		ep.NotifyMapMutex.Lock()
		// TODO check before pushing
		ep.NotifyMap[newDriverToTrack] = append(ep.NotifyMap[newDriverToTrack], conn)

		ep.NotifyMapMutex.Unlock()
		// Send an acknowledgment message.
		ackMsg := []byte(fmt.Sprintf(`{"status": "now tracking driver_id %s"}`, newDriverToTrack))
		if err := conn.WriteMessage(websocket.TextMessage, ackMsg); err != nil {
			log.Printf("ERROR: Failed to send tracking acknowledgment to %s: %v", conn.RemoteAddr(), err)
			return
		}

	default:
		log.Printf("WARN: Unknown command_type '%s' from %s. Message: %s", decodedMsg.CommandType, conn.RemoteAddr(), decodedMsg)
		errMsg := []byte(fmt.Sprintf(`{"error": "Unknown command_type: %s"}`, decodedMsg.CommandType))
		if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
			log.Printf("ERROR: Failed to send unknown command error message to %s: %v", conn.RemoteAddr(), writeErr)
			break
		}
	}

}
