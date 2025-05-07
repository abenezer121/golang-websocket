package models

import (
	"context"
	"encoding/json"
	"errors"
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

const (
	WorkerLocationSet  = "workers:locations" // Geo set for worker locations
	WorkerDetailsHash  = "workers:details"   // Hash storing worker details
	WorkerActivityHash = "workers:activity"  // Hash storing last activity timestamps
	WorkerActiveSet    = "workers:active"    // Set of active worker IDs
	ActivityTimeout    = 5 * time.Minute     // Time after which worker is considered inactive
)

type Epoll struct {
	Fd           int
	Connections  sync.Map
	Metrics      *Metrics
	redis        *redis.Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	WorkerChan  chan<- EventJob
	ShutdownCtx context.Context
	ShutdownWg  sync.WaitGroup // to wait for the epoll loop
	// roga    core.Roga

	// notify map
	NotifyMap      map[string][]*websocket.Conn
	NotifyMapMutex *sync.RWMutex
}

func NewEpoll(workerChan chan<- EventJob, shutdownCtx context.Context, m *Metrics, rt, wt time.Duration, notifyMap map[string][]*websocket.Conn, notifyMapMutex *sync.RWMutex, rd *redis.Client) (*Epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	log.Printf("Created epoll instance with fd: %d", fd)
	e := &Epoll{
		Fd:             fd,
		Metrics:        m,
		WorkerChan:     workerChan,
		ShutdownCtx:    shutdownCtx,
		ReadTimeout:    rt,
		WriteTimeout:   wt,
		redis:          rd,
		NotifyMap:      notifyMap,
		NotifyMapMutex: notifyMapMutex,
	}
	e.ShutdownWg.Add(1)
	go e.Wait()
	return e, nil
}

func (ep *Epoll) UpdateWorkersLocation(workerId string, lat, lng float64) error {
	ctx := context.Background()

	now := time.Now()
	nowStr := now.Format(time.RFC3339)
	nowUnixStr := strconv.FormatInt(now.Unix(), 10)

	pipe := ep.redis.Pipeline()
	ep.NotifyMapMutex.RLock()
	conns, ok := ep.NotifyMap[workerId]
	ep.NotifyMapMutex.RUnlock()

	if ok {
		broken := []int{} // collect indexes of dead connections
		update := LocationUpdate{
			WorkerID:  workerId,
			Latitude:  lat,
			Longitude: lng,
			Timestamp: nowStr,
			UnixTime:  nowUnixStr,
		}

		response := SocketResponse{
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
				ep.NotifyMap[workerId] = alive
			} else {
				delete(ep.NotifyMap, workerId)
			}
			ep.NotifyMapMutex.Unlock()
		}
	}
	// update workers geo location
	pipe.GeoAdd(ctx, WorkerLocationSet, &redis.GeoLocation{
		Name:      workerId,
		Latitude:  lat,
		Longitude: lng,
	})

	var workerToStore WorkersToTrack
	existingData, err := ep.redis.HGet(ctx, WorkerDetailsHash, workerId).Result()
	if err != nil && !errors.Is(err, redis.Nil) {

		log.Printf("Error fetching existing worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to fetch existing worker details: %w", err)
	}

	if errors.Is(err, redis.Nil) {
		workerToStore = WorkersToTrack{
			Id:        workerId,
			Lat:       lat,
			Lng:       lng,
			CreatedAt: nowStr,
			UpdatedAt: nowStr,

			Active: true,
		}
	} else {
		var existingWorker WorkersToTrack
		if err := json.Unmarshal([]byte(existingData), &existingWorker); err != nil {
			log.Printf("Error unmarshalling existing worker data for %s: %v\n", workerId, err)

			workerToStore = WorkersToTrack{
				Id:        workerId,
				Lat:       lat,
				Lng:       lng,
				CreatedAt: nowStr,
				UpdatedAt: nowStr,
				Active:    true,
			}
		} else {

			workerToStore = existingWorker
			workerToStore.Lat = lat
			workerToStore.Lng = lng
			workerToStore.UpdatedAt = nowStr
			workerToStore.Active = true

		}
	}

	// Store worker details
	workerJSON, err := json.Marshal(workerToStore)
	if err != nil {
		log.Printf("Error marshalling worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to marshal worker details: %w", err)
	}

	pipe.HSet(ctx, WorkerDetailsHash, workerId, string(workerJSON))
	pipe.HSet(ctx, WorkerActivityHash, workerId, nowUnixStr)
	pipe.SAdd(ctx, WorkerActiveSet, workerId)

	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error executing Redis pipeline for UpdateWorkersLocation (Worker: %s): %v\n", workerId, err)
		return fmt.Errorf("redis pipeline execution failed: %w", err)
	}
	return err

}

func (ep *Epoll) CheckAndUpdateWorkerStatus() error {
	ctx := context.Background()
	now := time.Now()
	cutoff := now.Add(-ActivityTimeout).Unix()

	// Get all workers
	activeWorkerIds, err := ep.redis.SMembers(ctx, WorkerActiveSet).Result()
	if err != nil {
		log.Printf("Error fetching active workers from %s: %v\n", WorkerActiveSet, err)
		return fmt.Errorf("failed to get active workers: %w", err)
	}
	if len(activeWorkerIds) == 0 {
		return nil
	}

	// Fetch last activity timestamps for all potentially active workers
	activityTimestamps, err := ep.redis.HMGet(ctx, WorkerActivityHash, activeWorkerIds...).Result()
	if err != nil {
		log.Printf("Error fetching activity timestamps from %s: %v\n", WorkerActivityHash, err)
		return fmt.Errorf("failed to get activity timestamps: %w", err)
	}

	pipe := ep.redis.Pipeline()
	inactiveWorkerIds := []string{}
	workersToUpdateDetails := make(map[string]string) // workerId -> updated JSON

	for i, workerId := range activeWorkerIds {
		if i >= len(activityTimestamps) || activityTimestamps[i] == nil {
			// Missing activity timestamp, maybe an inconsistency? Mark inactive for safety.
			log.Printf("Worker %s found in active set but missing activity timestamp. Marking inactive.\n", workerId)
			inactiveWorkerIds = append(inactiveWorkerIds, workerId)
			continue // Skip fetching details for this one initially, handle removal below
		}

		lastSeenStr, ok := activityTimestamps[i].(string)
		if !ok {
			log.Printf("Unexpected type for activity timestamp for worker %s: %T. Marking inactive.\n", workerId, activityTimestamps[i])
			inactiveWorkerIds = append(inactiveWorkerIds, workerId)
			continue
		}

		lastSeen, err := strconv.ParseInt(lastSeenStr, 10, 64)
		if err != nil {
			log.Printf("Error parsing activity timestamp '%s' for worker %s: %v. Marking inactive.\n", lastSeenStr, workerId, err)
			inactiveWorkerIds = append(inactiveWorkerIds, workerId)
			continue
		}

		// Check if inactive
		if lastSeen < cutoff {
			log.Printf("Worker %s timed out (last seen: %d, cutoff: %d). Marking inactive.\n", workerId, lastSeen, cutoff)
			inactiveWorkerIds = append(inactiveWorkerIds, workerId)
		}
	}

	if len(inactiveWorkerIds) == 0 {
		return nil // No workers became inactive
	}
	if len(inactiveWorkerIds) > 0 {
		detailsData, err := ep.redis.HMGet(ctx, WorkerDetailsHash, inactiveWorkerIds...).Result()
		if err != nil {
			log.Printf("Error fetching details for inactive workers from %s: %v\n", WorkerDetailsHash, err)
			// Continue to remove from active set, but details won't be updated
		} else {
			for i, workerId := range inactiveWorkerIds {
				if i < len(detailsData) && detailsData[i] != nil {
					detailStr, ok := detailsData[i].(string)
					if !ok {
						log.Printf("Unexpected type for worker detail for worker %s: %T. Skipping detail update.\n", workerId, detailsData[i])
						continue
					}

					var worker WorkersToTrack
					if err := json.Unmarshal([]byte(detailStr), &worker); err == nil {
						if worker.Active { // Only update if it's currently marked active
							worker.Active = false
							worker.UpdatedAt = now.Format(time.RFC3339) // Optionally update UpdatedAt
							if workerJSON, err := json.Marshal(worker); err == nil {
								// Prepare HSet command for the pipeline
								workersToUpdateDetails[workerId] = string(workerJSON)
							} else {
								log.Printf("Error marshalling updated inactive worker %s: %v\n", workerId, err)
							}
						}
					} else {
						log.Printf("Error unmarshalling worker details for inactive worker %s: %v\n", workerId, err)
					}
				} else {
					log.Printf("Details not found for inactive worker %s in %s\n", workerId, WorkerDetailsHash)
				}
			}
		}

		if len(inactiveWorkerIds) > 0 {
			pipe.SRem(ctx, WorkerActiveSet, inactiveWorkerIds)
		}

		if len(workersToUpdateDetails) > 0 {
			pipe.HSet(ctx, WorkerDetailsHash, workersToUpdateDetails)
		}

	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error executing Redis pipeline for CheckAndUpdateWorkerStatus: %v\n", err)
		return fmt.Errorf("redis pipeline execution failed for status update: %w", err)
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
		//e.roga.LogInfo(core.LogArgs{
		//
		//	Message: "hello",
		//	//Actor          Actor           `json:"actor"`
		//
		//})
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

		n, err := unix.EpollWait(ep.Fd, events, EpollWaitTimeout)
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
			var worker WorkersToTrack
			jsonErr := json.Unmarshal(msg, &worker)
			if jsonErr != nil {
				log.Printf("ERROR: Failed to unmarshal message on FD %d (%s): %v. Content: %s. Closing.", fd, conn.RemoteAddr(), jsonErr, string(msg))
				ep.Metrics.ProcessingErrors.Add(1)
				ep.DeleteAndClose(fd, conn, fmt.Sprintf("Message unmarshal failed: %v", jsonErr), false)
				return
			}

			fmt.Printf("Received worker data from FD %d (%s): %+v\n", fd, conn.RemoteAddr(), worker)
			err = ep.UpdateWorkersLocation(worker.Id, worker.Lat, worker.Lng)
			// If this processing is slow, it blocks the entire Epoll.Wait loop.

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
