package models

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/util"
	"fmt"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
	"io"
	"log"
	"net"
	"sort"
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

	activeWorkers sync.Map

	WorkerChan  chan<- EventJob
	ShutdownCtx context.Context
	ShutdownWg  sync.WaitGroup // to wait for the epoll loop

	// roga    core.Roga
}

func NewEpoll(workerChan chan<- EventJob, shutdownCtx context.Context, m *Metrics, rt, wt time.Duration) (*Epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	log.Printf("Created epoll instance with fd: %d", fd)
	e := &Epoll{
		Fd:           fd,
		Metrics:      m,
		WorkerChan:   workerChan,
		ShutdownCtx:  shutdownCtx,
		ReadTimeout:  rt,
		WriteTimeout: wt,
		redis:        rdb,
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

func (ep *Epoll) FindWorkersInBBox(minLat, minLng, maxLat, maxLng float64) ([]*WorkersToTrack, error) {
	ctx := context.Background()

	if minLat >= maxLat || minLng >= maxLng {
		return nil, errors.New("invalid bounding box coordinates")
	}

	centerLat := (minLat + maxLat) / 2
	centerLng := (minLng + maxLng) / 2
	height := maxLat - minLat
	width := maxLng - minLng

	searchQuery := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: centerLng,
			Latitude:  centerLat,
			BoxWidth:  width,
			BoxHeight: height,

			Count: 1000,
		},
		WithCoord: true,
		WithDist:  true,
	}

	locations, err := ep.redis.GeoSearchLocation(ctx, WorkerLocationSet, searchQuery).Result()
	if err != nil {
		// Handle case where the key doesn't exist gracefully
		if errors.Is(err, redis.Nil) {
			return []*WorkersToTrack{}, nil // No workers found is not an error
		}
		log.Printf("Error executing GeoSearchLocation: %v\n", err)
		return nil, fmt.Errorf("failed to search worker locations: %w", err)
	}

	if len(locations) == 0 {
		return []*WorkersToTrack{}, nil // No workers found in the bbox
	}

	workerIdsInBox := make([]string, 0, len(locations))
	for _, loc := range locations {
		workerIdsInBox = append(workerIdsInBox, loc.Name)
	}

	detailsData, err := ep.redis.HMGet(ctx, WorkerDetailsHash, workerIdsInBox...).Result()
	if err != nil {
		log.Printf("Error fetching worker details with HMGet: %v\n", err)
		return nil, fmt.Errorf("failed to fetch worker details: %w", err)
	}

	workers := make([]*WorkersToTrack, 0, len(locations))
	for i, data := range detailsData {
		if data == nil {
			// Worker ID found in GeoSet but not in Details Hash (potential inconsistency)
			log.Printf("Worker %s found in GeoSet but missing details in %s\n", workerIdsInBox[i], WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", workerIdsInBox[i], data)
			continue
		}

		var worker WorkersToTrack
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", workerIdsInBox[i], err)
			continue
		}
		if worker.Active {

			workers = append(workers, &worker)
		}
	}

	return workers, nil
}

func (ep *Epoll) GetAllWorkersPaginated(page, pageSize int) ([]*WorkersToTrack, int, error) {
	ctx := context.Background()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	allWorkerIds, err := ep.redis.HKeys(ctx, WorkerDetailsHash).Result()
	if err != nil {
		log.Printf("Error fetching all worker keys from %s: %v\n", WorkerDetailsHash, err)
		return nil, 0, fmt.Errorf("failed to get worker keys: %w", err)
	}

	totalWorkers := len(allWorkerIds)
	if totalWorkers == 0 {
		return []*WorkersToTrack{}, 0, nil
	}

	sort.Strings(allWorkerIds)

	start := (page - 1) * pageSize
	if start >= totalWorkers {
		return []*WorkersToTrack{}, totalWorkers, nil
	}

	end := start + pageSize
	if end > totalWorkers {
		end = totalWorkers
	}

	paginatedWorkerIds := allWorkerIds[start:end]

	if len(paginatedWorkerIds) == 0 {
		return []*WorkersToTrack{}, totalWorkers, nil
	}

	detailsData, err := ep.redis.HMGet(ctx, WorkerDetailsHash, paginatedWorkerIds...).Result()
	if err != nil {
		log.Printf("Error fetching paginated worker details with HMGet: %v\n", err)
		return nil, totalWorkers, fmt.Errorf("failed to fetch worker details for page %d: %w", page, err)
	}

	workers := make([]*WorkersToTrack, 0, len(paginatedWorkerIds))
	for i, data := range detailsData {
		if data == nil {
			log.Printf("Details not found for paginated worker %s in %s\n", paginatedWorkerIds[i], WorkerDetailsHash)
			continue
		}

		detailStr, ok := data.(string)
		if !ok {
			log.Printf("Unexpected data type for worker %s detail: %T\n", paginatedWorkerIds[i], data)
			continue
		}

		var worker WorkersToTrack
		if err := json.Unmarshal([]byte(detailStr), &worker); err != nil {
			log.Printf("Error unmarshalling worker detail for %s: %v\n", paginatedWorkerIds[i], err)
			continue
		}
		workers = append(workers, &worker)
	}

	return workers, totalWorkers, nil
}
func (ep *Epoll) Add(conn net.Conn) error {

	fd, err := util.GetFd(conn)
	if err != nil {
		log.Printf("ERROR: Failed to get FD for connection: %v", err)
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	// Add connection FD to epoll with edge-triggered monitoring for read, write readiness, and peer close.
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

func (ep *Epoll) DeleteAndClose(fd int, conn net.Conn, reason string, byPeer bool) {
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
	// signal the gorouting has finished
	defer ep.ShutdownWg.Done()
	defer log.Println("Epoll wait loop stopped.")

	log.Println("Starting epoll wait loop...")

	events := make([]unix.EpollEvent, 128)
	eventQueue := &EventQueue{}
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
			// EBADF (bad file descriptor),
			if errors.Is(err, unix.EBADF) {
				log.Println("Epoll wait returned EBADF, likely closed during shutdown.")
				return
			}
			log.Printf("ERROR: EpollWait failed: %v", err)
			ep.Metrics.EpollErrors.Add(1)

			// Prevent busy-looping on persistent errors
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ep.ShutdownCtx.Done():
				log.Println("Shutdown signal received after epoll error.")
				return
			}
			continue

		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			job := EventJob{
				Fd:     int(ev.Fd),
				Events: ev.Events,
			}
			select {
			case ep.WorkerChan <- job:
			case <-ep.ShutdownCtx.Done():
				log.Println("Shutdown while dispatching epoll event, discarding job.")
				return
			default:
				eventQueue.enqueue(job)
				// log.Printf("WARNING: Worker channel full. Queuing event for FD %d (Events: 0x%x).", job.fd, job.events)
			}
		}
	}
}

func (ep *Epoll) HandleEvents(fd int, events uint32) {
	connVal, ok := ep.Connections.Load(fd)

	if !ok {
		// Connection might have been closed and removed between event generation and processing.
		_ = ep.Delete(fd)
		return
	}
	conn := connVal.(net.Conn)

	// Check for errors or hangup events first.
	// EPOLLRDHUP indicates the peer has closed their writing end or the whole connection.
	// EPOLLHUP often indicates an unexpected close or reset.
	// EPOLLERR indicates an error on the FD.
	if events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
		log.Printf("Handling EPOLLERR/EPOLLHUP for FD %d", fd)
		ep.Metrics.ReadErrors.Add(1)                                   // Count as a read/general error
		ep.DeleteAndClose(fd, conn, "Epoll error/hangup event", false) // Server closing due to error
		return
	}

	if events&unix.EPOLLRDHUP != 0 {
		log.Printf("Handling EPOLLRDHUP for FD %d (peer closed write end or connection)", fd)
		ep.DeleteAndClose(fd, conn, "Peer closed connection (RDHUP)", true) // Closed by peer
		return
	}

	if events&unix.EPOLLOUT != 0 {
	}

	if events&unix.EPOLLIN != 0 {
		ep.HandleRead(fd, conn)
	}

}

func (ep *Epoll) HandleRead(fd int, conn net.Conn) {
	// For edge-triggered mode, we must read until we get EAGAIN/EWOULDBLOCK
	// to ensure we process all available data notified by the edge trigger.
	for {
		if ep.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
				log.Printf("WARN: Failed to set read deadline for FD %d before read: %v", fd, err)
				ep.Metrics.ReadErrors.Add(1)
				ep.DeleteAndClose(fd, conn, "Failed to set read deadline", false)
				return
			}
		}

		msg, op, err := wsutil.ReadClientData(conn)

		if err != nil {

			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				log.Printf("DEBUG: Read on FD %d returned EAGAIN/EWOULDBLOCK (expected in ET)", fd)
				return
			}

			// Handle different error types
			var reason string
			closedByPeer := false
			if errors.Is(err, io.EOF) {
				reason = "EOF (client disconnected)"
				closedByPeer = true
			} else if errors.Is(err, net.ErrClosed) {
				reason = "Connection already closed"
				ep.Delete(fd)
				return
			} else if errors.Is(err, syscall.ECONNRESET) {
				reason = "Connection reset by peer"
				ep.Metrics.ReadErrors.Add(1)
				closedByPeer = true
			} else if errors.Is(err, syscall.EPIPE) {
				reason = "Broken pipe"
				ep.Metrics.ReadErrors.Add(1)
				closedByPeer = true
			} else if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				reason = "Read timeout"
				ep.Metrics.ReadErrors.Add(1)

			} else {

				reason = fmt.Sprintf("Unexpected read error: %v", err)
				log.Printf("ERROR: %s on FD %d", reason, fd)
				ep.Metrics.ReadErrors.Add(1)

			}

			log.Printf("Closing FD %d due to read issue: %s", fd, reason)
			ep.DeleteAndClose(fd, conn, reason, closedByPeer)
			return
		}
		ep.Metrics.MessagesReceived.Add(1)
		ep.Metrics.BytesReceived.Add(int64(len(msg)))
		if err := ep.ProcessMessage(fd, conn, msg, op); err != nil {
			log.Printf("ERROR: Failed to process message on FD %d: %v. Closing connection.", fd, err)
			ep.Metrics.ProcessingErrors.Add(1)
			ep.DeleteAndClose(fd, conn, fmt.Sprintf("Message processing failed: %v", err), false)
			return
		}

	}
}

func (ep *Epoll) ProcessMessage(fd int, conn net.Conn, msg []byte, op ws.OpCode) error {

	switch op {
	case ws.OpText, ws.OpBinary:
		if ep.WriteTimeout > 0 {

			if err := conn.SetWriteDeadline(time.Now().Add(ep.WriteTimeout)); err != nil {

				return fmt.Errorf("failed to set write deadline before echo: %w", err)
			}
		}
		err := wsutil.WriteServerMessage(conn, op, msg)
		if err != nil {

			if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				return fmt.Errorf("write failed (likely closed connection): %w", err)
			} else if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				return fmt.Errorf("write timeout: %w", err)
			}

			return fmt.Errorf("failed to write echo response: %w", err)
		}
		ep.Metrics.MessagesSent.Add(1)
		ep.Metrics.BytesSent.Add(int64(len(msg)))

	case ws.OpClose:
		log.Printf("Client initiated close on FD %d", fd)
		return errors.New("client initiated close")
	case ws.OpPing:
		if ep.WriteTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(ep.WriteTimeout)); err != nil {
				return fmt.Errorf("failed to set write deadline before pong: %w", err)
			}
		}
		if err := wsutil.WriteServerMessage(conn, ws.OpPong, nil); err != nil {
			return fmt.Errorf("failed to write pong: %w", err)
		}
		ep.Metrics.MessagesSent.Add(1)

	case ws.OpPong:
		ep.Metrics.PongsReceived.Add(1)

	default:
		log.Printf("WARN: Received unknown WebSocket opcode %v on FD %d", op, fd)
		return fmt.Errorf("unknown WebSocket opcode: %v", op)
	}

	var worker WorkersToTrack
	err := json.Unmarshal(msg, &worker)
	if err != nil {
		fmt.Printf("Failed to parse worker message: %v\n", err)
		return err
	}

	err = ep.UpdateWorkersLocation(worker.Id, worker.Lat, worker.Lng)
	if err != nil {
		//return err
	}
	return nil
}

func (ep *Epoll) StartConnectionHealthCheck(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		log.Println("Connection health check (ping) disabled.")
		return
	}

	log.Printf("Starting connection health check (ping interval: %s)", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer log.Println("Connection health check stopped.")

	for {
		select {
		case <-ctx.Done():
			return // Shutdown signal
		case <-ticker.C:
			// log.Println("DEBUG: Running periodic health check (ping)")
			ep.Connections.Range(func(key, value interface{}) bool {
				select {
				case <-ctx.Done(): // Check shutdown again inside loop
					return false // Stop iteration
				default:
				}

				fd := key.(int)
				conn := value.(net.Conn)

				if ep.WriteTimeout > 0 {
					deadline := time.Now().Add(ep.WriteTimeout)
					if err := conn.SetWriteDeadline(deadline); err != nil {
						log.Printf("WARN: Health check: Failed to set write deadline for FD %d: %v", fd, err)

					}
				}

				err := wsutil.WriteServerMessage(conn, ws.OpPing, nil)
				if err != nil {
					log.Printf("Health check: Failed to send Ping to FD %d: %v. Closing connection.", fd, err)
					ep.Metrics.WriteErrors.Add(1)

					ep.DeleteAndClose(fd, conn, fmt.Sprintf("Ping failed: %v", err), false) // Closed by server

					return true
				}
				ep.Metrics.PingsSent.Add(1)

				return true
			})
		}
	}
}
