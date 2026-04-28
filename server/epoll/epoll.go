package epoll

import (
	"context"
	"encoding/json"
	"errors"
	"fastsocket/config"
	"fastsocket/core"
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
	ConnWriteMu  sync.Map
	Metrics      *models.Metrics
	redis        *redis.Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	WorkerChan  chan models.EventJob
	ShutdownCtx context.Context
	ShutdownWg  sync.WaitGroup
	WorkerWg    sync.WaitGroup

	subscriptionsMu       sync.RWMutex
	Subscriptions         map[string]*ClientSubscription
	ConnSubscriptionIDs   map[*websocket.Conn]string
	ExplicitDriverSubs    map[string]map[string]struct{}
	BBoxSubscriptionsByCo map[string]map[string]struct{}
}

const debugRawPayloads = false

type WSSubscriber struct {
	conn *websocket.Conn
	ep   *Epoll
}

func (s *WSSubscriber) Send(msg []byte) error {
	if s == nil || s.conn == nil || s.ep == nil {
		return fmt.Errorf("websocket subscriber is not initialized")
	}
	return s.ep.writeText(s.conn, msg)
}

func (s *WSSubscriber) Close() error {
	return s.conn.Close()
}

func NewEpoll(workerChan chan models.EventJob, workerCount int, shutdownCtx context.Context, m *models.Metrics, rt, wt time.Duration, rd *redis.Client) (*Epoll, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}

	log.Printf("Created epoll instance with fd: %d", fd)
	e := &Epoll{
		Fd:                    fd,
		Metrics:               m,
		WorkerChan:            workerChan,
		ShutdownCtx:           shutdownCtx,
		ReadTimeout:           rt,
		WriteTimeout:          wt,
		redis:                 rd,
		Subscriptions:         make(map[string]*ClientSubscription),
		ConnSubscriptionIDs:   make(map[*websocket.Conn]string),
		ExplicitDriverSubs:    make(map[string]map[string]struct{}),
		BBoxSubscriptionsByCo: make(map[string]map[string]struct{}),
	}

	e.StartWorkers(workerCount)
	e.ShutdownWg.Add(1)
	go e.Wait()
	return e, nil
}

func (ep *Epoll) RegisterSSESubscription(subscriber core.Subscriber, companyID string, bbox *BoundingBox, driverIDs []string) (*ClientSubscription, error) {
	if companyID == "" {
		return nil, fmt.Errorf("company_id is required")
	}

	sub := newClientSubscription(subscriber, nil)
	sub.CompanyID = companyID

	ep.subscriptionsMu.Lock()
	ep.Subscriptions[sub.ID] = sub
	if bbox != nil {
		sub.BBox = bbox
		ep.addBBoxIndexLocked(companyID, sub.ID)
	}
	ep.subscriptionsMu.Unlock()

	if len(driverIDs) > 0 {
		ep.addExplicitDrivers(sub, driverIDs)
	}

	return sub, nil
}

func (ep *Epoll) UnregisterSubscription(id string) {
	sub := ep.removeSubscription(id)
	if sub != nil {
		_ = sub.Subscriber.Close()
	}
}

func (ep *Epoll) SendInitialSnapshot(sub *ClientSubscription) error {
	if sub == nil {
		return fmt.Errorf("subscription is nil")
	}

	driverMap := make(map[string]models.Command)

	sub.mu.RLock()
	companyID := sub.CompanyID
	bbox := sub.BBox
	explicitIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
	for driverID := range sub.ExplicitDriverIDs {
		explicitIDs = append(explicitIDs, driverID)
	}
	sub.mu.RUnlock()

	if companyID == "" {
		return fmt.Errorf("company_id is required")
	}

	if bbox != nil {
		workers, err := redis2.FindWorkersInBBoxByCompany(ep.redis, bbox.MinLat, bbox.MinLng, bbox.MaxLat, bbox.MaxLng, companyID)
		if err != nil {
			return err
		}

		sub.mu.Lock()
		sub.CurrentBBoxDriverIDs = make(map[string]struct{}, len(workers))
		for _, worker := range workers {
			sub.CurrentBBoxDriverIDs[worker.Id] = struct{}{}
			driverMap[worker.Id] = worker
		}
		sub.mu.Unlock()
	}

	for _, driverID := range explicitIDs {
		worker, found, err := redis2.GetWorkerByID(ep.redis, driverID)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if worker.CompanyId != companyID {
			continue
		}
		driverMap[worker.Id] = worker
	}

	drivers := make([]models.Command, 0, len(driverMap))
	for _, driver := range driverMap {
		drivers = append(drivers, driver)
	}

	return sendJSON(sub.Subscriber, models.SocketResponse{
		Command:   "snapshot",
		Paginated: drivers,
	})
}

func (ep *Epoll) UpdateWorkersLocation(workerId string, lat, lng float64, companyId string) error {
	ctx := context.Background()

	now := time.Now()
	nowStr := now.Format(time.RFC3339)
	nowUnixStr := strconv.FormatInt(now.Unix(), 10)

	pipe := ep.redis.Pipeline()
	pipe.GeoAdd(ctx, config.WorkerLocationSet, &redis.GeoLocation{
		Name:      workerId,
		Latitude:  lat,
		Longitude: lng,
	})

	var workerToStore models.Command
	existingData, err := ep.redis.HGet(ctx, config.WorkerDetailsHash, workerId).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("Error fetching existing worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to fetch existing worker details: %w", err)
	}

	if errors.Is(err, redis.Nil) {
		active := true
		workerToStore = models.Command{
			Id:        workerId,
			Lat:       &lat,
			Lng:       &lng,
			CompanyId: companyId,
			CreatedAt: &nowStr,
			UpdatedAt: &nowStr,
			Active:    &active,
		}
	} else {
		var existingWorker models.Command
		if err := json.Unmarshal([]byte(existingData), &existingWorker); err != nil {
			log.Printf("Error unmarshalling existing worker data for %s: %v\n", workerId, err)
			active := true
			workerToStore = models.Command{
				Id:        workerId,
				Lat:       &lat,
				Lng:       &lng,
				CompanyId: companyId,
				CreatedAt: &nowStr,
				UpdatedAt: &nowStr,
				Active:    &active,
			}
		} else {
			active := true
			workerToStore = existingWorker
			workerToStore.Lat = &lat
			workerToStore.Lng = &lng
			workerToStore.CompanyId = companyId
			workerToStore.UpdatedAt = &nowStr
			workerToStore.Active = &active
		}
	}

	workerJSON, err := json.Marshal(workerToStore)
	if err != nil {
		log.Printf("Error marshalling worker details for %s: %v\n", workerId, err)
		return fmt.Errorf("failed to marshal worker details: %w", err)
	}

	pipe.HSet(ctx, config.WorkerDetailsHash, workerId, string(workerJSON))

	if _, err = pipe.Exec(ctx); err != nil {
		log.Printf("Error executing Redis pipeline for UpdateWorkersLocation (Worker: %s): %v\n", workerId, err)
		return fmt.Errorf("redis pipeline execution failed: %w", err)
	}

	update := models.LocationUpdate{
		WorkerID:  workerId,
		Latitude:  lat,
		Longitude: lng,
		Timestamp: nowStr,
		UnixTime:  nowUnixStr,
		CompanyId: companyId,
	}

	ep.dispatchLocationUpdate(update)
	return nil
}

func (ep *Epoll) Add(conn *websocket.Conn) error {
	fd, err := util.GetFd(conn)
	if err != nil {
		log.Printf("ERROR: Failed to get FD for connection: %v", err)
		return fmt.Errorf("failed to get file descriptor: %w", err)
	}

	err = unix.EpollCtl(ep.Fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP | unix.EPOLLET | unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLONESHOT,
		Fd:     int32(fd),
	})
	if err != nil {
		log.Printf("ERROR: Failed to add FD %d to epoll: %v", fd, err)
		return fmt.Errorf("epoll_ctl add failed: %w", err)
	}

	ep.Connections.Store(fd, conn)
	ep.ConnWriteMu.Store(conn, &sync.Mutex{})
	count := ep.Metrics.CurrentConnections.Add(1)
	ep.Metrics.TotalConnections.Add(1)
	log.Printf("EPOLL ADD: fd=%d remote=%s total=%d", fd, conn.RemoteAddr(), count)

	if ep.ReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
			log.Printf("WARN: Failed to set initial read deadline for FD %d: %v", fd, err)
		}
	}

	return nil
}

func (ep *Epoll) Delete(fd int) error {
	err := unix.EpollCtl(ep.Fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
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
		ep.cleanupConnTracking(conn)
		ep.ConnWriteMu.Delete(conn)

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

func (ep *Epoll) cleanupConnTracking(conn *websocket.Conn) {
	ep.subscriptionsMu.Lock()
	subID, ok := ep.ConnSubscriptionIDs[conn]
	if !ok {
		ep.subscriptionsMu.Unlock()
		return
	}
	sub := ep.removeSubscriptionLocked(subID)
	ep.subscriptionsMu.Unlock()

	if sub != nil {
		sub.Conn = nil
	}
}

func (ep *Epoll) writeText(conn *websocket.Conn, payload []byte) error {
	if conn == nil {
		return fmt.Errorf("nil websocket connection")
	}

	muVal, ok := ep.ConnWriteMu.Load(conn)
	if !ok {
		newMu := &sync.Mutex{}
		actual, _ := ep.ConnWriteMu.LoadOrStore(conn, newMu)
		muVal = actual
	}

	mu := muVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if ep.WriteTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(ep.WriteTimeout)); err != nil {
			return err
		}
	}

	err := conn.WriteMessage(websocket.TextMessage, payload)
	if err != nil {
		ep.Metrics.WriteErrors.Add(1)
		return err
	}
	ep.Metrics.MessagesSent.Add(1)
	ep.Metrics.BytesSent.Add(int64(len(payload)))
	return nil
}

func (ep *Epoll) StartWorkers(count int) {
	for i := 0; i < count; i++ {
		ep.WorkerWg.Add(1)
		go func() {
			defer ep.WorkerWg.Done()
			for {
				select {
				case <-ep.ShutdownCtx.Done():
					return
				case job, ok := <-ep.WorkerChan:
					if !ok {
						return
					}
					ep.HandleEvents(job.Fd, job.Events)
				}
			}
		}()
	}
}

func (ep *Epoll) Wait() {
	defer ep.ShutdownWg.Done()
	defer log.Println("Epoll wait loop stopped.")

	events := make([]unix.EpollEvent, 128)
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
			continue
		}

		for i := 0; i < n; i++ {
			select {
			case <-ep.ShutdownCtx.Done():
				return
			case ep.WorkerChan <- models.EventJob{
				Fd:     int(events[i].Fd),
				Events: events[i].Events,
			}:
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

	if events&unix.EPOLLHUP != 0 || events&unix.EPOLLERR != 0 {
		log.Printf("Client disconnected (fd: %d)", fd)
		ep.DeleteAndClose(fd, conn, "EPOLLERR/EPOLLHUP", true)
		return
	}

	if events&unix.EPOLLIN != 0 {
		ep.HandleRead(fd, conn)
	}

	if _, stillConnected := ep.Connections.Load(fd); stillConnected {
		ep.rearm(fd)
	}
}

func (ep *Epoll) rearm(fd int) {
	if err := unix.EpollCtl(ep.Fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLRDHUP | unix.EPOLLET | unix.EPOLLONESHOT,
		Fd:     int32(fd),
	}); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EBADF) {
		log.Printf("WARN: failed to rearm FD %d: %v", fd, err)
		ep.Metrics.EpollErrors.Add(1)
	}
}

func (ep *Epoll) HandleRead(fd int, conn *websocket.Conn) {
	for {
		if _, stillConnected := ep.Connections.Load(fd); !stillConnected {
			log.Printf("DEBUG: HandleRead: Connection for FD %d (%s) no longer in map, exiting read loop.", fd, conn.RemoteAddr())
			return
		}

		if ep.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
				log.Printf("WARN: HandleRead: Failed to set read deadline for FD %d (%s): %v. Closing.", fd, conn.RemoteAddr(), err)
				ep.Metrics.ReadErrors.Add(1)
				ep.DeleteAndClose(fd, conn, fmt.Sprintf("SetReadDeadline failed: %v", err), false)
				return
			}
		}

		log.Printf("DEBUG: HandleRead FD %d (%s): Attempting ReadMessage", fd, conn.RemoteAddr())
		msgType, msg, err := conn.ReadMessage()
		log.Printf("DEBUG: ReadMessage returned FD %d (%s): msgType=%d err=%v bytes=%d", fd, conn.RemoteAddr(), msgType, err, len(msg))
		if debugRawPayloads && len(msg) > 0 {
			log.Printf("DEBUG: Raw payload FD %d (%s): %s", fd, conn.RemoteAddr(), string(msg))
		}

		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				log.Printf("DEBUG: Read on FD %d (%s) returned EAGAIN/EWOULDBLOCK (expected in ET if all data read)", fd, conn.RemoteAddr())
				return
			}

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
				errMsg = "Connection already closed"
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
			if err := json.Unmarshal(msg, &worker); err != nil {
				log.Printf("ERROR: Failed to unmarshal message on FD %d (%s): %v. Content: %s. Closing.", fd, conn.RemoteAddr(), err, string(msg))
				ep.Metrics.ProcessingErrors.Add(1)
				ep.DeleteAndClose(fd, conn, fmt.Sprintf("Message unmarshal failed: %v", err), false)
				return
			}

			if worker.CommandType != nil {
				log.Printf("WATCHER command received from %s on fd=%d: command=%s decoded=%+v", conn.RemoteAddr(), fd, *worker.CommandType, worker)
				if debugRawPayloads {
					log.Printf("DEBUG: WATCHER raw payload from %s on fd=%d: %s", conn.RemoteAddr(), fd, string(msg))
				}
				ep.HandleWatcherMessage(worker, conn)
			} else {
				if worker.Lat == nil || worker.Lng == nil {
					log.Printf("WARN: location update missing lat/lng on FD %d (%s)", fd, conn.RemoteAddr())
					ep.Metrics.ProcessingErrors.Add(1)
					continue
				}
				if err := ep.UpdateWorkersLocation(worker.Id, *worker.Lat, *worker.Lng, worker.CompanyId); err != nil {
					log.Printf("ERROR: failed updating worker location for FD %d (%s): %v", fd, conn.RemoteAddr(), err)
					ep.Metrics.ProcessingErrors.Add(1)
				}
			}
		case websocket.CloseMessage:
			log.Printf("INFO: Received WebSocket Close frame from FD %d (%s). Closing connection.", fd, conn.RemoteAddr())
			ep.DeleteAndClose(fd, conn, "Received WebSocket close frame", true)
			return
		case websocket.PingMessage:
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
				log.Printf("WARN: HandleRead: Failed to reset read deadline after Ping for FD %d: %v. Closing.", fd, err)
				ep.DeleteAndClose(fd, conn, "Failed to set read deadline post-ping", false)
				return
			}
			continue
		case websocket.PongMessage:
			if err := conn.SetReadDeadline(time.Now().Add(ep.ReadTimeout)); err != nil {
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
	command := *decodedMsg.CommandType
	log.Printf("WATCHER dispatch: remote=%s command=%s", conn.RemoteAddr(), command)

	sub := ep.getOrCreateWSSubscription(conn)

	switch command {
	case "get-bbox":
		if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil || decodedMsg.CompanyId == "" {
			_ = ep.writeJSONError(conn, "get-bbox command requires company_id, min_lat, min_lng, max_lat, max_lng")
			return
		}

		paginated, err := redis2.FindWorkersInBBoxByCompany(ep.redis, *decodedMsg.MinLat, *decodedMsg.MinLng, *decodedMsg.MaxLat, *decodedMsg.MaxLng, decodedMsg.CompanyId)
		if err != nil {
			log.Printf("ERROR: 'get-bbox' failed to find workers for %s: %v", conn.RemoteAddr(), err)
			_ = ep.writeJSONError(conn, "Failed to retrieve data for bounding box")
			return
		}

		_ = sendJSON(sub.Subscriber, models.SocketResponse{Command: "get-bbox", Paginated: paginated})

	case "get-drivers":
		if decodedMsg.Page == nil {
			_ = ep.writeJSONError(conn, "get-drivers command requires a page number")
			return
		}

		var (
			paginated []models.Command
			err       error
		)
		if decodedMsg.CompanyId != "" {
			paginated, _, err = redis2.GetAllWorkersPaginatedByCompany(ep.redis, *decodedMsg.Page, 100, decodedMsg.CompanyId)
		} else {
			paginated, _, err = redis2.GetAllWorkersPaginated(ep.redis, *decodedMsg.Page, 100)
		}
		if err != nil {
			log.Printf("ERROR: 'get-drivers' failed to get all workers for %s: %v", conn.RemoteAddr(), err)
			_ = ep.writeJSONError(conn, "Failed to retrieve drivers list")
			return
		}

		_ = sendJSON(sub.Subscriber, models.SocketResponse{Command: "get-drivers", Paginated: paginated})

	case "track-driver", "subscribe-driver":
		driverIDs := normalizeDriverIDs(decodedMsg)
		if len(driverIDs) == 0 || decodedMsg.CompanyId == "" {
			_ = ep.writeJSONError(conn, command+" requires company_id and at least one driver_id")
			return
		}
		if err := ep.ensureCompany(sub, decodedMsg.CompanyId); err != nil {
			_ = ep.writeJSONError(conn, err.Error())
			return
		}
		ep.addExplicitDrivers(sub, driverIDs)
		if err := ep.sendExplicitDriverSnapshot(sub, driverIDs); err != nil {
			log.Printf("ERROR: failed to send driver snapshot to %s: %v", conn.RemoteAddr(), err)
			return
		}
		_ = sendJSON(sub.Subscriber, ackResponse(command, "ok", "driver subscription updated", driverIDs))

	case "untrack-driver", "unsubscribe-driver":
		driverIDs := normalizeDriverIDs(decodedMsg)
		if len(driverIDs) == 0 {
			_ = ep.writeJSONError(conn, command+" requires at least one driver_id")
			return
		}
		ep.removeExplicitDrivers(sub, driverIDs)
		_ = sendJSON(sub.Subscriber, ackResponse(command, "ok", "driver subscription removed", driverIDs))

	case "subscribe-bbox", "update-bbox":
		if decodedMsg.MinLat == nil || decodedMsg.MinLng == nil || decodedMsg.MaxLat == nil || decodedMsg.MaxLng == nil || decodedMsg.CompanyId == "" {
			_ = ep.writeJSONError(conn, command+" requires company_id, min_lat, min_lng, max_lat, max_lng")
			return
		}
		bbox := &BoundingBox{
			MinLat: *decodedMsg.MinLat,
			MinLng: *decodedMsg.MinLng,
			MaxLat: *decodedMsg.MaxLat,
			MaxLng: *decodedMsg.MaxLng,
		}
		if err := ep.setBBoxSubscription(sub, decodedMsg.CompanyId, bbox); err != nil {
			_ = ep.writeJSONError(conn, err.Error())
			return
		}
		if err := ep.SendInitialSnapshot(sub); err != nil {
			log.Printf("ERROR: failed to send bbox snapshot to %s: %v", conn.RemoteAddr(), err)
			return
		}
		_ = sendJSON(sub.Subscriber, ackResponse(command, "ok", "bbox subscription updated", nil))

	case "clear-subscriptions":
		ep.clearSubscription(sub)
		_ = sendJSON(sub.Subscriber, ackResponse(command, "ok", "all subscriptions cleared", nil))

	default:
		_ = ep.writeJSONError(conn, fmt.Sprintf("Unknown command_type: %s", command))
	}
}

func normalizeDriverIDs(cmd models.Command) []string {
	driverSet := make(map[string]struct{})
	if cmd.DriverId != nil && *cmd.DriverId != "" {
		driverSet[*cmd.DriverId] = struct{}{}
	}
	for _, driverID := range cmd.DriverIDs {
		if driverID != "" {
			driverSet[driverID] = struct{}{}
		}
	}

	driverIDs := make([]string, 0, len(driverSet))
	for driverID := range driverSet {
		driverIDs = append(driverIDs, driverID)
	}
	return driverIDs
}

func (ep *Epoll) ensureCompany(sub *ClientSubscription, companyID string) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if companyID == "" {
		return fmt.Errorf("company_id is required")
	}
	if sub.CompanyID == "" {
		sub.CompanyID = companyID
		return nil
	}
	if sub.CompanyID != companyID {
		return fmt.Errorf("subscription is already scoped to company_id %s", sub.CompanyID)
	}
	return nil
}

func (ep *Epoll) getOrCreateWSSubscription(conn *websocket.Conn) *ClientSubscription {
	ep.subscriptionsMu.RLock()
	if subID, ok := ep.ConnSubscriptionIDs[conn]; ok {
		sub := ep.Subscriptions[subID]
		ep.subscriptionsMu.RUnlock()
		return sub
	}
	ep.subscriptionsMu.RUnlock()

	wsSub := &WSSubscriber{conn: conn, ep: ep}
	sub := newClientSubscription(wsSub, conn)

	ep.subscriptionsMu.Lock()
	defer ep.subscriptionsMu.Unlock()
	if subID, ok := ep.ConnSubscriptionIDs[conn]; ok {
		return ep.Subscriptions[subID]
	}
	ep.Subscriptions[sub.ID] = sub
	ep.ConnSubscriptionIDs[conn] = sub.ID
	return sub
}

func (ep *Epoll) addExplicitDrivers(sub *ClientSubscription, driverIDs []string) {
	for _, driverID := range driverIDs {
		sub.addExplicitDriver(driverID)
	}

	ep.subscriptionsMu.Lock()
	defer ep.subscriptionsMu.Unlock()
	for _, driverID := range driverIDs {
		if _, ok := ep.ExplicitDriverSubs[driverID]; !ok {
			ep.ExplicitDriverSubs[driverID] = make(map[string]struct{})
		}
		ep.ExplicitDriverSubs[driverID][sub.ID] = struct{}{}
	}
}

func (ep *Epoll) removeExplicitDrivers(sub *ClientSubscription, driverIDs []string) {
	for _, driverID := range driverIDs {
		sub.removeExplicitDriver(driverID)
	}

	ep.subscriptionsMu.Lock()
	defer ep.subscriptionsMu.Unlock()
	for _, driverID := range driverIDs {
		subIDs, ok := ep.ExplicitDriverSubs[driverID]
		if !ok {
			continue
		}
		delete(subIDs, sub.ID)
		if len(subIDs) == 0 {
			delete(ep.ExplicitDriverSubs, driverID)
		}
	}
}

func (ep *Epoll) setBBoxSubscription(sub *ClientSubscription, companyID string, bbox *BoundingBox) error {
	if err := ep.ensureCompany(sub, companyID); err != nil {
		return err
	}

	sub.mu.Lock()
	oldCompanyID := sub.CompanyID
	sub.BBox = bbox
	sub.CurrentBBoxDriverIDs = make(map[string]struct{})
	sub.mu.Unlock()

	ep.subscriptionsMu.Lock()
	defer ep.subscriptionsMu.Unlock()
	if oldCompanyID != "" {
		ep.removeBBoxIndexLocked(oldCompanyID, sub.ID)
	}
	ep.addBBoxIndexLocked(companyID, sub.ID)
	return nil
}

func (ep *Epoll) clearSubscription(sub *ClientSubscription) {
	driverIDs := sub.snapshotDriverIDs()
	if len(driverIDs) > 0 {
		ep.removeExplicitDrivers(sub, driverIDs)
	}

	sub.mu.Lock()
	companyID := sub.CompanyID
	sub.BBox = nil
	sub.CurrentBBoxDriverIDs = make(map[string]struct{})
	sub.CompanyID = ""
	sub.mu.Unlock()

	if companyID != "" {
		ep.subscriptionsMu.Lock()
		ep.removeBBoxIndexLocked(companyID, sub.ID)
		ep.subscriptionsMu.Unlock()
	}
}

func (ep *Epoll) sendExplicitDriverSnapshot(sub *ClientSubscription, driverIDs []string) error {
	sub.mu.RLock()
	companyID := sub.CompanyID
	sub.mu.RUnlock()

	drivers := make([]models.Command, 0, len(driverIDs))
	for _, driverID := range driverIDs {
		worker, found, err := redis2.GetWorkerByID(ep.redis, driverID)
		if err != nil {
			return err
		}
		if !found || worker.CompanyId != companyID {
			continue
		}
		drivers = append(drivers, worker)
	}

	if len(drivers) == 0 {
		return nil
	}

	return sendJSON(sub.Subscriber, models.SocketResponse{
		Command:   "snapshot-driver",
		Paginated: drivers,
	})
}

func (ep *Epoll) dispatchLocationUpdate(update models.LocationUpdate) {
	ep.subscriptionsMu.RLock()
	explicitIDs := ep.copySubIDsLocked(ep.ExplicitDriverSubs[update.WorkerID])
	bboxIDs := ep.copySubIDsLocked(ep.BBoxSubscriptionsByCo[update.CompanyId])
	subscriptions := make(map[string]*ClientSubscription, len(explicitIDs)+len(bboxIDs))
	for _, subID := range explicitIDs {
		if sub, ok := ep.Subscriptions[subID]; ok {
			subscriptions[subID] = sub
		}
	}
	for _, subID := range bboxIDs {
		if sub, ok := ep.Subscriptions[subID]; ok {
			subscriptions[subID] = sub
		}
	}
	ep.subscriptionsMu.RUnlock()

	if len(subscriptions) == 0 {
		return
	}

	eventBySubID := make(map[string]string, len(subscriptions))
	for _, subID := range explicitIDs {
		sub, ok := subscriptions[subID]
		if !ok {
			continue
		}
		if sub.companyID() == update.CompanyId {
			eventBySubID[subID] = "track"
		}
	}

	for _, subID := range bboxIDs {
		sub, ok := subscriptions[subID]
		if !ok {
			continue
		}

		sub.mu.RLock()
		bbox := sub.BBox
		sub.mu.RUnlock()
		if bbox == nil {
			continue
		}

		inside := bbox.Contains(update.Latitude, update.Longitude)
		wasInside := sub.markBBoxMembership(update.WorkerID, inside)

		switch {
		case inside:
			eventBySubID[subID] = "track"
		case wasInside:
			if _, exists := eventBySubID[subID]; !exists {
				eventBySubID[subID] = "driver-left"
			}
		}
	}

	broken := make([]string, 0)
	for subID, command := range eventBySubID {
		sub := subscriptions[subID]
		if sub == nil {
			continue
		}

		response := models.SocketResponse{
			Command:    command,
			DriverData: &update,
		}
		if err := sendJSON(sub.Subscriber, response); err != nil {
			log.Printf("Failed to write update for worker %s to subscriber %s: %v", update.WorkerID, subID, err)
			broken = append(broken, subID)
		}
	}

	for _, subID := range broken {
		ep.cleanupBrokenSubscription(subID, subscriptions[subID], update.WorkerID)
	}
}

func (ep *Epoll) cleanupBrokenSubscription(subID string, sub *ClientSubscription, workerID string) {
	if sub == nil {
		return
	}

	if sub.Conn != nil {
		fd, err := util.GetFd(sub.Conn)
		if err == nil {
			log.Printf("Cleaning up broken websocket subscriber %s for worker %s on fd=%d", subID, workerID, fd)
			ep.DeleteAndClose(fd, sub.Conn, "subscriber send failure", false)
			return
		}
		log.Printf("WARN: Failed to resolve fd for broken websocket subscriber %s: %v", subID, err)
		ep.ConnWriteMu.Delete(sub.Conn)
	}

	removed := ep.removeSubscription(subID)
	if removed == nil {
		removed = sub
	}
	if err := removed.Subscriber.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("WARN: Failed to close broken subscriber %s for worker %s: %v", subID, workerID, err)
	}
}

func (ep *Epoll) copySubIDsLocked(subIDs map[string]struct{}) []string {
	if len(subIDs) == 0 {
		return nil
	}

	ids := make([]string, 0, len(subIDs))
	for subID := range subIDs {
		ids = append(ids, subID)
	}
	return ids
}

func (ep *Epoll) addBBoxIndexLocked(companyID, subID string) {
	if _, ok := ep.BBoxSubscriptionsByCo[companyID]; !ok {
		ep.BBoxSubscriptionsByCo[companyID] = make(map[string]struct{})
	}
	ep.BBoxSubscriptionsByCo[companyID][subID] = struct{}{}
}

func (ep *Epoll) removeBBoxIndexLocked(companyID, subID string) {
	subIDs, ok := ep.BBoxSubscriptionsByCo[companyID]
	if !ok {
		return
	}
	delete(subIDs, subID)
	if len(subIDs) == 0 {
		delete(ep.BBoxSubscriptionsByCo, companyID)
	}
}

func (ep *Epoll) removeSubscription(id string) *ClientSubscription {
	ep.subscriptionsMu.Lock()
	defer ep.subscriptionsMu.Unlock()
	return ep.removeSubscriptionLocked(id)
}

func (ep *Epoll) removeSubscriptionLocked(id string) *ClientSubscription {
	sub, ok := ep.Subscriptions[id]
	if !ok {
		return nil
	}

	delete(ep.Subscriptions, id)
	if sub.Conn != nil {
		delete(ep.ConnSubscriptionIDs, sub.Conn)
	}

	sub.mu.RLock()
	companyID := sub.CompanyID
	driverIDs := make([]string, 0, len(sub.ExplicitDriverIDs))
	for driverID := range sub.ExplicitDriverIDs {
		driverIDs = append(driverIDs, driverID)
	}
	sub.mu.RUnlock()

	for _, driverID := range driverIDs {
		subIDs, ok := ep.ExplicitDriverSubs[driverID]
		if !ok {
			continue
		}
		delete(subIDs, id)
		if len(subIDs) == 0 {
			delete(ep.ExplicitDriverSubs, driverID)
		}
	}

	if companyID != "" {
		ep.removeBBoxIndexLocked(companyID, id)
	}

	return sub
}

func (ep *Epoll) writeJSONError(conn *websocket.Conn, message string) error {
	msg, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return err
	}
	return ep.writeText(conn, msg)
}
