package handlers

import (
	"errors"
	"fastsocket/epoll"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"syscall"
)

func WsHander(upgrader websocket.Upgrader, w http.ResponseWriter, r *http.Request, ep *epoll.Epoll) {
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
	log.Printf("WebSocket connection established and added to epoll.")
}
