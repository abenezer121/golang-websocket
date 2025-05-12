package handlers

import (
	"fastsocket/models"
	"fmt"
	"net/http"
	"runtime"
	"time"
)

func MetricsHandler(m *models.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		now := time.Now()
		uptime := now.Sub(m.StartTime)

		fmt.Fprintf(w, "# Go WebSocket Server Metrics\n")
		fmt.Fprintf(w, "websocket_server_uptime_seconds %f\n", uptime.Seconds())

		fmt.Fprintf(w, "# HELP websocket_server_upgrades_success_total Total successful WebSocket upgrades.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_upgrades_success_total counter\n")
		fmt.Fprintf(w, "websocket_server_upgrades_success_total %d\n", m.UpgradesSuccess.Load())

		fmt.Fprintf(w, "# HELP websocket_server_upgrades_failed_total Total failed WebSocket upgrades.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_upgrades_failed_total counter\n")
		fmt.Fprintf(w, "websocket_server_upgrades_failed_total %d\n", m.UpgradesFailed.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_current Current number of active WebSocket connections.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_current gauge\n")
		fmt.Fprintf(w, "websocket_server_connections_current %d\n", m.CurrentConnections.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_total Total WebSocket connections handled since start.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_total %d\n", m.TotalConnections.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_total Total connections closed since start.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_total %d\n", m.ConnectionsClosed.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_by_peer_total Connections closed by the client/peer.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_by_peer_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_by_peer_total %d\n", m.ConnectionsClosedByPeer.Load())

		fmt.Fprintf(w, "# HELP websocket_server_connections_closed_by_server_total Connections closed by the server (errors, timeouts, etc.).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_connections_closed_by_server_total counter\n")
		fmt.Fprintf(w, "websocket_server_connections_closed_by_server_total %d\n", m.ConnectionsClosedByServer.Load())

		fmt.Fprintf(w, "# HELP websocket_server_messages_received_total Total WebSocket messages received.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_messages_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_messages_received_total %d\n", m.MessagesReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_messages_sent_total Total WebSocket messages sent (includes control frames like pong).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_messages_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_messages_sent_total %d\n", m.MessagesSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_bytes_received_total Total bytes received in WebSocket messages.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_bytes_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_bytes_received_total %d\n", m.BytesReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_bytes_sent_total Total bytes sent in WebSocket messages.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_bytes_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_bytes_sent_total %d\n", m.BytesSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_pings_sent_total Total keep-alive pings sent by the server.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_pings_sent_total counter\n")
		fmt.Fprintf(w, "websocket_server_pings_sent_total %d\n", m.PingsSent.Load())

		fmt.Fprintf(w, "# HELP websocket_server_pongs_received_total Total pongs received from clients.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_pongs_received_total counter\n")
		fmt.Fprintf(w, "websocket_server_pongs_received_total %d\n", m.PongsReceived.Load())

		fmt.Fprintf(w, "# HELP websocket_server_epoll_errors_total Total errors encountered in the epoll wait loop.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_epoll_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_epoll_errors_total %d\n", m.EpollErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_read_errors_total Total errors during connection reads (excluding EOF/expected closes).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_read_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_read_errors_total %d\n", m.ReadErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_write_errors_total Total errors during connection writes.\n")
		fmt.Fprintf(w, "# TYPE websocket_server_write_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_write_errors_total %d\n", m.WriteErrors.Load())

		fmt.Fprintf(w, "# HELP websocket_server_processing_errors_total Total errors during message processing (after read, before/during write).\n")
		fmt.Fprintf(w, "# TYPE websocket_server_processing_errors_total counter\n")
		fmt.Fprintf(w, "websocket_server_processing_errors_total %d\n", m.ProcessingErrors.Load())

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines that currently exist.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
		fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", memStats.Alloc)
		fmt.Fprintf(w, "# HELP go_memstats_sys_bytes Number of bytes obtained from system.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_sys_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_sys_bytes %d\n", memStats.Sys)
	}
}
