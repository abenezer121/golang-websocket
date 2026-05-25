package handlers

import (
	"fastsocket/models"
	"fmt"
	"net/http"
	"runtime"
	"time"
)

func GRPCMetricsHandler(m *models.GRPCMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		now := time.Now()
		uptime := now.Sub(m.StartTime)

		fmt.Fprintf(w, "# Go gRPC Server Metrics\n")
		fmt.Fprintf(w, "grpc_server_uptime_seconds %f\n", uptime.Seconds())

		fmt.Fprintf(w, "# HELP grpc_server_connections_current Current number of active gRPC connections.\n")
		fmt.Fprintf(w, "# TYPE grpc_server_connections_current gauge\n")
		fmt.Fprintf(w, "grpc_server_connections_current %d\n", m.CurrentConnections.Load())

		fmt.Fprintf(w, "# HELP grpc_server_connections_total Total gRPC connections handled since start.\n")
		fmt.Fprintf(w, "# TYPE grpc_server_connections_total counter\n")
		fmt.Fprintf(w, "grpc_server_connections_total %d\n", m.TotalConnections.Load())

		fmt.Fprintf(w, "# HELP grpc_server_messages_received_total Total gRPC messages received.\n")
		fmt.Fprintf(w, "# TYPE grpc_server_messages_received_total counter\n")
		fmt.Fprintf(w, "grpc_server_messages_received_total %d\n", m.MessagesReceived.Load())

		fmt.Fprintf(w, "# HELP grpc_server_messages_sent_total Total gRPC messages sent.\n")
		fmt.Fprintf(w, "# TYPE grpc_server_messages_sent_total counter\n")
		fmt.Fprintf(w, "grpc_server_messages_sent_total %d\n", m.MessagesSent.Load())

		fmt.Fprintf(w, "# HELP grpc_server_processing_errors_total Total errors during gRPC message processing.\n")
		fmt.Fprintf(w, "# TYPE grpc_server_processing_errors_total counter\n")
		fmt.Fprintf(w, "grpc_server_processing_errors_total %d\n", m.ProcessingErrors.Load())

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines that currently exist.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
		fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.\n")
		fmt.Fprintf(w, "# TYPE go_memstats_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_memstats_alloc_bytes %d\n", memStats.Alloc)
	}
}
