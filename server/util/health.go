package util

//func StartConnectionHealthCheck(ep *models.Epoll, ctx context.Context, interval time.Duration) {
//	if interval <= 0 {
//		log.Println("Connection health check (ping) disabled.")
//		return
//	}
//
//	log.Printf("Starting connection health check (ping interval: %s)", interval)
//	ticker := time.NewTicker(interval)
//	defer ticker.Stop()
//	defer log.Println("Connection health check stopped.")
//
//	for {
//		select {
//		case <-ctx.Done():
//			return // Shutdown signal
//		case <-ticker.C:
//			// log.Println("DEBUG: Running periodic health check (ping)")
//			ep.Connections.Range(func(key, value interface{}) bool {
//				select {
//				case <-ctx.Done(): // Check shutdown again inside loop
//					return false // Stop iteration
//				default:
//				}
//
//				fd := key.(int)
//				conn := value.(*websocket.Conn)
//
//				if ep.WriteTimeout > 0 {
//					deadline := time.Now().Add(ep.WriteTimeout)
//					if err := conn.SetWriteDeadline(deadline); err != nil {
//						log.Printf("WARN: Health check: Failed to set write deadline for FD %d: %v", fd, err)
//
//					}
//				}
//				err := conn.WriteMessage(websocket.PingMessage, nil)
//				if err != nil {
//					log.Printf("Health check: Failed to send Ping to FD %d: %v. Closing connection.", fd, err)
//					ep.Metrics.WriteErrors.Add(1)
//
//					ep.DeleteAndClose(fd, conn, fmt.Sprintf("Ping failed: %v", err), false) // Closed by server
//
//					return true
//				}
//				ep.Metrics.PingsSent.Add(1)
//
//				return true
//			})
//		}
//	}
//}
