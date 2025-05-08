package util

//
//func StartWorkers(n int, ep *models.Epoll, jobChan <-chan models.EventJob, wg *sync.WaitGroup, eventQueue *models.EventQueue) {
//	if n <= 0 {
//		n = runtime.NumCPU() * 2
//	}
//
//	log.Printf("Starting worker pool with %d goroutines", n)
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go WorkerFunc(i, ep, jobChan, wg, eventQueue)
//	}
//}
//
//func WorkerFunc(id int, ep *models.Epoll, jobChan <-chan models.EventJob, wg *sync.WaitGroup, eventQueue *models.EventQueue) {
//	defer wg.Done()
//	for {
//		select {
//		case job, ok := <-jobChan:
//			if !ok {
//				log.Printf("Worker %d stopping: Job channel closed.", id)
//				return // Channel closed, time to exit
//			}
//			ep.HandleEvents(job.Fd, job.Events)
//		case <-ep.ShutdownCtx.Done():
//			log.Printf("Worker %d stopping: Shutdown signal received.", id)
//			return
//
//		default:
//			if job, ok := eventQueue.Dequeue(); ok {
//				ep.HandleEvents(job.Fd, job.Events)
//			} else {
//				// Small sleep to prevent busy-waiting
//				time.Sleep(20 * time.Millisecond)
//			}
//		}
//	}
//}
