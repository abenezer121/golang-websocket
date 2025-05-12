package epoll

import (
	"log"
	"sync"
	"time"
)

type Pool struct {
	work chan func()   // channel to send tasks to idle workers
	sem  chan struct{} // limit the number of active worker goroutines
	wg   sync.WaitGroup
}

func NewPool(size int) *Pool {
	if size <= 0 {
		size = 1
	}
	return &Pool{
		work: make(chan func(), size),
		sem:  make(chan struct{}, size),
	}
}

func (p *Pool) internalWorker(initialTask func()) {
	defer func() {
		<-p.sem // release the semaphore slot
		p.wg.Done()
		log.Println("gopool : worker exiting")
	}()

	log.Println("gopool: worker started, executing initial task")
	initialTask()
	// Continuously process tasks from the work channel until it's closed.
	for task := range p.work {
		if task != nil {
			task()
		}
	}

}

func (p *Pool) Schedule(task func()) {
	if task == nil {
		log.Println("gopool: schedule task is nil")
		return
	}

	select {
	case p.work <- task:
		// If there's a worker already waiting, send the task to it
		return

	case p.sem <- struct{}{}:
		p.wg.Add(1)
		go p.internalWorker(task)
		return
	default:
		// Todo a way to prevent retry storm
		// or maybe add task array that will store the values and
		go func() {
			time.Sleep(300 * time.Millisecond)
			p.Schedule(task)
		}()
	}
}

func (p *Pool) Shutdown() {
	close(p.work)
	p.wg.Wait()
}
