package workers

import (
	"errors"
	"sync"
	"time"
)

var ErrNotReadyYet = errors.New("not ready to process job yet")

// MiddlewareTimed is used to control the processing interval of specific queues independent from the polling interval
type MiddlewareTimed struct {
	Done        chan bool
	ticker      *time.Ticker
	queue       string
	concurrency int

	readyM   sync.Mutex
	ready    bool
	runCount int
}

// NewTickedQueue returns an instance of MiddlewareTimed and starts the ticker in a separate goroutine
func NewTickedQueue(queue string, tickInterval time.Duration, concurrency int) *MiddlewareTimed {
	tw := &MiddlewareTimed{
		ticker:      time.NewTicker(tickInterval),
		queue:       queue,
		Done:        make(chan bool),
		concurrency: concurrency,
	}
	tw.readyM.Lock()
	tw.ready = true
	tw.readyM.Unlock()
	go func(m *MiddlewareTimed) {
		for {
			select {
			case <-m.Done:
				m.ticker.Stop()
				return
			case <-m.ticker.C:
				m.readyM.Lock()
				m.ready = true
				m.readyM.Unlock()
			}
		}
	}(tw)
	return tw
}

func (m *MiddlewareTimed) Call(queue string, message *Msg, next func() error) error {
	if queue == m.queue {
		m.readyM.Lock()
		isReady := m.ready
		m.readyM.Unlock()

		if !isReady {
			return ErrNotReadyYet
		}

		m.readyM.Lock()
		m.runCount++
		if m.runCount >= m.concurrency {
			m.ready = false
			m.runCount = 0
		}
		m.readyM.Unlock()
	}

	err := next()
	// do something after each message is processed?
	return err
}
