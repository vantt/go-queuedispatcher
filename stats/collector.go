package stats

import (
	"time"

	"github.com/vantt/go-queuedispatcher/queue"
)

// StatisticCollector ...
type StatisticCollector struct {
	Stats     *ServerStatistic
	QueueInfo queue.InterfaceQueueInfo
}

const (
	// ListTubeDelay is the time between sending list-tube to beanstalkd
	// to discover and watch newly created tubes.
	ListTubeDelay = 10 * time.Second

	// StatisticDelay is the time between sending list-tube to beanstalkd
	// to discover and watch newly created tubes.
	StatisticDelay = 10 * time.Millisecond
)

// NewStatisticCollector ...
func NewStatisticCollector(qi queue.InterfaceQueueInfo, ss *ServerStatistic) *StatisticCollector {
	return &StatisticCollector{
		Stats:     ss,
		QueueInfo: qi,
	}
}

// StartAgent statistic provide method to control CollectorAgent collect the statistics
// data in a Goroutine.
func (sc *StatisticCollector) StartAgent() {
	tick1 := instantTicker(StatisticDelay)
	tick2 := instantTicker(ListTubeDelay)

	go func() {
		for {
			select {
			case <-tick1:
				sc.updateStatistic()

			case <-tick2:
				sc.watchNewQueues()
			}
		}
	}()
}

// ShutdownAgent ...
func (sc *StatisticCollector) ShutdownAgent() {
	close(sc.Stats.UpdateChan)
}

// WatchNewQueues ...
func (sc *StatisticCollector) watchNewQueues() {
	queues, err := sc.QueueInfo.ListQueues()

	if err == nil {
		for _, queueName := range queues {
			sc.Stats.AddQueue(queueName)
		}
	}
}

// WatchNewQueues ...
func (sc *StatisticCollector) updateStatistic() {
	for _, queueName := range sc.Stats.GetQueueNames() {
		total, err := sc.QueueInfo.CountMessages(queueName)

		if err == nil {
			if stat, found := sc.Stats.GetQueue(queueName); found {
				stat.updateTotalItems(total)
			}
		}
	}

	sc.Stats.UpdateChan <- true
}

// Like time.Tick() but also fires immediately.
func instantTicker(t time.Duration) <-chan time.Time {
	c := make(chan time.Time)
	ticker := time.NewTicker(t)

	go func() {
		c <- time.Now()
		for t := range ticker.C {
			c <- t
		}
	}()

	return c
}
