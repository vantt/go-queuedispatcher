package stats

import (
	"fmt"
	"sync"
	"time"

	"github.com/vantt/go-queuedispatcher/queue"
)

const (
	// ListTubeDelay is the time between sending list-tube to beanstalkd
	// to discover and watch newly created tubes.
	ListTubeDelay = 10 * time.Second

	// StatisticDelay is the time between a new statistic collecting
	StatisticDelay = 3 * time.Second
)

// StatisticAgent ...
type StatisticAgent struct {
	stats     *ServerStatistic
	queueInfo queue.InterfaceQueueConnectionPool
}

// NewStatisticAgent ...
func NewStatisticAgent(qi queue.InterfaceQueueConnectionPool) *StatisticAgent {
	return &StatisticAgent{
		queueInfo: qi,
		stats:     NewServerStatistic(),
	}
}

// StartAgent statistic provide method to control CollectorAgent collect the statistics
// data in a Goroutine.
func (sc *StatisticAgent) StartAgent(done <-chan struct{}, wg *sync.WaitGroup) <-chan *ServerStatistic {
	tick1 := time.NewTicker(StatisticDelay)
	tick2 := time.NewTicker(ListTubeDelay)
	chanUpdate := make(chan *ServerStatistic)

	go func() {
		defer func() {
			tick1.Stop()
			tick2.Stop()
			close(chanUpdate)
			wg.Done()
			fmt.Println("QUIT Statistic Agent")
		}()

		for {
			select {
			case <-tick1.C:
				if ss := sc.collectStatistic(); ss.NotEmpty() {
					chanUpdate <- ss
				}

			case <-tick2.C:
				sc.watchNewQueues()

			case <-done:
				return
			}
		}
	}()

	return chanUpdate
}

// UpdateJobCost ...
func (sc *StatisticAgent) UpdateJobCost(queueName string, jobCost float64) {
	if queueStat, found := sc.stats.GetQueue(queueName); found {
		queueStat.UpdateCost(jobCost)
	}
}

func (sc *StatisticAgent) watchNewQueues() {
	queues, err := sc.queueInfo.ListQueues()

	if err == nil {
		for _, queueName := range queues {
			sc.stats.AddQueueName(queueName)
		}
	}
}

func (sc *StatisticAgent) collectStatistic() *ServerStatistic {
	ss := NewServerStatistic()

	for _, queueName := range sc.stats.GetQueueNames() {
		if queueStat, found := sc.stats.GetQueue(queueName); found {
			total, err := sc.queueInfo.CountMessages(queueName)
			if err == nil {
				queueStat.updateTotalItems(total)

				if total > 0 {
					ss.AddQueueStat(queueStat)
				}
			}
		}
	}

	return ss
}
