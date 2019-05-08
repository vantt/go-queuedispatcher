package stats

import (	
	"sync"
	"time"
	"context"
	"go.uber.org/zap"
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
	logger    *zap.Logger
	updateChan chan *ServerStatistic
	aliveChan chan chan struct{}
}

// NewStatisticAgent ...
func NewStatisticAgent(qi queue.InterfaceQueueConnectionPool, logger *zap.Logger) *StatisticAgent {
	return &StatisticAgent{
		queueInfo: qi,
		logger: logger,
		stats:  NewServerStatistic(),
	}
}

// IsAlive ...
func (sc *StatisticAgent) IsAlive() chan struct{} {	
	returnChan := make(chan struct{})

	go func() { sc.aliveChan <- returnChan }()

	return returnChan
}

// Start statistic provide method to control CollectorAgent collect the statistics
// data in a Goroutine.
func (sc *StatisticAgent) Start(ctx context.Context, wg *sync.WaitGroup, readyChan chan<- string) <-chan *ServerStatistic {	
	tick1 := time.NewTicker(StatisticDelay)
	tick2 := time.NewTicker(ListTubeDelay)
	sc.updateChan = make(chan *ServerStatistic)

	go func() {
		defer func() {
			tick1.Stop()
			tick2.Stop()
			close(sc.updateChan)
			wg.Done()
			
			sc.logger.Info("Statistic Agent QUIT")
		}()

		readyChan <- "Statistic Agent started"

		for {
			select {
			case <-tick1.C:
				if ss := sc.collectStatistic(); ss.NotEmpty() {
					sc.updateChan <- ss
				}

			case <-tick2.C:
				sc.watchNewQueues()				

			case <-ctx.Done():				
				return
			}
		}
	}()

	return sc.updateChan
}

// UpdateJobCost ...
func (sc *StatisticAgent) UpdateJobCost(queueName string, jobCost float64) {
	if queueStat, found := sc.stats.GetQueue(queueName); found {
		queueStat.UpdateCost(jobCost)
	}
}

// GetUpdateChan ...
func (sc *StatisticAgent) GetUpdateChan() <-chan *ServerStatistic  {
	return sc.updateChan
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
