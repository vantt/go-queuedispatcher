package dispatcher

import (
	"sync"
	"context"
	"time"

	"go.uber.org/zap"
	"github.com/vantt/go-queuedispatcher/config"	
	"github.com/vantt/go-queuedispatcher/queue"
	"github.com/vantt/go-queuedispatcher/schedule"
	"github.com/vantt/go-queuedispatcher/stats"
	"gopkg.in/go-playground/pool.v3"
)

// Broker ....
type Broker struct {
	config config.BrokerConfig
	logger *zap.Logger	
	statAgent *stats.StatisticAgent
	connPool queue.InterfaceQueueConnectionPool
	scheduler schedule.InterfaceScheduler
	dispatcher *Dispatcher
}

// NewBroker ...
func NewBroker(cnf config.BrokerConfig, logger *zap.Logger) *Broker{
	return &Broker{config: cnf, logger: logger}
}

// Start ...
func (br *Broker) Start(ctx context.Context, wg *sync.WaitGroup) {

	var wgChild sync.WaitGroup

	br.setup()

	wgChild.Add(1)
	br.statAgent.Start(ctx, &wgChild)

	wgChild.Add(1)
	br.scheduler.Start(ctx, &wgChild)

	wgChild.Add(1)
	br.dispatcher.Start(ctx, &wgChild, br.createWorkerFunc())

	go func() {
		defer func() {
			wgChild.Wait()
			wg.Done()
			br.logger.Info("Broker QUIT")
		}()

		br.logger.Info("Broker started")

		for {
			select {
			case <-ctx.Done():				
				return
			}
		}
	}()
	
}

func (br *Broker) setup()  {
	br.logger.Info("Broker setting up ....")

	connPool := queue.NewBeanstalkdConnectionPool(br.config.Host)

	statAgent := stats.NewStatisticAgent(connPool, br.logger)
	if statAgent == nil {
		br.logger.DPanic("Could not create Statistic Agent")
	}

	scheduler := br.createScheduler(statAgent)
	if scheduler == nil {
		br.logger.DPanic("Could not create Scheduler")
	}

	dpatcher  := NewDispatcher(connPool, scheduler, br.logger, int32(br.config.Concurrent), time.Millisecond)

	br.connPool = connPool
	br.statAgent = statAgent
	br.scheduler = scheduler
	br.dispatcher = dpatcher
}

func (br *Broker) createWorkerFunc() processFunction {
	if br.config.WorkerType == "cmd" {
		cmd := br.config.WorkerEndpoint

		return func(request interface{}) pool.WorkFunc {
			return NewCmdWorker(request, cmd[0], cmd[1:]...)
		}		
	}	

	br.logger.DPanic("Could not create Worker Function")
	
	return nil
}

func (br *Broker) createScheduler(statAgent *stats.StatisticAgent) schedule.InterfaceScheduler {
	if br.config.Scheduler == "lottery" {
		return schedule.NewLotteryScheduler(&(br.config), statAgent, br.logger)
	}	
	
	br.logger.DPanic("Could not create Dispatcher")

	return nil
}