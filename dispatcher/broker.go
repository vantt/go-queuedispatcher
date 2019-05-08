package dispatcher

import (
	"sync"
	"context"
	"time"
	"errors"

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
	wgChild sync.WaitGroup
}

// NewBroker ...
func NewBroker(cnf config.BrokerConfig, logger *zap.Logger) *Broker{
	return &Broker{config: cnf, logger: logger}
}

// Start ...
func (br *Broker) Start(ctx context.Context, wg *sync.WaitGroup) error {
	err := br.setup(ctx);

	if  err == nil {
		// start broker
		go func() {
			defer func() {
				br.wgChild.Wait()
				wg.Done()
				br.logger.Info("Broker QUIT")
			}()

			for {
				select {
				case <-ctx.Done():				
					return
				}
			}
		}()

		br.logger.Info("Broker started")

	} else {
		br.logger.Info("Broker setup fail")
		wg.Done()	
	}
		
	return err
}

func (br *Broker) setup(ctx context.Context) error {
	br.logger.Info("Broker setting up ....")
	
	connPool := queue.NewBeanstalkdConnectionPool(br.config.Host)
	
	if !connPool.CheckConnection() {
		return errors.New("Could not connect to beanstalkd: " + br.config.Host)		
	}
	
	statAgent := stats.NewStatisticAgent(connPool, br.logger)
	if statAgent == nil {
		return errors.New("Could not create Statistic Agent")		
	}

	scheduler := br.createScheduler(statAgent)
	if scheduler == nil {
		return errors.New("Could not create Scheduler")		
	}

	dpatcher := NewDispatcher(connPool, scheduler, br.logger, int32(br.config.Concurrent), time.Millisecond)

	br.connPool = connPool
	br.statAgent = statAgent
	br.scheduler = scheduler
	br.dispatcher = dpatcher

	childReady := make(chan string, 3)
	
	br.wgChild.Add(1)
	br.statAgent.Start(ctx, &(br.wgChild), childReady)
	
	br.wgChild.Add(1)
	br.scheduler.Start(ctx, &(br.wgChild), childReady)
	
	br.wgChild.Add(1)
	br.dispatcher.Start(ctx, &(br.wgChild), br.createWorkerFunc(), childReady)
	
	// wait for sub-goroutine to ready
	for i:=0; i < 3; i++  {
		br.logger.Info(<-childReady)
	}

	close(childReady)

	return nil
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