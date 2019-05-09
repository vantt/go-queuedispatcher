package dispatcher

import (
	"go.uber.org/zap"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"context"

	"github.com/vantt/go-queuedispatcher/queue"
	"github.com/vantt/go-queuedispatcher/schedule"
	"gopkg.in/go-playground/pool.v3"
)

const (
	// TimeoutTries is the number of timeouts a job must reach before it is
	// buried. Zero means never execute.
	TimeoutTries = 3

	// ReleaseTries is the number of releases a job must reach before it is
	// buried. Zero means never execute.
	ReturnTries = 3
)

type processFunction func(job interface{}) pool.WorkFunc

// Dispatcher ...
type Dispatcher struct {
	queues         queue.InterfaceQueueConnectionPool
	scheduler      schedule.InterfaceScheduler
	concurrent     int32
	processingJobs int32
	p              pool.Pool
	timeout        time.Duration
	aliveChan chan chan struct{}
	log            *zap.Logger
}

// NewDispatcher ...
func NewDispatcher(qc queue.InterfaceQueueConnectionPool, sc schedule.InterfaceScheduler, log *zap.Logger, concurrent int32, timeout time.Duration) *Dispatcher {
	return &Dispatcher{
		queues:         qc,
		scheduler:      sc,
		concurrent:     concurrent,
		timeout:        timeout,
		log:            log,
		processingJobs: 0,
	}
}

// IsAlive ...
func (tc *Dispatcher) IsAlive() chan struct{} {	
	returnChan := make(chan struct{})

	go func() { tc.aliveChan <- returnChan }()

	return returnChan
}

// Start ...
func (tc *Dispatcher) Start(ctx context.Context, wg *sync.WaitGroup, processFn processFunction, readyChan chan<- string) {
	go func() {
		var wgChild sync.WaitGroup

		// create a Worker Pool
		tc.p = pool.NewLimited(uint(tc.concurrent))

		ticker := time.NewTicker(1 * time.Millisecond)
		resultChan := make(chan pool.WorkUnit, tc.concurrent)

		// this is a Channel Merge pattern
		// Start an send goroutine for each input channel in cs.
		// send copies values from c to resultChan until c is closed, then calls wg.Done.
		mergeResult := func(c <-chan pool.WorkUnit, wg *sync.WaitGroup) {
			defer wg.Done()

			for r := range c {
				resultChan <- r
			}
		}

		defer func() {
			// stop so we dont submit new job-batch
			ticker.Stop()

			// wait for all submitted jobs finished
			wgChild.Wait()

			// close the worker pool
			tc.p.Close()

			// now close the resultChan
			close(resultChan)

			// ok I am done
			wg.Done()

			tc.log.Info("Dispatcher QUIT.")
		}()

		readyChan <- "Dispatcher started."

		for {
			select {

			case <-ctx.Done():	
				// receive cancel signal from the context
				return
				
			case _, ok := <-ticker.C:
				if ok {
					// sumbit new batch
					if batch := tc.submitBatch(processFn); batch != nil {
						wgChild.Add(1)
						go mergeResult(batch.Results(), &wgChild)
					}
				}

			case result, ok := <-resultChan:
				if ok { 
					// handle result
					wgChild.Add(1)
					go tc.handleBatchResult(result, &wgChild)
				}
			}
		}
	}()
}

func (tc *Dispatcher) submitBatch(processFn processFunction) pool.Batch {

	// Put more tasks in the channel unless it is full
	if tc.processingJobs < tc.concurrent {
		batch := tc.p.Batch()

		// DO NOT FORGET THIS OR GOROUTINES WILL DEADLOCK
		// if calling Cancel() it calles QueueComplete() internally
		defer batch.QueueComplete()

		numWorks := 0
		tries := int32(0)

		for tc.processingJobs < tc.concurrent {
			job := tc.getNextJob()

			if job != nil {
				// if job was processed too many times, just GiveUp (burry job)
				if job.NumReturns > ReturnTries || job.NumTimeOuts > TimeoutTries {
					err := tc.queues.GiveupMessage(job.QueueName, job)

					if err != nil {
						tc.log.With(zap.String("queue", job.QueueName), zap.Uint64("job_id", job.ID)).Error("Give up job Fail")
					} else {
						tc.log.With(zap.String("queue", job.QueueName), zap.Uint64("job_id", job.ID)).Info("Give up job")
					}
				} else {

					// otherwise put Job into the Channel
					numWorks++
					atomic.AddInt32(&(tc.processingJobs), 1)
					tc.log.With(zap.String("queue", job.QueueName), zap.Uint64("job_id", job.ID)).Info("Process job")
					batch.Queue(processFn(&TaskRequest{Job: job}))
				}
			} else if tries++; tries > tc.concurrent {
				break
			}
		}

		// if batch has jobs to do
		if numWorks > 0 {
			return batch
		}
	}

	return nil
}

func (tc *Dispatcher) handleBatchResult(result pool.WorkUnit, wg *sync.WaitGroup) {
	defer wg.Done()

	taskResult := result.Value().(*TaskResult)
	tc.handleTaskResult(taskResult, result.Error())
}

func (tc *Dispatcher) handleTaskResult(result *TaskResult, taskErr error) (err error) {
	defer func() {
		atomic.AddInt32(&(tc.processingJobs), -1)
	}()

	job := result.Job
	
	logger := tc.log.With(
		zap.String("queue", job.QueueName),
		zap.Uint64("job_id", job.ID),
	)

	logger.Info("Handle job result")

	tc.scheduler.UpdateJobCost(job.QueueName, result.Runtime)

	// if taskErr != nil {
	// 	logger.Error("Task execution error")
	// }

	if result.isTimedOut {
		logger.Error("Task execution TimeOut")
		return
	}

	switch result.ExitStatus {
	case 0:

		err = tc.queues.DeleteMessage(job.QueueName, job)

		if err == nil {
			logger.Info("Delete job")
			//logger.Info(strings.Join(result.Body, "..."))
		} else {
			logger.With(zap.String("error", err.Error())).Error("Deleting job FAIL")			
		}

	default:
		r := job.NumReturns

		if r <= 0 {
			r = 1
		}

		// r*r*r*r means final of 10 tries has 1h49m21s delay, 4h15m33s total.
		// See: http://play.golang.org/p/I15lUWoabI
		delay := time.Duration(r*r*r*r) * time.Second

		logger.With(zap.String("error", result.ErrorMsg)).Error("Job execution FAIL")

		err = tc.queues.ReturnMessage(job.QueueName, job, delay)

		if err != nil {
			logger.With(zap.String("error", result.ErrorMsg)).Error("Return job Fail")
		} else {
			logger.Info("Return job")
		}
	}

	return
}

func (tc *Dispatcher) getNextJob() *queue.Job {
	if queueName, found := tc.scheduler.GetNextQueue(); found {
		if job, err := tc.queues.ConsumeMessage(queueName, tc.timeout); err == nil && job != nil {
			tc.log.With(zap.String("queue", job.QueueName), zap.Uint64("job_id", job.ID)).Info("Reserve job")
			return job
		}
	}

	return nil
}
