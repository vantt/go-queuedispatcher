package dispatcher

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
	concurrent     uint16
	processingJobs uint16
	p              pool.Pool
	timeout        time.Duration
	log            *log.Logger
}

// NewDispatcher ...
func NewDispatcher(qc queue.InterfaceQueueConnectionPool, sc schedule.InterfaceScheduler, log *log.Logger, concurrent uint16, timeout time.Duration) *Dispatcher {
	return &Dispatcher{
		queues:         qc,
		scheduler:      sc,
		concurrent:     concurrent,
		timeout:        timeout,
		log:            log,
		processingJobs: 0,
	}
}

// Start ...
func (tc *Dispatcher) Start(processFn processFunction, done <-chan struct{}, wg *sync.WaitGroup) {
	go func() {
		var wg2 sync.WaitGroup

		// create a Worker Pool
		tc.p = pool.NewLimited(uint(tc.concurrent))

		ticker := time.NewTicker(1 * time.Millisecond)
		resultChan := make(chan pool.WorkUnit, tc.concurrent)

		defer func() {
			ticker.Stop()
			wg2.Wait()
			tc.p.Close()
			close(resultChan)
			wg.Done()

			tc.log.Info("QUIT Dispatcher.")
		}()

		// Start an send goroutine for each input channel in cs.
		// send copies values from c to resultChan until c is closed, then calls wg.Done.
		mergeResult := func(c <-chan pool.WorkUnit) {
			defer wg2.Done()

			for r := range c {
				resultChan <- r
			}
		}

		for {
			select {
			case <-done:
				return

			case <-ticker.C:
				// sumbit new batch
				if batch := tc.submitBatch(processFn); batch != nil {
					wg2.Add(1)
					go mergeResult(batch.Results())
				}

			case result := <-resultChan:
				wg2.Add(1)
				go tc.handleBatchResult(result, &wg2)

				// sumbit new batch
				if batch := tc.submitBatch(processFn); batch != nil {
					wg2.Add(1)
					go mergeResult(batch.Results())
				}
			}
		}
	}()
}

func (tc *Dispatcher) submitBatch(processFn processFunction) pool.Batch {

	// Put more tasks in the channel unless it is full
	if tc.processingJobs < tc.concurrent {
		batch := tc.p.Batch()
		numWorks := 0
		tries := uint16(0)

		for tc.processingJobs < tc.concurrent {
			job := tc.getNextJob()

			if job != nil {
				// if job was processed too many times, just GiveUp (burry job)
				if job.NumReturns > ReturnTries || job.NumTimeOuts > TimeoutTries {
					tc.queues.GiveupMessage(job.QueueName, job)
				}

				// otherwise put Job into the Channel
				// fmt.Println("get task")
				tc.processingJobs++
				numWorks++
				batch.Queue(processFn(&TaskRequest{Job: job}))
			} else if tries++; tries > tc.concurrent {
				break
			}
		}

		// if batch has jobs to do
		if numWorks > 0 {
			// DO NOT FORGET THIS OR GOROUTINES WILL DEADLOCK
			// if calling Cancel() it calles QueueComplete() internally
			batch.QueueComplete()

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
	job := result.Job
	logger := tc.log.WithFields(log.Fields{"queue": job.QueueName, "job_id": job.ID})

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
			logger.Info("Deleting Job SUCCESS")
		} else {
			logger.WithFields(log.Fields{"error": err.Error}).Error("Deleting Job FAIL")
		}

	default:
		r := job.NumReturns

		if r <= 0 {
			r = 1
		}

		// r*r*r*r means final of 10 tries has 1h49m21s delay, 4h15m33s total.
		// See: http://play.golang.org/p/I15lUWoabI
		delay := time.Duration(r*r*r*r) * time.Second

		logger.WithFields(log.Fields{"error": result.ErrorMsg}).Error("Task execution FAIL")
		logger.WithFields(log.Fields{"delay": delay}).Info("Will return job with delay.")

		err = tc.queues.ReturnMessage(job.QueueName, job, delay)

		if err != nil {
			logger.WithFields(log.Fields{"error": err.Error()}).Error("Returning Job Fail")
		}

	}

	tc.processingJobs--

	return
}

func (tc *Dispatcher) getNextJob() *queue.Job {
	if queueName, found := tc.scheduler.GetNextQueue(); found {
		if job, err := tc.queues.ConsumeMessage(queueName, tc.timeout); err == nil {
			return job
		}
	}

	return nil
}
