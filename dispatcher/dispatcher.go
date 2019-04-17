package dispatcher

import (
	"errors"
	"fmt"
	"sync"
	"time"

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
	chanRequest    chan *TaskRequest
	chanResponse   chan *TaskResult
}

// NewDispatcher ...
func NewDispatcher(qc queue.InterfaceQueueConnectionPool, sc schedule.InterfaceScheduler, concurrent uint16, timeout time.Duration) *Dispatcher {
	return &Dispatcher{
		queues:         qc,
		scheduler:      sc,
		concurrent:     concurrent,
		timeout:        timeout,
		processingJobs: 0,
		chanRequest:    make(chan *TaskRequest, concurrent),
		chanResponse:   make(chan *TaskResult, concurrent),
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

			fmt.Println("QUIT Dispatcher.")
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
			job, err := tc.getNextJob()

			if err == nil {
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
	if taskErr != nil {
		//fmt.Println("result error")
	}

	job := result.Job

	if result.isTimedOut {
		//fmt.Println("Time Out")
		//b.log.Printf("job %d timed out", job.Id)
		return
	}

	switch result.ExitStatus {
	case 0:
		//fmt.Println("Will Delete Message : %s ", job.QueueName)
		//b.log.Printf("deleting job %d", job.Id)
		err = tc.queues.DeleteMessage(job.QueueName, job)
		//spew.Dump(err)

	default:
		//fmt.Println("Will Return Message")
		r := job.NumReturns

		if r <= 0 {
			r = 1
		}

		// r*r*r*r means final of 10 tries has 1h49m21s delay, 4h15m33s total.
		// See: http://play.golang.org/p/I15lUWoabI
		delay := time.Duration(r*r*r*r) * time.Second
		err = tc.queues.ReturnMessage(job.QueueName, job, delay)

		//b.log.Printf("releasing job %d with %v delay (%d retries)", job.Id, delay, r)
	}

	tc.processingJobs--

	return
}

func (tc *Dispatcher) getNextJob() (*queue.Job, error) {
	if queueName, found := tc.scheduler.GetNextQueue(); found {
		return tc.queues.ConsumeMessage(queueName, tc.timeout)
	}

	return nil, errors.New("Could not reserve task")
}
