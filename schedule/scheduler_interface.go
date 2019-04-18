package schedule

import (
	"sync"
)

// InterfaceScheduler ...
type InterfaceScheduler interface {
	Schedule(done <-chan struct{}, wg *sync.WaitGroup)

	GetNextQueue() (queueName string, found bool)

	UpdateJobCost(queueName string, jobCost float64)
}
