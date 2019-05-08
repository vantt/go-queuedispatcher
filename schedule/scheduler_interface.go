package schedule

import (
	"sync"
	"context"
)

// InterfaceScheduler ...
type InterfaceScheduler interface {
	Start(ctx context.Context, wg *sync.WaitGroup, readyChan chan<- string )
	
	GetNextQueue() (queueName string, found bool)

	UpdateJobCost(queueName string, jobCost float64)
}
