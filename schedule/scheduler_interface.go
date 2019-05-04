package schedule

import (
	"sync"
	"context"
)

// InterfaceScheduler ...
type InterfaceScheduler interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
	
	GetNextQueue() (queueName string, found bool)

	UpdateJobCost(queueName string, jobCost float64)
}
