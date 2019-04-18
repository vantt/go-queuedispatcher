package stats

import (
	"sync"
)

// QueueStatistic ...
type QueueStatistic struct {
	Name       string
	TotalItems uint64
	AvgJobCost float64
	sync.RWMutex
}

// ServerStatistic ...
type ServerStatistic struct {
	Queues map[string]*QueueStatistic
	sync.RWMutex
}

// NewServerStatistic ...
func NewServerStatistic() *ServerStatistic {
	return &ServerStatistic{
		Queues: make(map[string]*QueueStatistic),
	}
}

// AddQueueName ...
func (ss *ServerStatistic) AddQueueName(queueName string) {
	ss.Lock()
	defer ss.Unlock()

	if _, found := ss.Queues[queueName]; !found {
		ss.Queues[queueName] = newQueueStatistic(queueName)
	}
}

// AddQueueStat ...
func (ss *ServerStatistic) AddQueueStat(stat *QueueStatistic) {
	ss.Lock()
	defer ss.Unlock()

	if _, found := ss.Queues[stat.Name]; !found {
		ss.Queues[stat.Name] = stat
	}
}

// GetQueue ...
func (ss *ServerStatistic) GetQueue(queueName string) (stat *QueueStatistic, found bool) {
	stat, found = ss.Queues[queueName]

	return
}

// GetQueueNames ...
func (ss *ServerStatistic) GetQueueNames() []string {
	keys := make([]string, 0, len(ss.Queues))

	for k := range ss.Queues {
		keys = append(keys, k)
	}

	return keys
}

// NotEmpty ...
func (ss *ServerStatistic) NotEmpty() bool {
	return len(ss.Queues) > 0
}

func newQueueStatistic(queueName string) *QueueStatistic {
	return &QueueStatistic{
		Name:       queueName,
		TotalItems: 0,
		AvgJobCost: 0.1,
	}
}

// Empty ...
func (qs *QueueStatistic) Empty() bool {
	qs.RLock()
	defer qs.RUnlock()

	return (qs.TotalItems == 0)
}

// GetTotalItems ...
func (qs *QueueStatistic) GetTotalItems() uint64 {
	qs.RLock()
	defer qs.RUnlock()

	return qs.TotalItems
}

// UpdateTotalItems ...
func (qs *QueueStatistic) updateTotalItems(value uint64) {
	qs.Lock()
	defer qs.Unlock()

	qs.TotalItems = value
}

// UpdateCost ...
func (qs *QueueStatistic) UpdateCost(cost float64) {
	qs.Lock()
	defer qs.Unlock()

	qs.AvgJobCost = (qs.AvgJobCost + cost) / 2
}

// GetAvgJobCost ...
func (qs *QueueStatistic) GetAvgJobCost() float64 {
	qs.RLock()
	defer qs.RUnlock()

	return qs.AvgJobCost
}
