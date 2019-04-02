package schedule

import (
	"math/rand"
	"sync"

	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/stats"
)

// Lottery ...
type Lottery struct {
	Stats        *stats.ServerStatistic
	Tickets      map[string]uint64
	TotalTickets uint64
	Config       *config.Configuration
	sync.RWMutex
}

// NewLotteryScheduler ...
func NewLotteryScheduler(c *config.Configuration, ss *stats.ServerStatistic) *Lottery {
	return &Lottery{
		Stats:        ss,
		Config:       c,
		TotalTickets: 0,
	}
}

// GetNextQueue ..
func (lt *Lottery) GetNextQueue() (queueName string, found bool) {
	queueName = ""
	found = false

	winner := uint64(rand.Intn(100))
	counter := uint64(0)

	lt.RLock()
	defer lt.RUnlock()

	for _, ticket := range lt.Tickets {
		counter = counter + ticket
		if counter > winner {
			found = true
			break
		}
	}

	return
}

// Schedule do re-assign the tickets everytime that Statistic changed
func (lt *Lottery) Schedule() {
	go func() {
		for range lt.Stats.UpdateChan {
			lt.assignTickets()
		}
	}()
}

func (lt *Lottery) assignTickets() {
	tickets := make(map[string]uint64)
	total := uint64(0)

	// assign WSJF for every queue
	for _, queueName := range lt.Stats.GetQueueNames() {
		wsjf := lt.wsjf(queueName)

		if wsjf > 0 {
			total += wsjf
			tickets[queueName] = wsjf
		}
	}

	// convert WSJF to Percent
	lt.TotalTickets = 100
	for queueName := range tickets {
		tickets[queueName] = (tickets[queueName] / total) * 100
	}

	lt.Lock()
	defer lt.Unlock()

	lt.Tickets = tickets
}

// WSJF = Cost of Delay / Job Duration(Size)
// Cost of Delay = NumJobs * Priority
// JobDuration = NumJobs * JobAvgTime
func (lt *Lottery) wsjf(queueName string) uint64 {
	priority := uint64(1)
	stat, found := lt.Stats.GetQueue(queueName)

	if !found {
		return 0
	}

	NumJobs := stat.GetTotalItems()
	CostOfDelay := NumJobs * priority
	JobDuration := NumJobs * stat.GetAvgJobCost()

	return (CostOfDelay / JobDuration)
}
