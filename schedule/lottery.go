package schedule

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"context"
	"github.com/olekukonko/tablewriter"
	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/stats"
	"go.uber.org/zap"
)

// Lottery ...
type Lottery struct {
	statAgent    *stats.StatisticAgent
	config       *config.BrokerConfig
	tickets      map[string]uint64
	priority     map[string]uint64
	totalTickets uint64
	sync.RWMutex
	logger *zap.Logger
}

// NewLotteryScheduler ...
func NewLotteryScheduler(c *config.BrokerConfig, sa *stats.StatisticAgent, logger *zap.Logger ) *Lottery {
	return &Lottery{
		statAgent:    sa,
		config:       c,
		priority:     make(map[string]uint64),
		totalTickets: 0,
		logger: logger,
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

	var ticket uint64

	for queueName, ticket = range lt.tickets {
		counter = counter + ticket

		if counter > winner {
			found = true
			break
		}
	}

	return
}

// Start do re-assign the tickets everytime that Statistic changed
func (lt *Lottery) Start(ctx context.Context, wg *sync.WaitGroup, readyChan chan<- string) {

	chanUpdate := lt.statAgent.GetUpdateChan()

	go func() {
		defer func() {
			wg.Done()
			lt.logger.Info("Lottery Scheduler QUIT")
		}()

		readyChan <- "Lottery Scheduler started"

		for {
			select {
			case stats, ok := <-chanUpdate:

				// closed channel
				if ok == false {
					return	
				}

				lt.assignTickets(stats)
				
			case <-ctx.Done():				
				return
			}
		}
	}()
}

func (lt *Lottery) assignTickets(stats *stats.ServerStatistic) {
	tickets := make(map[string]uint64)
	tmpTickets := make(map[string]float64)
	total := float64(0)

	// assign WSJF for every queue
	for _, queueName := range stats.GetQueueNames() {

		if stat, found := stats.GetQueue(queueName); found {
			wsjf := lt.wsjf(stat, lt.getQueuePriority(queueName))

			if wsjf > 0 {
				total = total + wsjf
				tmpTickets[queueName] = wsjf
			}
		}
	}

	// convert WSJF to Percent
	lt.totalTickets = 0

	for queueName := range tmpTickets {
		tickets[queueName] = uint64((tmpTickets[queueName] / total) * 100)
		lt.totalTickets += tickets[queueName]
	}

	lt.Lock()
	defer lt.Unlock()

	lt.tickets = tickets

	//dumpStats(stats, tickets)
}

func (lt *Lottery) getQueuePriority(queueName string) uint64 {
	var (
		priority uint64
		found    bool
	)

	if priority, found = lt.priority[queueName]; !found {
		priority = lt.config.GetTopicPriority(queueName)
		lt.priority[queueName] = priority
	}

	return priority
}

// WSJF = Cost of Delay / Job Duration(Size)
// Cost of Delay = NumJobs * Priority
// JobDuration = NumJobs * JobAvgTime
func (lt *Lottery) wsjf(stat *stats.QueueStatistic, priority uint64) float64 {
	NumJobs := float64(stat.GetTotalItems())
	AvgCost := stat.GetAvgJobCost()

	CostOfDelay := NumJobs * float64(priority)
	JobDuration := NumJobs * AvgCost

	return CostOfDelay / JobDuration
}

// UpdateJobCost ...
func (lt *Lottery) UpdateJobCost(queueName string, jobCost float64) {
	lt.statAgent.UpdateJobCost(queueName, jobCost)
}

func dumpStats(stats *stats.ServerStatistic, tickets map[string]uint64) {

	var ticket uint64
	var found bool

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Queue", "Ready", "Ticket", "AvgCost"})

	for queueName, stat := range stats.Queues {

		if ticket, found = tickets[queueName]; !found {
			ticket = 0
		}

		table.Append([]string{queueName, strconv.FormatUint(stat.GetTotalItems(), 10), strconv.FormatUint(ticket, 10), strconv.FormatFloat(stat.GetAvgJobCost(), 'f', 10, 64)})
	}

	table.Render() // Send output
}
