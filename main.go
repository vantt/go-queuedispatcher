package main

import (
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/queue"
	"github.com/vantt/go-queuedispatcher/schedule"
	"github.com/vantt/go-queuedispatcher/stats"
)

func main() {
	// init random seed
	rand.Seed(time.Now().UTC().UnixNano())

	config := config.ParseConfig()
	queueInfo := queue.NewBeanstalkdQueueInfo("52.221.223.247:11300")
	serverStats := stats.NewServerStatistic()
	statsCollector := stats.NewStatisticCollector(queueInfo, serverStats)
	scheduler := schedule.NewLotteryScheduler(&config, serverStats)

	statsCollector.StartAgent()
	scheduler.Schedule()

	handleSignals()
}

// handleSignals handle kill signal.
func handleSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-c
}
