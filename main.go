package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bcicen/grmon/agent"
	"github.com/brianvoe/gofakeit"
	"github.com/kr/beanstalk"
	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/dispatcher"
	"github.com/vantt/go-queuedispatcher/queue"
	"github.com/vantt/go-queuedispatcher/schedule"
	"github.com/vantt/go-queuedispatcher/stats"
	"gopkg.in/go-playground/pool.v3"
)

var (
	conf *config.Configuration
)

func init() {
	// init random seed
	rand.Seed(time.Now().UTC().UnixNano())
	conf = config.ParseConfig()

}

func main() {
	grmon.Start()

	// putRandomJobs("localhost:11300")
	// panic("done")

	var wg sync.WaitGroup

	quit := signalsHandle()
	done := make(chan struct{})

	defer func() {
		close(done)
		wg.Wait()
		fmt.Println("Bye bye.")
	}()

	processFn := func(request interface{}) pool.WorkFunc {
		return dispatcher.NewCmdWorker(conf.Brokers[0].WorkerEndpoint, request)
	}

	connPool := queue.NewBeanstalkdConnectionPool(conf.Brokers[0].Host)
	statAgent := stats.NewStatisticAgent(connPool)
	scheduler := schedule.NewLotteryScheduler(statAgent, &(conf.Brokers[0]))
	dpatcher := dispatcher.NewDispatcher(connPool, scheduler, conf.Brokers[0].Concurrent, 3*time.Second)

	wg.Add(2)
	scheduler.Schedule(done, &wg)
	dpatcher.Start(processFn, done, &wg)

	<-quit
}

func signalsHandle() <-chan struct{} {
	quit := make(chan struct{})

	go func() {
		signals := make(chan os.Signal)
		signal.Notify(signals, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, os.Interrupt)

		defer signalStop(signals)
		defer func() {
			close(signals)
			close(quit)
		}()

		<-signals

		fmt.Println("Receive interrup signal")
	}()

	return quit
}

// Stops signals channel. This function exists
// in Go greater or equal to 1.1.
func signalStop(c chan<- os.Signal) {
	signal.Stop(c)
}

// https://medium.com/@j.d.livni/write-a-go-worker-pool-in-15-minutes-c9b42f640923

func putRandomJobs(address string) {
	conn, err := beanstalk.Dial("tcp", address)

	tube1 := &beanstalk.Tube{Conn: conn, Name: "default1"}
	tube2 := &beanstalk.Tube{Conn: conn, Name: "default2"}
	tube3 := &beanstalk.Tube{Conn: conn, Name: "default3"}

	for i := 0; i < 10; i++ {
		_, err = tube1.Put([]byte("default1-"+gofakeit.JobTitle()), 1, 0, time.Minute)
		_, err = tube2.Put([]byte("default2-"+gofakeit.HackerPhrase()), 1, 0, time.Minute)
		_, err = tube3.Put([]byte("default3-"+gofakeit.HipsterWord()), 1, 0, time.Minute)

		if err != nil {
			panic(err)
		}
	}
}
