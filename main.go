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
	"github.com/sirupsen/logrus"
	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/dispatcher"
	"github.com/vantt/go-queuedispatcher/queue"
	"github.com/vantt/go-queuedispatcher/schedule"
	"github.com/vantt/go-queuedispatcher/stats"
	"gopkg.in/go-playground/pool.v3"
)

var (
	conf *config.Configuration
	log  = logrus.New() // Create a new instance of the logger. You can have any number of instances.
)

func init() {
	log.Out = os.Stdout

	// Production
	// log.SetFormatter(&logrus.JSONFormatter{})

	// DEV Environment
	log.SetFormatter(&logrus.TextFormatter{})
}

func init() {
	// init random seed
	rand.Seed(time.Now().UTC().UnixNano())
	conf = config.ParseConfig()

}

func main() {
	//putRandomJobs("localhost:11300")
	// connPool1 := queue.NewBeanstalkdConnectionPool(conf.Brokers[0].Host)
	// connPool2 := queue.NewBeanstalkdConnectionPool(conf.Brokers[0].Host)
	// connPool3 := queue.NewBeanstalkdConnectionPool(conf.Brokers[0].Host)

	// job1, err := connPool1.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job1)
	// job2, err := connPool1.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job2)
	// job3, err := connPool2.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job3)
	// // job4, err = connPool2.ConsumeMessage("default2", time.Millisecond)
	// // fmt.Println(job4)
	// job5, err := connPool3.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job5)
	// // job6, err = connPool2.ConsumeMessage("default2", time.Millisecond)
	// // fmt.Println(job6)
	// job6, err := connPool1.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job6)

	// job7, err := connPool2.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job7)

	// job8, err := connPool2.ConsumeMessage("default2", time.Millisecond)
	// fmt.Println(job8)

	// fmt.Println(err)

	// panic("adfasdf")

	grmon.Start()

	putRandomJobs("localhost:11300")
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
		cmd := conf.Brokers[0].WorkerEndpoint
		return dispatcher.NewCmdWorker(request, cmd[0], cmd[1:]...)
	}

	connPool := queue.NewBeanstalkdConnectionPool(conf.Brokers[0].Host)
	statAgent := stats.NewStatisticAgent(connPool)
	scheduler := schedule.NewLotteryScheduler(statAgent, &(conf.Brokers[0]))
	dpatcher := dispatcher.NewDispatcher(connPool, scheduler, log, int32(conf.Brokers[0].Concurrent), time.Millisecond)

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

	for i := 0; i < 1; i++ {
		_, err = tube1.Put([]byte("default1-"+gofakeit.JobTitle()), 1, 0, 60*time.Second)
		_, err = tube2.Put([]byte("default2-"+gofakeit.HackerPhrase()), 1, 0, 60*time.Second)
		_, err = tube3.Put([]byte("default3-"+gofakeit.HipsterWord()), 1, 0, 60*time.Second)

		if err != nil {
			panic(err)
		}
	}
}
