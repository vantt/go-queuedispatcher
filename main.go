package main

import (	
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
//    "net/http"
	"context"

	"github.com/bcicen/grmon/agent"
	"github.com/brianvoe/gofakeit"

	"github.com/kr/beanstalk"
	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/dispatcher"	
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"github.com/coreos/go-systemd/daemon"
//	"github.com/heptiolabs/healthcheck"
	//"github.com/oklog/run"
)

var (
	conf *config.Configuration
	logger  *zap.Logger // Create a new instance of the logger. You can have any number of instances.
)

func init() {
	// init random seed
	rand.Seed(time.Now().UTC().UnixNano())
	conf = config.ParseConfig()
}

func init() {

	// First, define our level-handling logic.
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})

	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})


	jsonEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())


	// High-priority output should also go to standard error, and low-priority
	// output should also go to standard out.
	consoleDebuggingOutput := zapcore.Lock(os.Stdout)
	consoleErrorsOutput := zapcore.Lock(os.Stderr)


	// lumberjack.Logger is already safe for concurrent use, so we don't need to lock it.
	fileOutput := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "./dispatcher.log",
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})

	// Join the outputs, encoders, and level-handling functions into
	// zapcore.Cores, then tee the four cores together.
	core := zapcore.NewTee(    
		zapcore.NewCore(jsonEncoder, fileOutput, zap.InfoLevel),
		zapcore.NewCore(consoleEncoder, consoleErrorsOutput, highPriority),
		zapcore.NewCore(consoleEncoder, consoleDebuggingOutput, lowPriority),
	)

	// From a zapcore.Core, it's easy to construct a Logger.
	logger = zap.New(core)
	
}

// func setupHealthCheck() healthcheck.Handler {
// 	health := healthcheck.NewHandler()

// 	// Our app is not happy if we've got more than 100 goroutines running.
// 	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(100))

// 	health.AddReadinessCheck(
// 		"upstream-dep-tcp",
// 		healthcheck.Async(TCPDialCheck(upstreamAddr, 50*time.Millisecond), 10*time.Second))

// 	go http.ListenAndServe("0.0.0.0:8086", health)

// 	return health
// }

// func setupSystemdListener(done <-chan struct{}) {
// 	for {
// 		select {
// 			case <-done:
// 				return

// 			default:
// 				_, err := http.Get("http://127.0.0.1:8081/live")

// 				if err == nil {
// 					daemon.SdNotify(false, "WATCHDOG=1")
// 				}

// 				time.Sleep(interval / 3)
// 		}
// 	}
// }

func main() {
	grmon.Start()
	putRandomJobs("localhost:11300")

	var wg sync.WaitGroup

	quit := signalsHandle()	
	ctx, cancelFunc := context.WithCancel(context.Background())

	defer func() {
		cancelFunc()
		wg.Wait()

		logger.Info("Bye bye.")
		logger.Sync()
	}()

	logger.Info("Setting up")

	for _, brokerConfig := range conf.Brokers {

		broker := dispatcher.NewBroker(brokerConfig, logger)
		
		wg.Add(1)
		broker.Start(ctx, &wg)		
	}

	logger.Info("Service started")
	daemon.SdNotify(false, "READY=1")

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

		logger.Info("Receive interrupt signal")
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


// for {
//     _, err := http.Get("http://127.0.0.1:8081") // ❸
//     if err == nil {
//         daemon.SdNotify(false, "WATCHDOG=1")
//     }
//     time.Sleep(interval / 3)
// }