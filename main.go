package main

import (	
	"strconv"	
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
    "net/http"
	"context"

	//"github.com/bcicen/grmon/agent"
	"github.com/brianvoe/gofakeit"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"github.com/coreos/go-systemd/daemon"
	"github.com/heptiolabs/healthcheck"

	"github.com/kr/beanstalk"
	"github.com/vantt/go-queuedispatcher/config"
	"github.com/vantt/go-queuedispatcher/dispatcher"	
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

func setupLogger(conf config.LoggerConfig) {

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
		Filename:   conf.Filename,
		MaxSize:    conf.MaxSize, // megabytes
		MaxBackups: conf.MaxBackups,
		MaxAge:     conf.MaxAge, // days
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

func setupHealthCheck(ctx context.Context, conf *config.Configuration) {
	
	health := healthcheck.NewHandler()

	// check connection to beanstalkd servers
	for i, brokerConfig := range conf.Brokers {
		health.AddReadinessCheck(
			"upstream-beanstalkd-" + strconv.Itoa(i) + "-" + brokerConfig.Host,
			healthcheck.Async(healthcheck.TCPDialCheck(brokerConfig.Host, 50*time.Millisecond), 
			10*time.Second))
	}
	

	// Our app is not happy if we've got too much goroutine running
	var numGoroutines int

	for i, brokerConfig := range conf.Brokers {
		numGoroutines += int(brokerConfig.Concurrent) + 5
		
		health.AddLivenessCheck(
			"upstream-beanstalkd-" + strconv.Itoa(i) + "-" + brokerConfig.Host,
			healthcheck.Async(healthcheck.TCPDialCheck(brokerConfig.Host, 50*time.Millisecond), 
			10*time.Second))
	}

	// core goroutine
	numGoroutines += 10

	// health check goroutine
	numGoroutines += 10

	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(numGoroutines))

	httpServer := &http.Server{Addr: conf.Viper.GetString("monitor.host"), Handler: health}
	go httpServer.ListenAndServe()

	// wait to shutdown the http server
	go func() {
		for {
			select {
				case <-ctx.Done():

					// shutdown health server
					if httpServer != nil {
						ctx2, cancel2 := context.WithTimeout(ctx, 10*time.Second)
						httpServer.Shutdown(ctx2)
						httpServer = nil			
						cancel2()

						logger.Info("Http Health server QUIT.")
					}

					return
			}
		}
	}()
}

func setupSystemdNotify(ctx context.Context, healthEndPoint string) {

	interval, err := daemon.SdWatchdogEnabled(false)

    if err != nil || interval == 0 {
        return
	}
	
	// send READY signal to systemd
	ready := false

	for !ready {
		response, err := http.Get(healthEndPoint + "/ready")

		if err == nil && response.StatusCode == 200 {
			daemon.SdNotify(false, "READY=1")
			ready = true
		}

		time.Sleep(interval / 3)
	}

	// send LIVENESS signal to systemd
	for {
		select {
			case <-ctx.Done():
				return

			default:
				response, err := http.Get(healthEndPoint + "/live")

				if err == nil && response.StatusCode == 200 {
					daemon.SdNotify(false, "WATCHDOG=1")
				}

				time.Sleep(interval / 3)
		}
	}
}

func setupBrokers(ctx context.Context, wg *sync.WaitGroup, conf *config.Configuration) {
	for _, brokerConfig := range conf.Brokers {
		broker := dispatcher.NewBroker(brokerConfig, logger)
		
		wg.Add(1)
		broker.Start(ctx, wg)		
	}
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


func main() {
	// grmon.Start()
	// putRandomJobs("localhost:11300")

	var wg sync.WaitGroup
	quit := signalsHandle()	
	ctx, cancelFunc := context.WithCancel(context.Background())

	defer func() {
		cancelFunc()
		wg.Wait()

		logger.Info("Bye bye.")
		logger.Sync()
	}()

	setupLogger(conf.Logging)

	logger.Info("GoDispatcher setting up ... ")

	setupHealthCheck(ctx, conf)
	setupSystemdNotify(ctx, conf.Viper.GetString("monitor.host"))
	setupBrokers(ctx, &wg, conf)
	
	logger.Info("GoDispatcher started")
	
	<-quit
}