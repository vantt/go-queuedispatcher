package dispatcher

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vantt/go-cmd"
	"gopkg.in/go-playground/pool.v3"
)

const (
	// ttrMargin compensates for beanstalkd's integer precision.
	// e.g. reserving a TTR=1 job will show time-left=0.
	// We need to set our SIGTERM timer to time-left + ttrMargin.
	ttrMargin = 1 * time.Second
)

// NewCmdWorker ...
func NewCmdWorker(request interface{}, cmdName string, cmdArgs ...string) pool.WorkFunc {
	return func(wu pool.WorkUnit) (interface{}, error) {
		start := time.Now()

		req := request.(*TaskRequest)
		job := req.Job

		tickerTimeOut := time.NewTicker(job.TimeLeft + ttrMargin)
		tickerMonitor := time.NewTicker(1 * time.Millisecond)

		defer func() {
			tickerTimeOut.Stop()
			tickerMonitor.Stop()
		}()

		result := &TaskResult{
			ID:         job.ID,
			Job:        job,
			isTimedOut: false,
		}

		opt := cmd.Options{
			Stdin:     true,
			Buffered:  true,
			Streaming: false,
		}

		process := cmd.NewCmdOptions(opt, cmdName, cmdArgs...)
		statChan := process.Start()

		// send Job info to Command
		process.WriteStdin([]byte(strconv.FormatUint(job.ID, 10) + " "))
		process.WriteStdin(job.Payload.([]byte))
		process.CloseStdin()

		// waiting for Command to finish
		for {
			select {
			// process time out
			// this will
			case <-tickerTimeOut.C:
				fmt.Println("TimeOut ", job.ID)
				process.Stop()
				result.isTimedOut = true

				// please dont break here,
				// let see::  status := <-statChan:

			case <-tickerMonitor.C:
				// pool request to cancel
				if wu.IsCancelled() {
					fmt.Println("Pool cancel ", job.ID)
					process.Stop()
					result.isTimedOut = true
				}

				// please dont break here,
				// let see:: case status := <-statChan:

			// this case will always happen for:
			// command finished, or fail, or cancel or timeout
			case status := <-statChan:
				result.Error = status.Error
				result.ExitStatus = status.Exit
				result.isFail = (!status.Complete || status.Error != nil || status.Exit > 0)
				result.isExecuted = (status.Complete && status.Error == nil)
				result.Body = status.Stdout
				result.ErrorMsg = strings.Join(status.Stderr[:], "\n")

				if result.Error != nil {
					result.ErrorMsg += "\n--------\n" + result.Error.Error()
				}

				result.Runtime = time.Since(start).Seconds()

				return result, nil
			}
		}
	}
}

/**
Get Status
(cmd.Status) {
 Cmd: (string) (len=9) "/bin/bash",
 PID: (int) 22188,
 Complete: (bool) true,
 Exit: (int) 126,
 Error: (error) <nil>,
 StartTs: (int64) 1555564309928581478,
 StopTs: (int64) 1555564309948296312,
 Runtime: (float64) 0.019714811,
 Stdout: ([]string) {
 },
 Stderr: ([]string) (len=1 cap=1) {
  (string) (len=46) "/bin/cat: /bin/cat: cannot execute binary file"
 }
}


**/
