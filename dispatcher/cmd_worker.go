package dispatcher

import (
	"fmt"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/vantt/go-cmd"
	"gopkg.in/go-playground/pool.v3"
)

const (
	// Shell will have the command line passed to its `-c` option.
	Shell = "/bin/bash"

	// ttrMargin compensates for beanstalkd's integer precision.
	// e.g. reserving a TTR=1 job will show time-left=0.
	// We need to set our SIGTERM timer to time-left + ttrMargin.
	ttrMargin = 1 * time.Second
)

// NewCmdWorker ...
func NewCmdWorker(shellCmd string, request interface{}) pool.WorkFunc {
	return func(wu pool.WorkUnit) (interface{}, error) {
		req := request.(*TaskRequest)
		job := req.Job
		result := &TaskResult{ID: req.ID, Job: job}

		ticker1 := time.NewTicker(job.TimeLeft + ttrMargin)
		ticker2 := time.NewTicker(1 * time.Millisecond)

		opt := cmd.Options{
			Stdin:     true,
			Buffered:  true,
			Streaming: false,
		}

		process := cmd.NewCmdOptions(opt, Shell, "-C", shellCmd)
		statChan := process.Start()

		// send Job info to Command
		process.WriteStdin([]byte(strconv.FormatUint(job.ID, 10) + " "))
		process.WriteStdin(job.Payload.([]byte))
		process.CloseStdin()

		// waiting for Comand to process
	waitLoop:
		for {
			select {
			// process time out
			case <-ticker1.C:
				fmt.Println("Process Time Out")
				process.Stop()
				result.isTimedOut = true

				break waitLoop

			case <-ticker2.C:
				// pool request to cancel
				if wu.IsCancelled() {
					fmt.Println("Pool request to cancel task.")
					process.Stop()
					result.isTimedOut = true
					break waitLoop
				}

			case status := <-statChan:
				fmt.Println("Get Status")
				spew.Dump(status.Error)
				result.Error = status.Error
				result.ExitStatus = status.Exit
				result.isFail = (!status.Complete || status.Error != nil || status.Exit > 0)
				result.isExecuted = (status.Complete && status.Error == nil)
				result.isTimedOut = false
				result.Body = status.Stdout
				process.Stop()
				break waitLoop
			}
		}

		fmt.Println("Process Done")
		fmt.Println("%v", result)

		ticker1.Stop()
		ticker2.Stop()

		return result, nil
	}
}
