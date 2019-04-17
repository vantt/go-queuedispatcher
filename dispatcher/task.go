package dispatcher

import "github.com/vantt/go-queuedispatcher/queue"

// TaskResult ...
type TaskResult struct {

	// JobID from beanstalkd.
	ID uint64

	*queue.Job

	// Body of the command.
	Body []string

	// Executed is true if the job command was executed (or attempted).
	isExecuted bool

	// TimedOut indicates the worker exceeded TTR for the job.
	// Note this is tracked by a timer, separately to beanstalkd.
	isTimedOut bool

	// Buried is true if the job was buried.
	isFail bool

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// Error raised while attempting to handle the job.
	Error error
}

// TaskRequest ...
type TaskRequest struct {
	*queue.Job
}
