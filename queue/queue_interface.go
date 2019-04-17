package queue

import (
	"time"
)

// Job Structure that wraps Jobs information
type Job struct {
	ID        uint64
	Payload   interface{}
	QueueName string

	NumTimeOuts uint64        // number of times the job has been reserved and timeout
	NumReturns  uint64        // number of times the job has been returned (release) to the queue
	TimeLeft    time.Duration // remain time for processing
}

// InterfaceQueueConnectionPool ...
type InterfaceQueueConnectionPool interface {

	// Returns a list of all queue names.
	ListQueues() (queueNames []string, err error)

	// Count the number of messages in queue. This can be a approximately number.
	CountMessages(queueName string) (uint64, error)

	// ConsumeMessage
	ConsumeMessage(queueName string, timeout time.Duration) (job *Job, err error)

	// DeleteMessage
	DeleteMessage(queueName string, job *Job) error

	// ReturnMessage
	ReturnMessage(queueName string, job *Job, delay time.Duration) error

	// GiveupMessage
	GiveupMessage(queueName string, job *Job) error
}
