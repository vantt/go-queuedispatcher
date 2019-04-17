package queue

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

// BeanstalkdConnectionPool ...
type BeanstalkdConnectionPool struct {
	address string
	conn    *beanstalk.Conn
	Queues  map[string]*beanstalk.TubeSet
}

const (
	// deadlineSoonDelay defines a period to sleep between receiving
	// DEADLINE_SOON in response to reserve, and re-attempting the reserve.
	DeadlineSoonDelay = 1 * time.Second
)

// NewBeanstalkdConnectionPool ...
func NewBeanstalkdConnectionPool(address string) *BeanstalkdConnectionPool {
	return &BeanstalkdConnectionPool{address: address, Queues: make(map[string]*beanstalk.TubeSet)}
}

// ListQueues Returns a list of all queue names.
func (bs *BeanstalkdConnectionPool) ListQueues() (queueNames []string, err error) {
	queueNames, err = bs.getGlobalConnect().ListTubes()
	return
}

// CountMessages return total number of ready-items in the queue
func (bs *BeanstalkdConnectionPool) CountMessages(queueName string) (uint64, error) {
	return bs.statsUint64(queueName, "current-jobs-ready")

}

// statsUint64 return an Uint64 stat by queuName and key
func (bs *BeanstalkdConnectionPool) statsUint64(queueName string, key string) (uint64, error) {

	tubeStats := &beanstalk.Tube{
		Conn: bs.getGlobalConnect(),
		Name: queueName,
	}

	statsMap, err := tubeStats.Stats()

	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(statsMap[key], 10, 64)
}

//
// ConsumeMessage reserve-with-timeout until there's a job or something panic-worthy.
// Handles beanstalk.ErrTimeout by retrying immediately.
// Handles beanstalk.ErrDeadline by sleeping DeadlineSoonDelay before retry.
// panics for other errors.
func (bs *BeanstalkdConnectionPool) ConsumeMessage(queueName string, timeout time.Duration) (job *Job, err error) {
	var (
		id   uint64
		body []byte
		ttl  string
	)

	for {
		id, body, err = bs.GetQueue(queueName).Reserve(timeout)

		if err == nil {
			job = &Job{
				ID:          id,
				Payload:     body,
				QueueName:   queueName,
				NumReturns:  0,
				NumTimeOuts: 0,
				TimeLeft:    0,
			}

			fmt.Println("Consume Message %d  %p", id, bs.GetQueue(queueName))

			job.NumReturns, err = bs.uint64JobStat(queueName, job, "releases")
			job.NumTimeOuts, err = bs.uint64JobStat(queueName, job, "timeouts")
			ttl, err = bs.statJob(queueName, job, "time-left")

			if err == nil {
				job.TimeLeft, err = time.ParseDuration(ttl + "s")
			}

			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
			err = errors.New("Timeout for reserving a job")
			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
			time.Sleep(DeadlineSoonDelay)
			continue
		} else {
			panic(err)
		}
	}
}

// DeleteMessage ...
func (bs *BeanstalkdConnectionPool) DeleteMessage(queueName string, job *Job) error {
	fmt.Println("Delete Message %d  %p", job.ID, bs.GetQueue(queueName))
	return bs.GetQueue(queueName).Conn.Delete(job.ID)
}

// ReturnMessage ...
func (bs *BeanstalkdConnectionPool) ReturnMessage(queueName string, job *Job, delay time.Duration) error {
	pri, err := bs.uint64JobStat(queueName, job, "pri")

	if err != nil {
		return err
	}

	return bs.GetQueue(queueName).Conn.Release(job.ID, uint32(pri), delay)
}

// GiveupMessage ...
func (bs *BeanstalkdConnectionPool) GiveupMessage(queueName string, job *Job) error {
	pri, err := bs.uint64JobStat(queueName, job, "pri")

	if err != nil {
		return err
	}

	return bs.GetQueue(queueName).Conn.Bury(job.ID, uint32(pri))
}

func (bs *BeanstalkdConnectionPool) statJob(queueName string, job *Job, key string) (string, error) {
	stats, err := bs.GetQueue(queueName).Conn.StatsJob(job.ID)

	if err != nil {
		return "", err
	}

	return stats[key], nil
}

func (bs *BeanstalkdConnectionPool) uint64JobStat(queueName string, job *Job, key string) (uint64, error) {
	stat, err := bs.statJob(queueName, job, key)

	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(stat, 10, 64)
}

// GetQueue ...
func (bs *BeanstalkdConnectionPool) GetQueue(queueName string) *beanstalk.TubeSet {

	if queue, found := bs.Queues[queueName]; found {
		return queue
	}

	conn := bs.connect()

	queue := beanstalk.NewTubeSet(conn, queueName)
	bs.Queues[queueName] = queue

	return queue
}

func (bs *BeanstalkdConnectionPool) getGlobalConnect() *beanstalk.Conn {
	if bs.conn == nil {
		bs.conn = bs.connect()
	}

	return bs.conn
}

func (bs *BeanstalkdConnectionPool) connect() *beanstalk.Conn {
	conn, err := beanstalk.Dial("tcp", bs.address)

	if err != nil {
		panic("Count not connect to beanstalkd server")
	}

	return conn
}
