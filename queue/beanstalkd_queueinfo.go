package queue

import (
	"strconv"

	"github.com/kr/beanstalk"
)

// BeanstalkdQueueInfo sdf
type BeanstalkdQueueInfo struct {
	Address string
	Conn    *beanstalk.Conn
}

// NewBeanstalkdQueueInfo create a BeanstalkdQueue wrapper
func NewBeanstalkdQueueInfo(address string) *BeanstalkdQueueInfo {
	conn, err := beanstalk.Dial("tcp", address)

	if err != nil {
		panic("Count not connect to beanstalkd server")
	}

	return &BeanstalkdQueueInfo{
		Address: address,
		Conn:    conn,
	}
}

// ListQueues Returns a list of all queue names.
func (bd *BeanstalkdQueueInfo) ListQueues() (queueNames []string, err error) {
	queueNames, err = bd.Conn.ListTubes()
	return
}

// CountMessages return total number of ready-items in the queue
func (bd *BeanstalkdQueueInfo) CountMessages(queueName string) (uint64, error) {
	return bd.statsUint64(queueName, "current-jobs-ready")

}

// statsUint64 return an Uint64 stat by queuName and key
func (bd *BeanstalkdQueueInfo) statsUint64(queueName string, key string) (uint64, error) {

	tubeStats := &beanstalk.Tube{
		Conn: bd.Conn,
		Name: queueName,
	}

	statsMap, err := tubeStats.Stats()

	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(statsMap[key], 10, 64)
}
