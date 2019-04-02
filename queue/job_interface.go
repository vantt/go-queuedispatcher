package queue

// Job Structure that wraps Jobs information
type Job struct {
	id      uint
	payload interface{}
}
