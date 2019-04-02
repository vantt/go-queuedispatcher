package schedule

// InterfaceScheduler ...
type InterfaceScheduler interface {
	Schedule()

	GetNextQueue() (queueName string, found bool)
}
