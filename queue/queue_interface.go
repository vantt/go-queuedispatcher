package queue

// InterfaceQueueInfo ...
type InterfaceQueueInfo interface {

	// Returns a list of all queue names.
	ListQueues() (queueNames []string, err error)

	// Count the number of messages in queue. This can be a approximately number.
	CountMessages(queueName string) (uint64, error)
}

// InterfaceQueueConsuming for queue handling
type InterfaceQueueConsuming interface {

	/**
	 * Remove the next message in line. And if no message is available
	 * wait $duration seconds.
	 *
	 * @param string $queueName
	 * @param int    $duration
	 *
	 * @return array An array like array($message, $receipt);
	 */
	consumeMessage(queueName string, duration uint64)

	/**
	 * If the driver supports it, this will be called when a message
	 * have been consumed.
	 *
	 * @param string $queueName
	 * @param mixed  $receipt
	 */
	// acknowledgeMessage(string queueName, interface{} receipt)
}
