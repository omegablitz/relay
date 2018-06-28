package broker

import "time"

// Broker implements a high level interface that
// can provide Consumer or Publisher
type Broker interface {
	Close() error
	Consumer(queue string) (Consumer, error)
	Publisher(queue string) (Publisher, error)
}

// Publisher is used to push messages into a queue
type Publisher interface {
	Close() error
	Publish(in interface{}) error
}

// Consumer is used to consume messages from a queue
type Consumer interface {
	Close() error
	Consume(out interface{}) error
	ConsumeAck(out interface{}) error
	ConsumeId(out interface{}) (uint64, error)
	ConsumeTimeout(out interface{}, timeout time.Duration) error
	ConsumeTimeoutId(out interface{}, timeout time.Duration) (uint64, error)
	Ack() error
	Nack() error
}
