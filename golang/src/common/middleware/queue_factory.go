package middleware

import (
	"fmt"
	"sync"
)

var queue_amount_consumers uint64
var queue_lock_consumers sync.Mutex

type QueueFactory struct {
	*middlewareConnection
}

func NewQueueFactory(queueName string, connectionSettings ConnSettings) (QueueFactory, error) {
	connection, err := newMiddlewareConnection(connectionSettings)
	if err != nil {
		return QueueFactory{}, err
	}
	factory := QueueFactory{middlewareConnection: connection}

	factory.queue, err = factory.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		factory.Close()
		return QueueFactory{}, err
	}

	queue_lock_consumers.Lock()
	factory.id = fmt.Sprintf("%d", queue_amount_consumers)
	queue_amount_consumers++
	queue_lock_consumers.Unlock()
	return factory, nil
}

func (factory *QueueFactory) Send(msg Message) error {
	return factory.SendTo(factory.queue.Name, msg)
}

func (factory *QueueFactory) SendTo(key string, msg Message) error {
	factory.publishLock.Lock()
	defer factory.publishLock.Unlock()
	return factory.publishMessage("", key, msg)
}
