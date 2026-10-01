package middleware

import (
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var exchange_amount_consumers uint64
var exchange_lock_consumers sync.Mutex

type ExchangeFactory struct {
	*middlewareConnection
	exchange string
	keys     []string
}

func newExchangeFactory(exchangeName string, keys []string, connectionSettings ConnSettings) (ExchangeFactory, error) {
	connection, err := newMiddlewareConnection(connectionSettings)
	if err != nil {
		return ExchangeFactory{}, err
	}
	factory := ExchangeFactory{middlewareConnection: connection, exchange: exchangeName, keys: keys}

	err = factory.channel.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
	if err != nil {
		factory.Close()
		return ExchangeFactory{}, err
	}

	exchange_lock_consumers.Lock()
	factory.id = fmt.Sprintf("%d", exchange_amount_consumers)
	exchange_amount_consumers++
	exchange_lock_consumers.Unlock()
	return factory, nil
}

func NewExchangeFactory(exchangeName string, keys []string, connectionSettings ConnSettings, queueName ...string) (ExchangeFactory, error) {
	factory, err := newExchangeFactory(exchangeName, keys, connectionSettings)
	if err != nil {
		return ExchangeFactory{}, err
	}

	name := ""
	if len(queueName) > 0 {
		name = queueName[0]
	}
	factory.queue, err = factory.channel.QueueDeclare(name, name != "", false, name == "", false, nil)
	if err != nil {
		factory.Close()
		return ExchangeFactory{}, err
	}

	err = factory.Bind(keys)
	if err != nil {
		factory.Close()
		return ExchangeFactory{}, err
	}

	return factory, nil
}

func (factory *ExchangeFactory) declareQueue(queueName string, keys []string) (amqp.Queue, error) {
	queue, err := factory.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, err
	}

	for _, key := range keys {
		err = factory.channel.QueueBind(queue.Name, key, factory.exchange, false, nil)
		if err != nil {
			return amqp.Queue{}, err
		}
	}

	return queue, nil
}

func (factory *ExchangeFactory) Bind(keys []string) error {
	factory.keys = keys
	for _, key := range keys {
		err := factory.channel.QueueBind(factory.queue.Name, key, factory.exchange, false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (factory *ExchangeFactory) Send(msg Message) error {
	factory.publishLock.Lock()
	defer factory.publishLock.Unlock()
	for _, key := range factory.keys {
		err := factory.publishMessage(factory.exchange, key, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (factory *ExchangeFactory) SendTo(key string, msg Message) error {
	factory.publishLock.Lock()
	defer factory.publishLock.Unlock()
	return factory.publishMessage(factory.exchange, key, msg)
}
