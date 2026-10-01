package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type middlewareConnection struct {
	id            string
	channel       *amqp.Channel
	connection    *amqp.Connection
	queue         amqp.Queue
	returns       <-chan amqp.Return
	channelErrors <-chan *amqp.Error
	publishLock   sync.Mutex
	consumerLock  sync.Mutex
	closing       bool
	consuming     bool
}

func newMiddlewareConnection(connectionSettings ConnSettings) (*middlewareConnection, error) {
	connection, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, err
	}

	err = channel.Qos(10, 0, false)
	if err != nil {
		connection.Close()
		return nil, err
	}

	err = channel.Confirm(false)
	if err != nil {
		connection.Close()
		return nil, err
	}

	return &middlewareConnection{
		channel:       channel,
		connection:    connection,
		returns:       channel.NotifyReturn(make(chan amqp.Return, 1)),
		channelErrors: channel.NotifyClose(make(chan *amqp.Error, 1)),
	}, nil
}

func (factory *middlewareConnection) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	factory.consumerLock.Lock()
	if factory.closing {
		factory.consumerLock.Unlock()
		return nil
	}

	messages, err := factory.channel.Consume(factory.queue.Name, factory.id, false, false, false, false, nil)
	if err != nil {
		factory.consumerLock.Unlock()
		return map_middleware_error(factory.connection, err)
	}
	factory.consuming = true
	factory.consumerLock.Unlock()

	for message := range messages {
		ack := func() {
			err := message.Ack(false)
			if err != nil {
				slog.Error("While acknowledging message", "err", err)
			}
		}

		nack := func() {
			err := message.Nack(false, true)
			if err != nil {
				slog.Error("While rejecting message", "err", err)
			}
		}

		callbackFunc(Message{Body: string(message.Body)}, ack, nack)
	}

	factory.consumerLock.Lock()
	factory.consuming = false
	closing := factory.closing
	factory.consumerLock.Unlock()
	if closing {
		return nil
	}

	select {
	case err := <-factory.channelErrors:
		if err != nil {
			return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
		}
	default:
	}

	return ErrMessageMiddlewareDisconnected
}

func (factory *middlewareConnection) StopConsuming() error {
	factory.consumerLock.Lock()
	defer factory.consumerLock.Unlock()
	if factory.closing {
		return nil
	}

	factory.closing = true
	if !factory.consuming {
		return nil
	}

	err := factory.channel.Cancel(factory.id, false)
	if err != nil {
		return map_middleware_error(factory.connection, err)
	}

	return nil
}

func (factory *middlewareConnection) publishMessage(exchange string, key string, msg Message) error {
	message := amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(msg.Body),
	}

	confirmation, err := factory.channel.PublishWithDeferredConfirm(exchange, key, true, false, message)
	if err != nil {
		return map_middleware_error(factory.connection, err)
	}
	if confirmation == nil {
		return ErrMessageMiddlewareMessage
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		factory.Close()
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
	}
	if !confirmed {
		if factory.channel.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	select {
	case message, open := <-factory.returns:
		if !open {
			return ErrMessageMiddlewareDisconnected
		}
		return fmt.Errorf("%w: %s (%s:%s)", ErrMessageMiddlewareMessage, message.ReplyText, message.Exchange, message.RoutingKey)
	default:
	}

	return nil
}

func (factory *middlewareConnection) Close() error {
	factory.consumerLock.Lock()
	defer factory.consumerLock.Unlock()
	factory.closing = true
	return close_middleware(factory.connection)
}
