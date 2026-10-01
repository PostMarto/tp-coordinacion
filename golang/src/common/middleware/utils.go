package middleware

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func map_middleware_error(connection *amqp.Connection, err error) error {
	if err == nil {
		return nil
	}

	if connection == nil || connection.IsClosed() || errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
}

func close_middleware(connection *amqp.Connection) error {
	if connection == nil || connection.IsClosed() {
		return nil
	}

	err := connection.Close()
	if err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareClose, err)
	}

	return nil
}
