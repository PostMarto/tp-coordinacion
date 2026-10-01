package middleware

import "errors"

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

type Message struct {
	Body string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	StopConsuming() error

	Send(msg Message) error

	SendTo(key string, msg Message) error

	Close() error
}
