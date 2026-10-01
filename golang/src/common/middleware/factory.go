package middleware

import "fmt"

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	factory, err := NewQueueFactory(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}
	return &factory, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings, queueName ...string) (Middleware, error) {
	factory, err := NewExchangeFactory(exchange, keys, connectionSettings, queueName...)
	if err != nil {
		return nil, err
	}
	return &factory, nil
}

func CreateExchangePublisher(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	factory, err := newExchangeFactory(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		_, err = factory.declareQueue(key, []string{key})
		if err != nil {
			factory.Close()
			return nil, err
		}
	}

	return &factory, nil
}

func CreateCoordinationMiddleware(exchange string, keys []string, inputKey string, connectionSettings ConnSettings) (Middleware, error) {
	factory, err := newExchangeFactory(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}

	found := false
	for _, key := range keys {
		queue, err := factory.declareQueue(key, []string{key, "all"})
		if err != nil {
			factory.Close()
			return nil, err
		}
		if key == inputKey {
			factory.queue = queue
			found = true
		}
	}

	if !found {
		factory.Close()
		return nil, fmt.Errorf("%w: unknown input routing key %s", ErrMessageMiddlewareMessage, inputKey)
	}

	return &factory, nil
}
