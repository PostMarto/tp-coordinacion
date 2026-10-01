package aggregation

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	id            int
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	clients       map[string]*inner.Client
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{inputExchangeRoutingKey}, connSettings, inputExchangeRoutingKey)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		id:            config.Id,
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		clients:       make(map[string]*inner.Client),
	}, nil
}

func (aggregation *Aggregation) Run() {
	runContext, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer aggregation.inputExchange.Close()
	defer aggregation.outputQueue.Close()
	consumerFinished := make(chan struct{})
	watcherFinished := make(chan struct{})
	defer func() {
		close(consumerFinished)
		<-watcherFinished
	}()
	go func() {
		defer close(watcherFinished)
		select {
		case <-runContext.Done():
			closeError := aggregation.inputExchange.Close()
			if closeError != nil {
				slog.Error("While closing aggregation consumer", "err", closeError)
			}
		case <-consumerFinished:
		}
	}()

	failed := false
	err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack func(), _ func()) {
		if failed || runContext.Err() != nil {
			return
		}
		handleError := aggregation.handleMessage(msg)
		if handleError != nil {
			failed = true
			slog.Error("While handling aggregation message", "err", handleError)
			stopError := aggregation.inputExchange.StopConsuming()
			if stopError != nil {
				slog.Error("While stopping aggregation consumer", "err", stopError)
				aggregation.inputExchange.Close()
			}
			return
		}
		ack()
	})
	if err != nil && runContext.Err() == nil {
		slog.Error("While consuming aggregation messages", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message) error {
	clientMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}

	if clientMessage.IsEOF {
		return aggregation.handleEndOfRecordsMessage(clientMessage.ClientId)
	}
	return aggregation.handleDataMessage(clientMessage)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	client := aggregation.getOrCreateClient(clientId)
	if client.Status == inner.END {
		return nil
	}
	client.Status = inner.UPDATING

	fruitRecords := make([]fruititem.FruitItem, 0, len(client.FruitItemMap))
	for _, fruitRecord := range client.FruitItemMap {
		fruitRecords = append(fruitRecords, fruitRecord)
	}
	message, err := inner.SerializeMessage(clientId, false, 0, fruitRecords, client.CoordinatorId)
	if err != nil {
		return err
	}
	err = aggregation.outputQueue.Send(*message)
	if err != nil {
		return err
	}

	message, err = inner.SerializeMessage(clientId, true, 0, nil, client.CoordinatorId)
	if err != nil {
		return err
	}
	err = aggregation.outputQueue.Send(*message)
	if err != nil {
		return err
	}
	client.Status = inner.END
	client.FruitItemMap = nil
	return nil
}

func (aggregation *Aggregation) handleDataMessage(message inner.ClientMessage) error {
	client := aggregation.getOrCreateClient(message.ClientId)
	if client.Status != inner.SENDING {
		return fmt.Errorf("received data after client %s EOF", message.ClientId)
	}
	for _, fruitRecord := range message.FruitRecords {
		previousRecord, exists := client.FruitItemMap[fruitRecord.Fruit]
		if exists {
			client.FruitItemMap[fruitRecord.Fruit] = previousRecord.Sum(fruitRecord)
		} else {
			client.FruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (aggregation *Aggregation) getOrCreateClient(clientId string) *inner.Client {
	client, exists := aggregation.clients[clientId]
	if exists {
		return client
	}
	client = &inner.Client{
		Id:            clientId,
		Status:        inner.SENDING,
		FruitItemMap:  make(map[string]fruititem.FruitItem),
		CoordinatorId: aggregation.id,
	}
	aggregation.clients[clientId] = client
	return client
}
