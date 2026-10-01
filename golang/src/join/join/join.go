package join

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type JoinClient struct {
	Id                        string
	Status                    inner.ClientStatus
	FruitRecordsByAggregation map[int][]fruititem.FruitItem
	EOFReceived               map[int]bool
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	clients           map[string]*JoinClient
	aggregationAmount int
	topSize           int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		clients:           make(map[string]*JoinClient),
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) Run() {
	runContext, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer join.inputQueue.Close()
	defer join.outputQueue.Close()
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
			closeError := join.inputQueue.Close()
			if closeError != nil {
				slog.Error("While closing Join consumer", "err", closeError)
			}
		case <-consumerFinished:
		}
	}()

	failed := false
	err := join.inputQueue.StartConsuming(func(message middleware.Message, ack func(), _ func()) {
		if failed || runContext.Err() != nil {
			return
		}
		handleError := join.handleMessage(message)
		if handleError != nil {
			failed = true
			slog.Error("While handling Join message", "err", handleError)
			stopError := join.inputQueue.StopConsuming()
			if stopError != nil {
				slog.Error("While stopping Join consumer", "err", stopError)
				join.inputQueue.Close()
			}
			return
		}
		ack()
	})
	if err != nil && runContext.Err() == nil {
		slog.Error("While consuming Join messages", "err", err)
	}
}

func (join *Join) handleMessage(message middleware.Message) error {
	clientMessage, err := inner.DeserializeMessage(&message)
	if err != nil {
		return err
	}
	if clientMessage.IsEOF {
		return join.handleEndOfRecordsMessage(clientMessage.ClientId, clientMessage.CoordinatorId)
	}
	join.handleDataMessage(clientMessage)
	return nil
}

func (join *Join) getOrCreateClient(clientId string) *JoinClient {
	client, exists := join.clients[clientId]
	if !exists {
		client = &JoinClient{
			Id:                        clientId,
			Status:                    inner.SENDING,
			FruitRecordsByAggregation: make(map[int][]fruititem.FruitItem),
			EOFReceived:               make(map[int]bool),
		}
		join.clients[clientId] = client
	}
	return client
}

func (join *Join) handleDataMessage(message inner.ClientMessage) {
	client := join.getOrCreateClient(message.ClientId)
	if client.Status == inner.END || client.EOFReceived[message.CoordinatorId] {
		return
	}
	client.FruitRecordsByAggregation[message.CoordinatorId] = message.FruitRecords
}

func (join *Join) handleEndOfRecordsMessage(clientId string, coordinatorId int) error {
	client := join.getOrCreateClient(clientId)
	if client.Status == inner.END {
		return nil
	}
	client.Status = inner.UPDATING
	client.EOFReceived[coordinatorId] = true
	return join.tryComplete(clientId)
}

func (join *Join) tryComplete(clientId string) error {
	client := join.getOrCreateClient(clientId)
	if client.Status == inner.END || len(client.EOFReceived) != join.aggregationAmount {
		return nil
	}

	fruitTopRecords := join.buildFruitTop(clientId)
	message, err := inner.SerializeMessage(clientId, false, 0, fruitTopRecords)
	if err != nil {
		return err
	}
	err = join.outputQueue.Send(*message)
	if err != nil {
		return err
	}

	client.Status = inner.END
	client.FruitRecordsByAggregation = nil
	client.EOFReceived = nil
	return nil
}

func (join *Join) buildFruitTop(clientId string) []fruititem.FruitItem {
	client := join.getOrCreateClient(clientId)
	fruitItemMap := make(map[string]fruititem.FruitItem)
	for aggregationId := range join.aggregationAmount {
		for _, fruitRecord := range client.FruitRecordsByAggregation[aggregationId] {
			fruitItem, exists := fruitItemMap[fruitRecord.Fruit]
			if exists {
				fruitItemMap[fruitRecord.Fruit] = fruitItem.Sum(fruitRecord)
			} else {
				fruitItemMap[fruitRecord.Fruit] = fruitRecord
			}
		}
	}

	fruitNames := make([]string, 0, len(fruitItemMap))
	for fruit := range fruitItemMap {
		fruitNames = append(fruitNames, fruit)
	}
	sort.Strings(fruitNames)

	fruitItems := make([]fruititem.FruitItem, 0, len(fruitNames))
	for _, fruit := range fruitNames {
		fruitItems = append(fruitItems, fruitItemMap[fruit])
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
