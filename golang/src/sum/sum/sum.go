package sum

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	MAX_CLIENT_BATCH = 10
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	Id                int
	sumPrefix         string
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	eofExchange       middleware.Middleware
	clients           map[string]*inner.Client
	clientTotalMsgs   map[string]int
	aggregationAmount int
	aggregationKeys   []string
}

type ConsumerMessage struct {
	message          middleware.Message
	ack              func()
	isSumMessage     bool
	messageProcessed chan struct{}
}

func NewSum(config SumConfig) (*Sum, error) {
	if config.SumAmount < 1 || config.AggregationAmount < 1 || config.Id < 0 || config.Id >= config.SumAmount {
		return nil, errors.New("invalid Sum configuration")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangePublisher(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	sumRouteKeys := make([]string, config.SumAmount)
	for index := range config.SumAmount {
		sumRouteKeys[index] = fmt.Sprintf("%s_%d", config.SumPrefix, index)
	}
	eofExchange, err := middleware.CreateCoordinationMiddleware(config.SumPrefix, sumRouteKeys, sumRouteKeys[config.Id], connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		Id:                config.Id,
		sumPrefix:         config.SumPrefix,
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		eofExchange:       eofExchange,
		clients:           map[string]*inner.Client{},
		clientTotalMsgs:   map[string]int{},
		aggregationAmount: config.AggregationAmount,
		aggregationKeys:   outputExchangeRouteKeys,
	}, nil
}

func process(source middleware.Middleware, isSumMessage bool, consumerMessages chan<- ConsumerMessage, consumerErrors chan<- error, end <-chan struct{}) {
	consumerErrors <- source.StartConsuming(func(msg middleware.Message, ack func(), _ func()) {
		message := ConsumerMessage{
			message:          msg,
			ack:              ack,
			isSumMessage:     isSumMessage,
			messageProcessed: make(chan struct{}),
		}
		select {
		case consumerMessages <- message:
		case <-end:
			return
		}
		select {
		case <-message.messageProcessed:
		case <-end:
		}
	})
}

func (sum *Sum) Run() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	consumerMessages := make(chan ConsumerMessage)
	consumerErrors := make(chan error, 2)
	end := make(chan struct{})
	defer func() {
		close(end)
		err := sum.inputQueue.StopConsuming()
		if err != nil {
			slog.Error("While stopping input consumer", "err", err)
		}
		err = sum.eofExchange.StopConsuming()
		if err != nil {
			slog.Error("While stopping control consumer", "err", err)
		}
		sum.inputQueue.Close()
		sum.eofExchange.Close()
		sum.outputExchange.Close()
	}()

	go process(sum.inputQueue, false, consumerMessages, consumerErrors, end)
	go process(sum.eofExchange, true, consumerMessages, consumerErrors, end)

	for {
		select {
		case message := <-consumerMessages:
			var err error
			if message.isSumMessage {
				err = sum.handleSumMessage(message.message)
			} else {
				err = sum.handleMessage(message.message)
			}
			if err != nil {
				slog.Error("While handling message", "err", err)
				close(message.messageProcessed)
				return
			}
			message.ack()
			close(message.messageProcessed)
		case <-ctx.Done():
			return
		case err := <-consumerErrors:
			if err != nil {
				slog.Error("While consuming messages", "err", err)
			}
			return
		}
	}
}

func (sum *Sum) handleMessage(msg middleware.Message) error {
	clientMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		return err
	}
	if clientMessage.IsEOF {
		return sum.handleEndOfRecordMessage(clientMessage.ClientId, clientMessage.MessageCount)
	}
	return sum.handleDataMessage(clientMessage)
}

func (sum *Sum) handleSumMessage(msg middleware.Message) error {

	sumMessage, err := inner.DeserializeSumMessage(&msg)
	if err != nil {
		return err
	}

	switch sumMessage.MsgType {
	case inner.EOF_NOTICE:
		return sum.handleSumEofNotice(sumMessage)
	case inner.EOF_ACK:
		return sum.handleSumEofAck(sumMessage)
	case inner.COUNT_UPDATE:
		return sum.handleSumCountUpdate(sumMessage)
	case inner.COMPlETE:
		return sum.handleSumComplete(sumMessage)
	}
	return nil
}

func (sum *Sum) handleSumEofNotice(msg inner.SumMessage) error {
	slog.Info("Received Sum End Of Records message")
	if msg.SenderId == sum.Id {
		return nil
	}

	client := sum.getOrCreateClient(msg.ClientId)
	client.CoordinatorId = msg.SenderId
	if client.Status != inner.END {
		client.Status = inner.UPDATING
		err := sum.flush(msg.ClientId)
		if err != nil {
			return err
		}
	}

	localCount := client.SeenBy[fmt.Sprintf("id_%d", sum.Id)]
	message, err := inner.SerializeSumMessage(inner.EOF_ACK, sum.Id, msg.ClientId, localCount)
	if err != nil {
		return err
	}

	return sum.eofExchange.SendTo(fmt.Sprintf("%s_%d", sum.sumPrefix, msg.SenderId), *message)
}

func (sum *Sum) handleSumEofAck(msg inner.SumMessage) error {
	slog.Info("Received Sum End Of Records Ack message")
	return sum.handleSumCountUpdate(msg)
}

func (sum *Sum) tryComplete(clientId string) error {
	client, ok := sum.clients[clientId]
	if !ok || client.Status != inner.UPDATING || client.CoordinatorId != sum.Id {
		return nil
	}

	expectedAmount, ok := sum.clientTotalMsgs[clientId]
	if !ok {
		return nil
	}
	totalSeen := 0
	for _, count := range client.SeenBy {
		totalSeen += count
	}
	if totalSeen != expectedAmount {
		return nil
	}

	err := sum.flush(clientId)
	if err != nil {
		return err
	}

	message, err := inner.SerializeMessage(clientId, true, 0, nil)
	if err != nil {
		return err
	}

	err = sum.outputExchange.Send(*message)
	if err != nil {
		return err
	}

	message, err = inner.SerializeSumMessage(inner.COMPlETE, sum.Id, clientId, 0)
	if err != nil {
		return err
	}

	err = sum.eofExchange.SendTo("all", *message)
	if err != nil {
		return err
	}

	sum.finishClient(clientId)
	return nil
}

func (sum *Sum) finishClient(clientId string) {
	client, ok := sum.clients[clientId]
	if ok {
		client.Status = inner.END
		client.FruitItemMap = nil
		client.SeenBy = nil
		client.CurrentBatchSize = 0
	}
	delete(sum.clients, clientId)
	delete(sum.clientTotalMsgs, clientId)
}

func (sum *Sum) handleSumCountUpdate(msg inner.SumMessage) error {
	client, ok := sum.clients[msg.ClientId]
	if !ok || client.Status != inner.UPDATING || client.CoordinatorId != sum.Id || msg.SenderId == sum.Id {
		return nil
	}
	client.AddViews(msg)
	return sum.tryComplete(msg.ClientId)
}

func (sum *Sum) handleSumComplete(msg inner.SumMessage) error {
	if msg.SenderId == sum.Id {
		return nil
	}
	sum.finishClient(msg.ClientId)
	return nil
}

func (sum *Sum) handleEndOfRecordMessage(clientId string, expectedAmount int) error {
	slog.Info("Received End Of Records message")

	client := sum.getOrCreateClient(clientId)
	if client.Status == inner.END {
		return nil
	}

	client.CoordinatorId = sum.Id
	client.Status = inner.UPDATING
	sum.clientTotalMsgs[clientId] = expectedAmount
	err := sum.flush(clientId)
	if err != nil {
		return err
	}

	err = sum.broadcast_eof(clientId, expectedAmount)
	if err != nil {
		return err
	}

	return sum.tryComplete(clientId)
}

func (sum *Sum) broadcast_eof(clientId string, expectedAmount int) error {
	message, err := inner.SerializeSumMessage(inner.EOF_NOTICE, sum.Id, clientId, expectedAmount)
	if err != nil {
		return err
	}

	return sum.eofExchange.SendTo("all", *message)
}

func (sum *Sum) handleDataMessage(message inner.ClientMessage) error {
	client := sum.getOrCreateClient(message.ClientId)

	if client.Status == inner.END {
		return errors.New("received data after client completion")
	}

	sum.processData(client, message.FruitRecords)

	if client.Status == inner.UPDATING || client.CurrentBatchSize >= MAX_CLIENT_BATCH {
		err := sum.flush(client.Id)
		if err != nil {
			return err
		}
	}

	if client.Status == inner.UPDATING {
		if client.CoordinatorId == sum.Id {
			return sum.tryComplete(client.Id)
		}
		return sum.notify(client.CoordinatorId, client.Id)
	}

	return nil
}

func (sum *Sum) notify(coordinatorId int, clientId string) error {
	localCount := sum.clients[clientId].SeenBy[fmt.Sprintf("id_%d", sum.Id)]
	message, err := inner.SerializeSumMessage(inner.COUNT_UPDATE, sum.Id, clientId, localCount)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s_%d", sum.sumPrefix, coordinatorId)
	return sum.eofExchange.SendTo(key, *message)
}

func (sum *Sum) flush(clientId string) error {
	client := sum.clients[clientId]

	for fruit, item := range client.FruitItemMap {
		err := sum.dispatch(clientId, fruit, item)
		if err != nil {
			return err
		}
		delete(client.FruitItemMap, fruit)
	}

	client.CurrentBatchSize = 0
	return nil
}

func (sum *Sum) dispatch(clientId string, fruit string, fruitItem fruititem.FruitItem) error {
	routeKey := fmt.Sprintf("{%s}-{%s}", clientId, fruit)
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(routeKey))
	index := hasher.Sum32() % uint32(sum.aggregationAmount)

	message, err := inner.SerializeMessage(clientId, false, 0, []fruititem.FruitItem{fruitItem})
	if err != nil {
		return err
	}

	return sum.outputExchange.SendTo(sum.aggregationKeys[index], *message)
}

func (sum *Sum) processData(client *inner.Client, fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		_, ok := client.FruitItemMap[fruitRecord.Fruit]
		if ok {
			client.FruitItemMap[fruitRecord.Fruit] = client.FruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			client.FruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	client.SeenBy[fmt.Sprintf("id_%d", sum.Id)]++
	client.CurrentBatchSize++
}

func (sum *Sum) getOrCreateClient(clientId string) *inner.Client {
	if client, ok := sum.clients[clientId]; ok {
		return client
	}

	client := &inner.Client{
		Id:               clientId,
		Status:           inner.SENDING,
		FruitItemMap:     make(map[string]fruititem.FruitItem),
		SeenBy:           make(map[string]int),
		CurrentBatchSize: 0,
		CoordinatorId:    -1,
	}

	sum.clients[clientId] = client
	return client
}
