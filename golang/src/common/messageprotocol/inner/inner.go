package inner

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumMessageType int

const (
	EOF_NOTICE = iota
	EOF_ACK
	COUNT_UPDATE
	COMPlETE
)

type ClientStatus int

const (
	SENDING = iota
	UPDATING
	END
)

type Client struct {
	Id               string
	Status           ClientStatus
	FruitItemMap     map[string]fruititem.FruitItem
	SeenBy           map[string]int
	CurrentBatchSize int
	CoordinatorId    int
}

type ClientMessage struct {
	CoordinatorId int
	ClientId      string
	IsEOF         bool
	MessageCount  int
	FruitRecords  []fruititem.FruitItem
}

type SumMessage struct {
	MsgType      SumMessageType
	SenderId     int
	ClientId     string
	MessageCount int
}

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	err := json.Unmarshal(message, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(id string, isEOF bool, messageCount int, fruitRecords []fruititem.FruitItem, coordinatorId ...int) (*middleware.Message, error) {
	metadata := []interface{}{id, isEOF, messageCount}
	if len(coordinatorId) > 0 {
		metadata = append(metadata, coordinatorId[0])
	}
	data := make([]interface{}, 0, len(fruitRecords))
	for _, fruitRecord := range fruitRecords {
		data = append(data, []interface{}{fruitRecord.Fruit, fruitRecord.Amount})
	}
	body, err := serializeJson([]interface{}{metadata, data})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (ClientMessage, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return ClientMessage{}, err
	}
	if len(data) != 2 {
		return ClientMessage{}, errors.New("invalid client message")
	}

	metadata, ok := data[0].([]interface{})
	if !ok || (len(metadata) != 3 && len(metadata) != 4) {
		return ClientMessage{}, errors.New("invalid client metadata")
	}
	clientMessage := ClientMessage{CoordinatorId: -1}
	clientMessage.ClientId, ok = metadata[0].(string)
	if !ok || clientMessage.ClientId == "" {
		return ClientMessage{}, errors.New("invalid client ID")
	}
	clientMessage.IsEOF, ok = metadata[1].(bool)
	if !ok {
		return ClientMessage{}, errors.New("invalid EOF flag")
	}
	messageCount, ok := metadata[2].(float64)
	if !ok || messageCount < 0 || math.Trunc(messageCount) != messageCount {
		return ClientMessage{}, errors.New("invalid message count")
	}
	clientMessage.MessageCount = int(messageCount)
	if len(metadata) == 4 {
		coordinatorId, ok := metadata[3].(float64)
		if !ok || coordinatorId < 0 || math.Trunc(coordinatorId) != coordinatorId {
			return ClientMessage{}, errors.New("invalid coordinator ID")
		}
		clientMessage.CoordinatorId = int(coordinatorId)
	}

	fruitData, ok := data[1].([]interface{})
	if !ok {
		return ClientMessage{}, errors.New("invalid fruit records")
	}
	clientMessage.FruitRecords = make([]fruititem.FruitItem, 0, len(fruitData))
	for _, datum := range fruitData {
		fruitPair, ok := datum.([]interface{})
		if !ok || len(fruitPair) != 2 {
			return ClientMessage{}, errors.New("invalid fruit record")
		}
		fruit, ok := fruitPair[0].(string)
		if !ok {
			return ClientMessage{}, errors.New("invalid fruit name")
		}
		fruitAmount, ok := fruitPair[1].(float64)
		if !ok || fruitAmount < 0 || fruitAmount > math.MaxUint32 || math.Trunc(fruitAmount) != fruitAmount {
			return ClientMessage{}, errors.New("invalid fruit amount")
		}
		clientMessage.FruitRecords = append(clientMessage.FruitRecords, fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)})
	}
	return clientMessage, nil
}

func DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, bool, error) {
	clientMessage, err := DeserializeMessage(message)
	if err != nil {
		return nil, false, err
	}
	return clientMessage.FruitRecords, clientMessage.IsEOF, nil
}

func SerializeSumMessage(msgType SumMessageType, senderId int, clientId string, messageCount int) (*middleware.Message, error) {

	body, err := serializeJson([]interface{}{
		msgType,
		senderId,
		clientId,
		messageCount,
	})
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeSumMessage(message *middleware.Message) (SumMessage, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return SumMessage{}, err
	}

	if len(data) != 4 {
		return SumMessage{}, errors.New("invalid Sum message")
	}

	sumMessage := SumMessage{}

	msgType, ok := data[0].(float64)
	if !ok {
		return SumMessage{}, errors.New("message type is not a number")
	}

	sumMessage.MsgType = SumMessageType(msgType)

	senderId, ok := data[1].(float64)
	if !ok {
		return SumMessage{}, errors.New("sender ID is not a number")
	}

	sumMessage.SenderId = int(senderId)

	sumMessage.ClientId, ok = data[2].(string)
	if !ok {
		return SumMessage{}, errors.New("client ID is not a string")
	}

	messageCount, ok := data[3].(float64)
	if !ok {
		return SumMessage{}, errors.New("message count is not a number")
	}
	sumMessage.MessageCount = int(messageCount)

	return sumMessage, nil
}

func (client *Client) AddViews(msg SumMessage) {
	key := fmt.Sprintf("id_%d", msg.SenderId)
	if msg.MessageCount > client.SeenBy[key] {
		client.SeenBy[key] = msg.MessageCount
	}
}
