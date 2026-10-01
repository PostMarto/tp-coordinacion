package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientId     string
	messageCount int
}

func NewMessageHandler() MessageHandler {
	clientId := uuid.New()
	return MessageHandler{clientId: clientId.String(), messageCount: 0}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	message, err := inner.SerializeMessage(messageHandler.clientId, false, 0, []fruititem.FruitItem{fruitRecord})
	if err != nil {
		return nil, err
	}

	messageHandler.messageCount++
	return message, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.clientId, true, messageHandler.messageCount, []fruititem.FruitItem{})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientMessage.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return clientMessage.FruitRecords, nil
}
