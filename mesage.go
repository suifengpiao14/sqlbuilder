package sqlbuilder

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pkg/errors"
)

var gochannelPool sync.Map
var subscriberLookPool sync.Map // 用于存储订阅者，防止重复创建
var MessageLogger = watermill.NewStdLogger(false, false)

func newGoChannel() (pubsub *gochannel.GoChannel) {
	pubsub = gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: false, // 等待订阅者ack消息,防止消息丢失（关闭前一定已经消费完，内部的主要用于数据异构，所以需要确保数据已经处理完）
		},
		MessageLogger,
	)
	return pubsub
}

func GetPublisher(topic string) (publisher message.Publisher) {
	value, ok := gochannelPool.Load(topic)
	if ok {
		publisher = value.(message.Publisher)
		return publisher
	}
	pubsub := newGoChannel()
	gochannelPool.Store(topic, pubsub)
	publisher = pubsub
	return publisher
}

func GetSubscriber(topic string) (subscriber message.Subscriber) {
	value, ok := gochannelPool.Load(topic)
	if ok {
		subscriber = value.(message.Subscriber)
		return subscriber
	}
	pubsub := newGoChannel()
	gochannelPool.Store(topic, pubsub)
	subscriber = pubsub
	return subscriber
}

// StartSubscriberOnce 防止重复创建订阅者，例如：重复调用订阅者，会导致重复消费消息
func StartSubscriberOnce(consumer Consumer) (err error) {
	topic := consumer.Topic
	if topic == "" {
		err = errors.Errorf("StartSubscriberOnce Topic required, consumer:%s", consumer.String())
		return err
	}
	_, ok := subscriberLookPool.LoadOrStore(topic, true)
	if ok { //已经存在
		return nil
	}
	err = consumer.Consume()
	if err != nil {
		subscriberLookPool.Delete(topic)
	}
	return err
}

type Consumer struct {
	Description string                             `json:"description"`
	Topic       string                             `json:"topic"`
	Subscriber  message.Subscriber                 `json:"-"` // 消费者，支持自定义消费者，例如：gochannel,rabbitmq,kafka等
	WorkFn      func(message *Message) (err error) `json:"-"`
	Logger      watermill.LoggerAdapter            `json:"-"` // 日志适配器，如果不设置则使用默认日志适配器
}

func (c Consumer) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func (s Consumer) Consume() (err error) {
	logger := s.Logger
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}
	if s.Topic == "" {
		err = errors.Errorf("Subscriber.Consume Topic required, consume:%s", s.String())
		return err
	}
	if s.WorkFn == nil {
		err = errors.Errorf("Subscriber.Consume WorkFn required, consume:%s", s.String())
		return err
	}
	if s.Subscriber == nil {
		err = errors.Errorf("Subscriber.Consume Subscriber required, consume:%s", s.String())
		return err
	}
	go func() {
		msgChan, err := s.Subscriber.Subscribe(context.Background(), s.Topic)
		if err != nil {
			logger.Error("Subscriber.Consumer.Subscribe", err, nil)
			return
		}
		for msg := range msgChan {
			func() { // 使用函数包裹，提供defer 处理 ack 操作，防止消息丢失
				defer msg.Ack()
				err = s.WorkFn(msg)
				if err != nil {
					logger.Error("Subscriber.SubscriberFn", err, nil)
				}
			}()
		}
	}()
	return nil
}

func MakeMessage(event any) (msg *Message, err error) {
	b, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	msg = message.NewMessage(watermill.NewUUID(), b)
	return msg, nil
}

type Message = message.Message

type EventMessage interface {
	ToMessage() (msg *Message, err error)
}

// type ExchangedEvent struct {
// 	Identity          string `json:"identity" validate:"required"`
// 	IdentityFieldName string `json:"identityFieldName" validate:"required"`
// }

// func (e ExchangedEvent) String() string {
// 	b, err := json.Marshal(e)
// 	if err != nil {
// 		return err.Error()
// 	}
// 	return string(b)
// }

// func (e ExchangedEvent) ToMessage() (msg *Message, err error) {
// 	return MakeMessage(e)
// }

type IdentityEventI interface {
	GetIdentityValue() string
	GetIdentityFieldName() string
	String() string
}
type IdentityEvent struct {
	Operation         string `json:"operation"`
	IdentityValue     string `json:"identityValue"`
	IdentityFieldName string `json:"identityFieldName"`
}

const (
	IdentityEventOperationCreate = "create"
	IdentityEventOperationUpdate = "update"
	IdentityEventOperationDelete = "delete"
	IdentityEventOperationSet    = "set"
)

func (e IdentityEvent) GetIdentityValue() string {
	return e.IdentityValue
}

func (e IdentityEvent) GetIdentityFieldName() string {
	return e.IdentityFieldName
}

func (e IdentityEvent) String() string {
	b, err := json.Marshal(e)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (e IdentityEvent) ToMessage() (msg *Message, err error) {
	return MakeMessage(e)
}

func MakeIdentityEventSubscriber[Model any](publishTable TableConfig, workFn func(ruleModel Model) (err error)) (subscriber Consumer) {
	return makeIdentityEventISubscriber[IdentityEvent](publishTable, workFn)
}

func makeIdentityEventISubscriber[IdentityEvent IdentityEventI, Model any](publishTable TableConfig, workFn func(ruleModel Model) (err error)) (subscriber Consumer) {
	topic := publishTable.GetTopic()
	return Consumer{
		Description: "数据变更订阅者",
		Topic:       topic,
		Subscriber:  GetSubscriber(topic),
		WorkFn: MakeWorkFn(func(event IdentityEvent) (err error) {
			fieldName := event.GetIdentityFieldName()
			if fieldName == "" {
				err = errors.Errorf("事件(%s)中没有包含字段名", event.String())
				return err
			}
			identity := event.GetIdentityValue()
			if identity == "" {
				err = errors.Errorf("事件(%s)中没有包含唯一标识", event.String())
				return err
			}
			col, err := publishTable.Columns.GetByFieldNameAsError(fieldName)
			if err != nil {
				return err
			}
			field := col.GetField().SetModelRequered(true).SetValue(event.GetIdentityValue())
			fs := Fields{field}
			ruleModel := new(Model)
			err = publishTable.Repository().FirstMustExists(ruleModel, fs)
			if err != nil {
				return err
			}
			err = workFn(*ruleModel)
			if err != nil {
				return err
			}
			return nil
		}),
	}
}

type InsertEvent struct {
	Identity string `json:"identity"`
}

func (e InsertEvent) ToMessage() (msg *Message, err error) {
	return MakeMessage(e)
}

type UpdateEvent struct {
	Identity string `json:"identity"`
}

func (e UpdateEvent) ToMessage() (msg *Message, err error) {
	return MakeMessage(e)
}

func MakeWorkFn[Event any](doFn func(event Event) (err error)) (fn func(msg *Message) error) {
	return func(msg *Message) error {
		var event Event
		err := json.Unmarshal(msg.Payload, &event)
		if err != nil {
			return err
		}
		err = doFn(event)
		if err != nil {
			return err
		}
		return nil
	}
}
