package broker

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/nireo/rq/internal/consumer"
	"github.com/nireo/rq/internal/store"
)

//go:generate mockgen -source=$GOFILE -destination=broker_mock.go -package=broker
type Broker interface {
	Publish(topic string, value *store.Value) error
	Subscribe(topic string) *consumer.Consumer
	Unsubscribe(topic, id string) error
}

type broker struct {
	store     store.Store
	consumers map[string][]*consumer.Consumer
	sync.RWMutex
}

func NewBroker(store store.Store) Broker {
	return &broker{
		store:     store,
		consumers: make(map[string][]*consumer.Consumer),
	}
}

func (b *broker) Close() error {
	return b.store.Close()
}

func (b *broker) Publish(topic string, val *store.Value) error {
	if err := b.store.Insert([]byte(topic), val); err != nil {
		return err
	}

	b.Notify(topic, consumer.EvPub)
	return nil
}

func (b *broker) Subscribe(topic string) *consumer.Consumer {
	c := &consumer.Consumer{
		ID:          uuid.New().String(),
		Topic:       []byte(topic),
		AckOffset:   0,
		Store:       b.store,
		EvChan:      make(chan consumer.EvType),
		Outstanding: false,
	}

	b.Lock()
	b.consumers[topic] = append(b.consumers[topic], c)
	b.Unlock()

	return c
}

func (b *broker) Unsubscribe(topic, id string) error {
	b.RLock()
	cons := b.consumers[topic]
	b.RUnlock()

	for idx, con := range cons {
		if con.ID == id {
			if con.Outstanding {
				con.Nack()
			}

			b.Lock()
			ln := len(b.consumers[topic])
			b.consumers[topic][idx] = b.consumers[topic][ln-1]
			b.consumers[topic] = b.consumers[topic][:ln-1]
			b.Unlock()

			return nil
		}
	}

	return fmt.Errorf("consumer with id [%s] not found for topic: %s", id, topic)
}

func (b *broker) Notify(topic string, ev consumer.EvType) {
	b.RLock()
	defer b.RUnlock()

	for _, c := range b.consumers[topic] {
		select {
		case c.EvChan <- ev:
			return
		default:
			// failed to send message to consumer
		}
	}
}
