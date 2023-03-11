package consumer

import (
	"fmt"

	"github.com/nireo/rq/internal/store"
)

const (
	evPub = iota
	evNack
	evRet
)

type Consumer struct {
	ID          string
	Topic       []byte
	AckOffset   uint64
	Store       store.Store
	evChan      chan int
	Outstanding bool
}

func (c *Consumer) Ack() error {
	if err := c.Store.Ack(c.Topic, c.AckOffset); err != nil {
		return fmt.Errorf("failed to acknowledge topic [%s] with offset [%d]: %v", string(c.Topic), c.AckOffset, err)
	}
	c.Outstanding = false

	return nil
}

func (c *Consumer) Nack() error {
	if err := c.Store.Nack(c.Topic, c.AckOffset); err != nil {
		return fmt.Errorf("failed to nacking topic [%s] with offset [%d]: %v", string(c.Topic), c.AckOffset, err)
	}
	c.Outstanding = false

	return nil
}

func (c *Consumer) EventChan() <-chan int {
	return c.evChan
}
