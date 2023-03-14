package consumer

import (
	"fmt"

	"github.com/nireo/rq/internal/store"
)

type EvType int

const (
	EvPub EvType = iota
	EvNack
	EvRet
)

type Consumer struct {
	ID          string
	Topic       []byte
	AckOffset   uint64
	Store       store.Store
	EvChan      chan EvType
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
