package store

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	primaryPrefix = 1
	ackPrefix     = 2

	tailIndicator = math.MaxUint64
	headIndicator = math.MaxUint64 - 1
)

var (
	ErrKeyDoesntExist = errors.New("key doesn't exist")
)

type Store interface {
	Insert(topic []byte, val Value) error
	Ack(topic []byte, offset uint64) error
	Nack(topic []byte, offset uint64) error
	Close() error
}

type store struct {
	path string
	db   *leveldb.DB
	sync.RWMutex
}

type leveldbCommon interface {
}

func NewStore(path string) (Store, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &store{
		path: path,
		db:   db,
	}, nil
}

func (s *store) Ack(topic []byte, offset uint64) error {
	s.Lock()
	defer s.Unlock()
	encodedKey := encodeKeyWithOffset(ackPrefix, topic, offset)

	return s.db.Delete(encodedKey, nil)
}

func (s *store) Nack(topic []byte, offset uint64) error {
	s.Lock()
	defer s.Unlock()

	tx, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}
	encodedKey := encodeKeyWithOffset(ackPrefix, topic, offset)

	valBytes, err := tx.Get(encodedKey, nil)
	if err != nil {
		return err
	}

	return nil
}

func prependTx(tx *leveldb.Transaction, topic []byte, val Value) (uint64, error) {
	headKey := encodeKeyWithOffset(primaryPrefix, topic, headIndicator)

	headVal, err := tx.Get(headKey, nil)
	if err != nil {
		return 0, err
	}

	headOffset := binary.LittleEndian.Uint64(headVal)
	newOffset := headOffset - 1
	newKey := encodeKeyWithOffset(primaryPrefix, topic, newOffset)

	bytes := val.Encode()

	if err := tx.Put(newKey, bytes, nil); err != nil {
		return 0, err
	}

	return 0, nil
}

func (s *store) Close() error {
	return s.db.Close()
}

func encodeKeyWithOffset(prefix int, topic []byte, offset uint64) []byte {
	bufferSize := 1 + 8 + len(topic)
	buffer := make([]byte, bufferSize)

	buffer[0] = byte(prefix)
	binary.LittleEndian.PutUint64(buffer[1:9], uint64(offset))
	copy(buffer[9:], topic)

	return buffer
}
