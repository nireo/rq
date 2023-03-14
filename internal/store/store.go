package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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
	Insert(topic []byte, val *Value) error
	Ack(topic []byte, offset uint64) error
	Nack(topic []byte, offset uint64) error
	GetNext(topic []byte) (*Value, uint64, error)
	Close() error
}

type store struct {
	path string
	db   *leveldb.DB
	sync.RWMutex
}

type leveldbCommon interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	Put(key, value []byte, ro *opt.WriteOptions) error
	Has(key []byte, ro *opt.ReadOptions) (bool, error)
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
	decoded := Decode(valBytes)

	if _, err := prependTx(tx, topic, decoded); err != nil {
		tx.Discard()
		return fmt.Errorf("prepending value to topic [%s]: %v", string(topic), err)
	}

	if err := tx.Delete(encodedKey, nil); err != nil {
		return fmt.Errorf("error deleting ack-key: %v", err)
	}

	if err := tx.Commit(); err != nil {
		tx.Discard()
		return fmt.Errorf("commiting nack transaction: %v", err)
	}

	return nil
}

func prependTx(db leveldbCommon, topic []byte, val *Value) (uint64, error) {
	headKey := encodeKeyWithOffset(primaryPrefix, topic, headIndicator)

	headVal, err := db.Get(headKey, nil)
	if err != nil {
		return 0, err
	}

	headOffset := binary.LittleEndian.Uint64(headVal)
	newOffset := headOffset - 1
	newKey := encodeKeyWithOffset(primaryPrefix, topic, newOffset)

	bytes := val.Encode()

	if err := db.Put(newKey, bytes, nil); err != nil {
		return 0, err
	}

	_, newPos, err := addPos(db, topic, -1)
	if err != nil {
		return 0, fmt.Errorf("decrementing head by 1: %v", err)
	}

	return newPos, nil
}

func (s *store) Close() error {
	return s.db.Close()
}

func (s *store) GetNext(topic []byte) (*Value, uint64, error) {
	s.Lock()
	defer s.Unlock()

	headOffset, err := getPos(s.db, topic)
	if err != nil {
		return nil, 0, err
	}

	val, err := getValue(s.db, topic, headOffset)
	if err != nil {
		return nil, 0, err
	}

	inserted, err := appendValue(s.db, ackPrefix, topic, val)
	if err != nil {
		return nil, 0, err
	}

	if _, _, err := addPos(s.db, topic, 1); err != nil {
		return nil, 0, err
	}

	return val, inserted, nil
}

func (s *store) Insert(topic []byte, val *Value) error {
	s.Lock()
	defer s.Unlock()

	tailKey := encodeKeyWithOffset(primaryPrefix, topic, tailIndicator)
	exists, err := s.db.Has(tailKey, nil)
	if err != nil {
		return fmt.Errorf("checking existance failed: %v", err)
	}

	if exists {
		if _, err := appendValue(s.db, primaryPrefix, topic, val); err != nil {
			return err
		}

		return nil
	}

	headKey := encodeKeyWithOffset(primaryPrefix, topic, headIndicator)
	emptyU64 := make([]byte, 8)
	if err := s.db.Put(headKey, emptyU64, nil); err != nil {
		return err
	}

	ackTailKey := encodeKeyWithOffset(ackPrefix, topic, tailIndicator)
	if err := s.db.Put(ackTailKey, emptyU64, nil); err != nil {
		return err
	}

	emptyU64[0] = 1
	if err := s.db.Put(tailKey, emptyU64, nil); err != nil {
		return err
	}
	newKey := encodeKeyWithOffset(primaryPrefix, topic, 0)
	b := val.Encode()

	if err := s.db.Put(newKey, b, nil); err != nil {
		return err
	}

	return nil
}

func getValue(db leveldbCommon, topic []byte, offset uint64) (*Value, error) {
	key := encodeKeyWithOffset(primaryPrefix, topic, offset)
	valBytes, err := db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	decoded := Decode(valBytes)
	return decoded, nil
}

func getPos(db leveldbCommon, topic []byte) (uint64, error) {
	encodedKey := encodeKeyWithOffset(primaryPrefix, topic, headIndicator)
	pos, err := db.Get(encodedKey, nil)
	if err != nil {
		return 0, fmt.Errorf("error getting offset position: %v", err)
	}

	return binary.LittleEndian.Uint64(pos), nil
}

func addPos(db leveldbCommon, topic []byte, sum int) (uint64, uint64, error) {
	oldPos, err := getPos(db, topic)
	if err != nil {
		return 0, 0, err
	}

	if int(oldPos)+sum < 0 {
		panic("invalid new position position")
	}

	newPos := int(oldPos) + sum
	newPosBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(newPosBytes, uint64(newPos))

	encodedKey := encodeKeyWithOffset(primaryPrefix, topic, headIndicator)
	if err := db.Put(encodedKey, newPosBytes, nil); err != nil {
		return 0, 0, fmt.Errorf("failed putting new position: %v", err)
	}

	return oldPos, uint64(newPos), nil
}

func appendValue(db leveldbCommon, topicPrefix int,  topic []byte, val *Value) (uint64, error) {
	tailPosKey := encodeKeyWithOffset(topicPrefix, topic, tailIndicator)

	tailPosVal, err := db.Get(tailPosKey, nil)
	if err != nil {
		return 0, fmt.Errorf("error getting tail value: %v", err)
	}

	origOffset := binary.LittleEndian.Uint64(tailPosVal)
	newKey := encodeKeyWithOffset(topicPrefix, topic, origOffset)
	b := val.Encode()

	if err := db.Put(newKey, b, nil); err != nil {
		return 0, fmt.Errorf("error putting value: %v", err)
	}

	tail := make([]byte, 8)
	binary.LittleEndian.PutUint64(tail, origOffset+1)
	if err := db.Put(tailPosKey, tail, nil); err != nil {
		return 0, fmt.Errorf("putting new tail position: %v", err)
	}

	return origOffset, nil
}

func encodeKeyWithOffset(prefix int, topic []byte, offset uint64) []byte {
	bufferSize := 1 + 8 + len(topic)
	buffer := make([]byte, bufferSize)

	buffer[0] = byte(prefix)
	binary.LittleEndian.PutUint64(buffer[1:9], uint64(offset))
	copy(buffer[9:], topic)

	return buffer
}
