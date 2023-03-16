package store

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
  testTopic = []byte("testtopic")
)

func TestInsert_Single(t *testing.T) {
	store := newTestStore(t)

	err := store.Insert(testTopic, NewValue([]byte("new_value")))
	assert.NoError(t, err)

	val, _, err := store.GetNext(testTopic)
	assert.NoError(t, err)

	assert.Equal(t, "new_value", string(val.Raw))
}

func TestGetNext(t *testing.T) {
	store := newTestStore(t)

	var (
		msg1 = NewValue([]byte("test_value_1"))
		msg2 = NewValue([]byte("test_value_2"))
		msg3 = NewValue([]byte("test_value_3"))
	)

	assert.NoError(t, store.Insert(testTopic, msg1))
	assert.NoError(t, store.Insert(testTopic, msg2))
	assert.NoError(t, store.Insert(testTopic, msg3))

	val, offset, err := store.GetNext([]byte("topic"))
	assert.NoError(t, err)
	assert.Equal(t, msg1, val)
	assert.Equal(t, uint64(0), offset)

	val, offset, err = store.GetNext([]byte("topic"))
	assert.NoError(t, err)
	assert.Equal(t, msg2, val)
	assert.Equal(t, uint64(1), offset)

	val, offset, err = store.GetNext([]byte("topic"))
	assert.NoError(t, err)
	assert.Equal(t, msg3, val)
	assert.Equal(t, uint64(2), offset)
}

func TestAck(t *testing.T) {
  store := newTestStore(t).(*store)

  ackOffset := uint64(1)
  key := encodeKeyWithOffset(ackPrefix, testTopic, ackOffset)
  assert.NoError(t, store.db.Put(key, []byte("ack_event"), nil))

  assert.NoError(t, store.Ack(testTopic, ackOffset))

  has, err := store.db.Has(key, nil)
  assert.NoError(t, err)
  assert.False(t, has)
}

func TestNack(t *testing.T) {
  s := newTestStore(t)

  assert.NoError(t, s.Insert(testTopic, NewValue([]byte("test_value_1"))))
  _, offset, err := s.GetNext(testTopic)
  assert.NoError(t, err)

  assert.NoError(t, s.Nack(testTopic, offset))
}

func newTestStore(t *testing.T) Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "rq-test-store")
	if err != nil {
		t.Fatalf("error creating tmp dir: %v", err)
	}

	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("error creating store: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(dir)
		store.Close()
	})

	return store
}
