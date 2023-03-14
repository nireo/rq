package store

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert_Single(t *testing.T) {
	store := newTestStore(t)

	err := store.Insert([]byte("topic"), newValue([]byte("new_value")))
	assert.NoError(t, err)

	val, _, err := store.GetNext([]byte("topic"))
	assert.NoError(t, err)

	assert.Equal(t, "new_value", string(val.Raw))
}

func TestGetNext(t *testing.T) {
	store := newTestStore(t)
	topic := []byte("topic")

	var (
		msg1 = newValue([]byte("test_value_1"))
		msg2 = newValue([]byte("test_value_2"))
		msg3 = newValue([]byte("test_value_3"))
	)

	assert.NoError(t, store.Insert(topic, msg1))
	assert.NoError(t, store.Insert(topic, msg2))
	assert.NoError(t, store.Insert(topic, msg3))

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
