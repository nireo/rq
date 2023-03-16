package broker

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/nireo/rq/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
  ctrl := gomock.NewController(t)
  defer ctrl.Finish()

  val := store.NewValue([]byte("test_value"))

  mockStore := store.NewMockStore(ctrl)
  mockStore.EXPECT().Insert([]byte("test_topic"), val)

  b := NewBroker(mockStore)

  require.NoError(t, b.Publish("test_topic", val))
}
