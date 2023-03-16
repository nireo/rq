package broker

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/nireo/rq/internal/consumer"
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

func TestSubscribe(t *testing.T) {
  ctrl := gomock.NewController(t)
  defer ctrl.Finish()

  mockStore := store.NewMockStore(ctrl)

  b := NewBroker(mockStore)
  cons := b.Subscribe("test_topic")

  require.IsType(t, &consumer.Consumer{}, cons)
}

func TestUnsubscribe(t *testing.T) {
  type tc struct {
    name string
    fn   func(t *testing.T)
  }

  topic := "test_topic"
  testCases := []tc{
    {
      name: "removes consumer from topic",
      fn: func(t *testing.T) {
        b := broker{
          consumers: map[string][]*consumer.Consumer{},
        }

        c := b.Subscribe(topic)
        err := b.Unsubscribe(topic, c.ID)

        require.NoError(t, err)
        require.Len(t, b.consumers[topic], 0)
      },
    },
    {
      name: "removes correct consumer from many",
      fn: func(t *testing.T) {
        b := broker{
          consumers: map[string][]*consumer.Consumer{},
        }

        c1 := b.Subscribe(topic)
        c2 := b.Subscribe(topic)

        err := b.Unsubscribe(topic, c1.ID)
        require.NoError(t, err)
        require.Len(t, b.consumers[topic], 1)
        require.Equal(t, c2.ID, b.consumers[topic][0].ID)
      },
    },
    {
      name: "error if consumer non existant consumer",
      fn: func(t *testing.T) {
        b := broker{
          consumers: map[string][]*consumer.Consumer{},
        }

        err := b.Unsubscribe(topic, "NONEXISTANT")
        require.Error(t, err)
      },
    },
  }

  for _, testCase := range testCases {
    t.Run(testCase.name, testCase.fn)
  }
}
