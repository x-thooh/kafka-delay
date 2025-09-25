package consumer

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/x-thooh/kafka-delay/api"
)

type testConsumer struct {
	logger api.Logger
}

func NewTestConsumer(
	logger api.Logger,
) api.IConsumer {
	return &testConsumer{
		logger: logger,
	}
}

func (c *testConsumer) Consumer(ctx context.Context, msg *kafka.Message) error {
	if c.logger != nil {
		c.logger.Infof("[consumer.Consumer] Message deal: %+v\n", msg)
	}
	if strings.HasPrefix(*msg.TopicPartition.Topic, "RETRY..") {
		if rand.Int32N(3) != 0 {
			return errors.New("invalid delay partition")
		}
		return nil
	}
	switch string(msg.Key) {
	case "success":
		return nil
	}

	return errors.New("invalid normal partition")
}
