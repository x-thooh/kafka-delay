package api

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type IConsumer interface {
	// Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Consumer(ctx context.Context, msg *kafka.Message) error
}
