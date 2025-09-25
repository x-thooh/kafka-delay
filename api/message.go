package api

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type IMessage interface {
	Store(ctx context.Context, groupId string, msg *kafka.Message) (bool, error)
	Remove(ctx context.Context, groupId string, msg *kafka.Message) (bool, error)
	Obtain(ctx context.Context, groupId string, tp *kafka.TopicPartition, interval int32) ([]*kafka.Message, error)
}
