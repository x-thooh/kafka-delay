package producer

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/x-thooh/kafka-delay/api"
	pkf "github.com/x-thooh/kafka-delay/pkg/kafka"
)

type Producer struct {
	producer *kafka.Producer
	drType   pkf.DrType
}

func New(
	logger api.Logger,
	kcm *kafka.ConfigMap,
	drType pkf.DrType,
) (*Producer, error) {
	producer, err := kafka.NewProducer(kcm)
	if err != nil {
		return nil, err
	}

	// Listen to all the events on the default events channel
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					if logger != nil {
						logger.Errorf("Delivery failed: %v", m.TopicPartition.Error)
					}
				} else {
					if logger != nil {
						logger.Infof("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				if logger != nil {
					logger.Errorf("Error: %v\n", ev)
				}
			default:
				if logger != nil {
					logger.Errorf("Ignored event: %s\n", ev)
				}
			}
		}
	}()
	return &Producer{
		producer: producer,
		drType:   drType,
	}, nil
}

func (p *Producer) Get() *kafka.Producer {
	return p.producer
}

func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event, opts ...pkf.EntityOption) error {
	if msg == nil {
		return errors.New("nil message")
	}

	entity := pkf.NewEntity()

	for _, opt := range opts {
		opt(entity.EntityOptions)
	}

	c := pkf.NewControl(msg, pkf.WithDelayEntity(entity))

	b, err := c.Marshal()
	if err != nil {
		return err
	}

	if entity.IsDelay {
		topic := ""
		switch p.drType {
		case pkf.Merge:
			topic = fmt.Sprintf("%s%s%s%s", pkf.PrefixRetry, pkf.PrefixDelay, pkf.TopicSeparate, *msg.TopicPartition.Topic)
		case pkf.Separation:
			topic = fmt.Sprintf("%s%s", pkf.PrefixDelay, *msg.TopicPartition.Topic)
		case pkf.Independent:
			topic = fmt.Sprintf("%s%s", pkf.PrefixDelay, pkf.DefaultCommonDelayTopic)
		default:

		}
		b.TopicPartition.Topic = &topic
	}

	return p.producer.Produce(b, deliveryChan)
}
