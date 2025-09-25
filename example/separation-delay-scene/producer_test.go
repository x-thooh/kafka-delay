package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	spd "github.com/x-thooh/kafka-delay/impl/server/producer"
	pkf "github.com/x-thooh/kafka-delay/pkg/kafka"
)

func TestProducerFailRetry(t *testing.T) {

	producer, err := spd.New(nil, &kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
	}, pkf.Separation)
	if err != nil {
		panic(err)
	}

	// Listen to all the events on the default events channel
	go func() {
		for e := range producer.Get().Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	// for i := 1000; i < 2000; i++ {
	i := rand.Int31()
	drChan := make(chan kafka.Event, 10)

	key := fmt.Sprintf("fail-%d", i)
	topic := "topic_test"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value: func() []byte {
			bs, _ := json.Marshal(map[string]interface{}{
				"foo": "bar",
			})
			return bs
		}(),
		Key: []byte(key),
	}, drChan, pkf.WithDelayLevel([]int{1, 2, 3, 4, 5}...), pkf.WithDelay(true), pkf.WithDelayLevelIndex(2))
	if err != nil {
		panic(err)
	}

	ev := <-drChan
	m := ev.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		t.Errorf("Expected error for message")
	} else {
		t.Logf("Message %s, key: %v", m.TopicPartition, key)
	}
	close(drChan)
	// }

}
