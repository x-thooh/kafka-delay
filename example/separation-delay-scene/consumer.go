package main

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/x-thooh/kafka-delay/example/consumer"
	"github.com/x-thooh/kafka-delay/example/log"
	isc "github.com/x-thooh/kafka-delay/impl/server/consumer"
	pkf "github.com/x-thooh/kafka-delay/pkg/kafka"
)

func main() {
	logger := log.NewHelper(log.GetLogger())

	server, err := isc.New(logger, &pkf.Config{
		KCM: &kafka.ConfigMap{
			"bootstrap.servers": "127.0.0.1:9092",
			// "group.id":          "group_test",
		},
		Items: []*pkf.ConfigItem{
			{

				Topics: []string{
					"topic_test",
				},
				GroupId:         "group_test",
				PerCoroutineCnt: 1,
				PoolTimeoutMs:   100,
			},
		},
		DrType: pkf.Separation,
	}, isc.WithConsumer(consumer.NewTestConsumer(logger)))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err = server.Start(ctx); err != nil {
		panic(err)
	}
}
