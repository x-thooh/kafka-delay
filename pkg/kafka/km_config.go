package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KMConfig struct {
	cm *kafka.ConfigMap
}

func NewConfig(cm *kafka.ConfigMap) *KMConfig {
	return &KMConfig{
		cm: cm,
	}
}

func (c *KMConfig) SetDefaultValue(key string, value kafka.ConfigValue) error {
	if err := c.cm.SetKey(key, value); err != nil {
		return fmt.Errorf("Set KMConfig err: %v, value:%v\n", err, key)
	}
	return nil
}
