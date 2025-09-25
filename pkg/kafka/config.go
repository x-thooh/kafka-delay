package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Config struct {
	KCM    *kafka.ConfigMap
	Items  []*ConfigItem
	DrType DrType
}

type DrType int

const (
	// Merge 延迟队列和重试队列使用同一个Topic，重试不区分分组（group.id）
	Merge DrType = iota
	// Separation 延迟队列和重试队列分开，重试队列区分分组
	Separation
	// Independent 延迟队列和重试队列分开，重试队列区分分组, 所有延迟队列使用同一个
	Independent
)

type ConfigItem struct {
	GroupId string
	Topics  []string
	// 在分组维度下，单分片的协程数量
	PerCoroutineCnt int32
	// 拉取频率，单位ms
	PoolTimeoutMs int
}
