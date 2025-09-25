package kafka

import "C"
import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mohae/deepcopy"
)

type Control struct {
	msg   *kafka.Message
	index int

	co *controlOptions
}

type ControlOption func(o *controlOptions)

type controlOptions struct {
	de *Entity
}

func WithDelayEntity(de *Entity) ControlOption {
	return func(co *controlOptions) {
		co.de = de
	}
}

const (
	_DefaultKeyHeaderDelay = "__delay__"

	PrefixRetry   = "RETRY.."
	PrefixDelay   = "DELAY.."
	TopicSeparate = ".."

	DefaultCommonDelayTopic = "common"
)

func NewControl(msg *kafka.Message, opts ...ControlOption) *Control {
	nMsg := deepcopy.Copy(msg)
	c := &Control{
		msg: nMsg.(*kafka.Message),
		co: &controlOptions{
			de: &Entity{
				CurrentTopic: *msg.TopicPartition.Topic,
			},
		},
	}
	for _, o := range opts {
		o(c.co)
	}
	ret := GetHeaderValue(c.msg.Headers, _DefaultKeyHeaderDelay)
	item, ok := ret[_DefaultKeyHeaderDelay]
	if !ok {
		return c
	}
	c.index = item.Index
	if c.index > -1 {
		hDe := item.Header.Value
		if len(hDe) <= 0 {
			return c
		}

		// dec := gob.NewDecoder(bytes.NewReader(hDe))
		// if err := dec.Decode(c.co.de); err != nil {
		// 	return c
		// }

		if err := json.Unmarshal(hDe, c.co.de); err != nil {
			return c
		}

	} else {
		c.index = len(c.msg.Headers)
		c.msg.Headers = append(c.msg.Headers, kafka.Header{
			Key:   _DefaultKeyHeaderDelay,
			Value: []byte(``),
		})
	}
	return c
}

func (c *Control) Next(mode DrType, typ TopicType, groupId string) *Control {
	tp := c.msg.TopicPartition
	topic := *tp.Topic

	switch typ {
	case Retry:
		switch mode {
		case Merge:
			topic = fmt.Sprintf("%s%s%s", PrefixRetry, PrefixDelay, topic)
			c.msg.TopicPartition.Partition = int32(c.co.de.DelayLevelIndex)
		case Separation:
			topic = fmt.Sprintf("%s%s%s%s", PrefixRetry, groupId, TopicSeparate, topic)
			c.msg.TopicPartition.Partition = int32(c.co.de.DelayLevelIndex)
		case Independent:
			topic = fmt.Sprintf("%s%s%s%s", PrefixRetry, groupId, TopicSeparate, topic)
			c.msg.TopicPartition.Partition = int32(c.co.de.DelayLevelIndex)
		}
	case Delay:
		fallthrough
	default:
		if strings.HasPrefix(topic, PrefixRetry) {
			ta := strings.Split(topic, TopicSeparate)
			topic = ta[len(ta)-1]
		}
		if strings.HasPrefix(topic, PrefixDelay) {
			ta := strings.Split(topic, TopicSeparate)
			topic = ta[len(ta)-1]
		}
		if topic == DefaultCommonDelayTopic {
			topic = c.co.de.CurrentTopic
		}
		c.msg.TopicPartition.Partition = kafka.PartitionAny
		c.co.de.DelayLevelIndex++
	}

	c.co.de.CurrentTopic = *tp.Topic
	c.co.de.CascadeMessage = fmt.Sprintf("%s:%s:%d:%d", groupId, *tp.Topic, tp.Partition, tp.Offset)
	c.msg.TopicPartition.Topic = &topic
	c.msg.Timestamp = time.Now()
	return c
}

func (c *Control) IsStop() bool {
	return c.co.de.DelayLevelIndex >= len(c.co.de.DelayLevel) || c.co.de.DelayLevelIndex < 0
}

func (c *Control) GetDelayLevel(tt TopicType) int {
	delayLevel := defaultDelayLevel
	switch tt {
	case Retry:
		delayLevel = c.co.de.DelayLevel
	case Delay:
		delayLevel = defaultDelayLevel
	default:
		delayLevel = defaultDelayLevel
	}
	if len(delayLevel) > c.co.de.DelayLevelIndex {
		return delayLevel[c.co.de.DelayLevelIndex]
	}
	c.co.de.DelayLevelIndex = -1
	return -1
}

func (c *Control) Marshal() (*kafka.Message, error) {
	// var buf bytes.Buffer
	// enc := gob.NewEncoder(&buf)
	// if err := enc.Encode(c.co.de); err != nil {
	// 	return nil, err
	// }
	// c.msg.Headers[c.index].Value = buf.Bytes()

	bs, err := json.Marshal(c.co.de)
	if err != nil {
		return nil, err
	}
	c.msg.Headers[c.index].Value = bs

	return c.msg, nil
}

func AppendTopics(groupId string, topics []string, m DrType) []string {
	ret := make([]string, 0)
	for _, topic := range topics {
		switch m {
		case Merge:
			ret = append(ret, topic, fmt.Sprintf("%s%s%s", PrefixRetry, PrefixDelay, topic))
		case Separation:
			ret = append(ret, topic, fmt.Sprintf("%s%s%s%s", PrefixRetry, groupId, TopicSeparate, topic), fmt.Sprintf("%s%s", PrefixDelay, topic))
		case Independent:
			ret = append(ret, topic, fmt.Sprintf("%s%s%s", PrefixRetry, PrefixDelay, topic))
		default:
		}
	}
	return ret
}

func GetTopicType(topicName string) TopicType {
	switch {
	case strings.HasPrefix(topicName, PrefixRetry):
		return Retry
	case strings.HasPrefix(topicName, PrefixDelay):
		return Delay
	default:
		return Normal
	}
}

func IsDealTopic(topicName string) bool {
	return strings.HasPrefix(topicName, PrefixRetry) || strings.HasPrefix(topicName, PrefixDelay)
}
