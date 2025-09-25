package message

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/redis/go-redis/v9"
	"github.com/x-thooh/kafka-delay/api"
)

type redisMessage struct {
	logger api.Logger

	rc *redis.Client
}

const (
	// kafka:{group}:{topic}:{partition}
	_keyMessage = "kafka:%s:%s:%d"
)

func NewRedisMessage(
	logger api.Logger,
) api.IMessage {
	rc := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "password",
		DB:       1,
	})
	return &redisMessage{
		logger: logger,
		rc:     rc,
	}
}

func (d redisMessage) Store(ctx context.Context, groupId string, msg *kafka.Message) (flag bool, err error) {
	if d.logger != nil {
		d.logger.Infof("[message.Store] Store message: msg:%+v\n", msg.String())
	}
	defer func() {
		if d.logger != nil {
			d.logger.Infof("[message.Store] Store message done: msg:%+v, flag:%+v, err:%+v\n", msg.String(), flag, err)
		}
	}()
	msgBs, err := json.Marshal(msg)
	if err != nil {
		return false, err
	}
	ret, err := d.rc.ZAdd(ctx, fmt.Sprintf(_keyMessage, groupId, *msg.TopicPartition.Topic, msg.TopicPartition.Partition), redis.Z{
		Score:  float64(msg.TopicPartition.Offset),
		Member: string(msgBs),
	}).Result()
	if err != nil {
		return false, err
	}
	return ret > 0, nil
}

func (d redisMessage) Remove(ctx context.Context, groupId string, msg *kafka.Message) (flag bool, err error) {
	if d.logger != nil {
		d.logger.Infof("[message.Remove] Remove message: msg:%+v\n", msg.String())
	}
	defer func() {
		if d.logger != nil {
			d.logger.Infof("[message.Remove] Remove message done: msg:%+v, flag:%+v, err:%+v\n", msg.String(), flag, err)
		}
	}()
	offset := strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)
	ret, err := d.rc.ZRemRangeByScore(ctx, fmt.Sprintf(_keyMessage, groupId, *msg.TopicPartition.Topic, msg.TopicPartition.Partition), offset, offset).Result()
	if err != nil {
		return false, err
	}
	return ret > 0, nil
}

func (d redisMessage) Obtain(ctx context.Context, groupId string, tp *kafka.TopicPartition, interval int32) ([]*kafka.Message, error) {
	defer func() {
		if rec := recover(); rec != nil {
			if d.logger != nil {
				d.logger.Infof("[message.Obtain] Recover msg:%+v\n", rec)
			}
		}
	}()
	result, err := d.rc.ZRangeByScoreWithScores(ctx, fmt.Sprintf(_keyMessage, groupId, *tp.Topic, tp.Partition), &redis.ZRangeBy{
		Count: int64(interval),
	}).Result()
	if err != nil {
		return nil, err
	}
	ret := make([]*kafka.Message, 0, len(result))

	for _, r := range result {
		v, ok := r.Member.(string)
		if !ok {
			continue
		}
		entity := &kafka.Message{}
		if err = json.Unmarshal([]byte(v), entity); err != nil {
			return nil, err
		}
		ret = append(ret, entity)
	}
	return ret, nil
}
