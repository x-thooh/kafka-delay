package consumer

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/panjf2000/ants/v2"
	"github.com/x-thooh/kafka-delay/api"
	im "github.com/x-thooh/kafka-delay/impl/message"
	spd "github.com/x-thooh/kafka-delay/impl/server/producer"
	"github.com/x-thooh/kafka-delay/pkg/app/transport"
	pkf "github.com/x-thooh/kafka-delay/pkg/kafka"
	ptw "github.com/x-thooh/kafka-delay/pkg/timingwheel"
	"golang.org/x/sync/errgroup"
)

type consumer struct {
	closeCh chan struct{}
	run     bool

	m map[string]*instance
	o *options
}

type instance struct {
	c     *kafka.Consumer
	ts    []*ptw.Timer
	pools map[string]map[int32]*ants.Pool

	s      *consumer
	config *pkf.ConfigItem
}

// New 创建
func New(
	logger api.Logger,
	config *pkf.Config,
	opts ...Option,
) (transport.Server, error) {
	producer, err := spd.New(logger, config.KCM, config.DrType)
	if err != nil {
		return nil, err
	}
	o := &options{
		message:     im.NewRedisMessage(logger),
		timingWheel: ptw.NewTimingWheel(time.Second, 60),
		producer:    producer,
		logger:      logger,
		config:      config,
	}
	for _, opt := range opts {
		opt(o)
	}

	if o.message == nil {
		return nil, fmt.Errorf("consumer message is empty")
	}

	if o.producer == nil {
		return nil, fmt.Errorf("consumer producer is empty")
	}

	if o.timingWheel == nil {
		return nil, fmt.Errorf("consumer timingWheel is empty")
	}

	if o.config == nil && len(o.config.Items) == 0 {
		return nil, fmt.Errorf("consumer config is empty")
	}

	s := &consumer{
		closeCh: make(chan struct{}),
		run:     true,

		m: make(map[string]*instance, len(o.config.Items)),
		o: o,
	}

	eg := errgroup.Group{}
	for _, item := range o.config.Items {
		eg.Go(func() error {
			// 默认初始化值
			pkc := pkf.NewConfig(o.config.KCM)

			if err = pkc.SetDefaultValue("enable.auto.offset.store", false); err != nil {
				return err
			}
			if err = pkc.SetDefaultValue("enable.auto.commit", false); err != nil {
				return err
			}
			if err = pkc.SetDefaultValue("group.id", item.GroupId); err != nil {
				return err
			}

			// 创建消费者
			c, kErr := kafka.NewConsumer(o.config.KCM)
			if kErr != nil {
				return fmt.Errorf("new consumer err: %v", kErr)
			}

			// 默认值
			if item.PerCoroutineCnt == 0 {
				item.PerCoroutineCnt = 100
			}

			// 初始化消费者
			ins := &instance{
				ts:    make([]*ptw.Timer, 0),
				pools: make(map[string]map[int32]*ants.Pool),

				config: item,
				s:      s,
				c:      c,
			}

			// 订单topic
			err = c.SubscribeTopics(pkf.AppendTopics(item.GroupId, item.Topics, o.config.DrType), ins.rebalancedCallback)
			if err != nil {
				return fmt.Errorf("Subscribe Topics err: %v, Topics:%+v\n", err, item.Topics)
			}

			// 追加
			s.m[item.GroupId] = ins
			return nil
		})
	}
	if err = eg.Wait(); err != nil {
		// 没有全部起启起来，需要把已启动的给关闭
		for _, item := range o.config.Items {
			if ins, ok := s.m[item.GroupId]; ok {
				if err = ins.c.Close(); err != nil {
					if s.o.logger != nil {
						s.o.logger.Errorf("close err: %v, groupId:%v\n", err, item.GroupId)
					}
				}
			}
		}
		return nil, err
	}

	return s, nil
}

func (s *consumer) Start(ctx context.Context) error {
	s.o.timingWheel.Start()
	for s.run {
		select {
		case sig := <-s.closeCh:
			if s.o.logger != nil {
				s.o.logger.Infof("Consumer close at %s\n", sig)
			}
			s.run = false
		default:
			eg := errgroup.Group{}
			for _, ins := range s.m {
				eg.Go(func() error {
					return ins.deal(ctx)
				})
			}
			if err := eg.Wait(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *consumer) Stop(_ context.Context) error {
	if s.run {
		close(s.closeCh)
		// if _, err := s.c.Commit(); err != nil {
		// 	return err
		// }
		s.o.timingWheel.Stop()
		s.o.producer.Get().Close()
		for _, ins := range s.m {
			if err := ins.c.Close(); err != nil {
				return err
			}
			for _, item := range ins.pools {
				for _, p := range item {
					p.Release()
				}
			}
			for _, ts := range ins.ts {
				ts.Stop()
			}
		}
	}
	return nil
}

func (ins *instance) deal(ctx context.Context) error {
	for ins.s.run {
		ev := ins.c.Poll(ins.config.PoolTimeoutMs)
		if ev == nil {
			continue
		}

		ctx = context.Background()
		switch e := ev.(type) {
		case *kafka.Message:
			if ins.s.o.logger != nil {
				ins.s.o.logger.Debugf("[%s] Accept message, msg:%v\n", ins.config.GroupId, e.String())
			}
			// to cache
			flag, err := ins.s.o.message.Store(ctx, ins.config.GroupId, e)
			if err != nil {
				return fmt.Errorf("[%s] Store message err: %v, msg:%v", ins.config.GroupId, err, e.String())
			}
			if !flag {
				if ins.s.o.logger != nil {
					ins.s.o.logger.Infof("[%s] Store message invalid, msg:%v\n", ins.config.GroupId, e.String())
				}
			}

			// to consumer
			tp := e.TopicPartition
			pool, ok := ins.pools[*tp.Topic][tp.Partition]
			if !ok {
				if ins.s.o.logger != nil {
					ins.s.o.logger.Errorf("[%s] pool exist coroutine, msg:%v\n", ins.config.GroupId, e.String())
				}
				return fmt.Errorf("pool exist coroutine")
			}
			if err = pool.Submit(func() {
				var (
					msgErr  error
					isDelay bool
				)
				rc := pkf.NewControl(e)
				defer func() {
					if isDelay {
						return
					}
					flag, err = ins.s.o.message.Remove(ctx, ins.config.GroupId, e)
					if err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Remove message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					if !flag {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Infof("[%s] Remove message invalid, msg:%v\n", ins.config.GroupId, e.String())
						}
					}
					if msgErr != nil {
						rc = rc.Next(ins.s.o.config.DrType, pkf.Retry, ins.config.GroupId)
						if rc.IsStop() {
							if _, err = ins.c.StoreMessage(e); err != nil {
								if ins.s.o.logger != nil {
									ins.s.o.logger.Errorf("[%s] Store message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
								}
								return
							}
							// todo DLQ
							if ins.s.o.logger != nil {
								ins.s.o.logger.Infof("[%s] Drop message err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
							}
							return
						}
						ne, err := rc.Marshal()
						if err != nil {
							if ins.s.o.logger != nil {
								ins.s.o.logger.Errorf("[%s] Control Marshal err: %v, rc:%+v\n", ins.config.GroupId, err, rc)
							}
						}
						if err = ins.s.o.producer.Produce(ne, nil); err != nil {
							if ins.s.o.logger != nil {
								ins.s.o.logger.Errorf("[%s] Forward message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
							}
							return
						}
					}
					if _, err = ins.c.StoreMessage(e); err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Store message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
				}()

				topic := *tp.Topic
				if !pkf.IsDealTopic(topic) {
					// to exec
					if msgErr = ins.s.o.consumer.Consumer(ctx, e); msgErr != nil {
						return
					}
					return
				}

				diff := e.Timestamp.Add(pkf.GetDelayTime(rc.GetDelayLevel(pkf.GetTopicType(topic)))).Sub(time.Now().UTC())

				isPause := false
				if diff > 0 {
					// to Pause
					if err = ins.c.Pause([]kafka.TopicPartition{tp}); err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Pause message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					isPause = true
				}

				ins.s.o.timingWheel.AfterFunc(diff, func() {
					flag, err = ins.s.o.message.Remove(ctx, ins.config.GroupId, e)
					if err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Remove message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					if !flag {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Remove message invalid, msg: %v\n", ins.config.GroupId, e.String())
						}
					}
					rc = rc.Next(ins.s.o.config.DrType, pkf.Delay, ins.config.GroupId)
					if rc.IsStop() {
						if _, err = ins.c.StoreMessage(e); err != nil {
							if ins.s.o.logger != nil {
								ins.s.o.logger.Errorf("[%s] Store message err: %v, msg:%v\n", ins.config.GroupId, err, e.String())
							}
							return
						}
						// todo DLQ
						if ins.s.o.logger != nil {
							ins.s.o.logger.Infof("[%s] Drop message err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					ne, err := rc.Marshal()
					if err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Control Marshal err: %v, rc:%+v\n", ins.config.GroupId, err, rc)
						}
					}
					if err = ins.s.o.producer.Produce(ne, nil); err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Forward message err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					if _, err = ins.c.StoreMessage(e); err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Store message err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
						}
						return
					}
					if isPause {
						if err = ins.c.Resume([]kafka.TopicPartition{e.TopicPartition}); err != nil {
							if ins.s.o.logger != nil {
								ins.s.o.logger.Errorf("[%s] Resume message err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
							}
							return
						}
					}
				})
				isDelay = true
			}); err != nil {
				if ins.s.o.logger != nil {
					ins.s.o.logger.Errorf("[%s] Pool submit err: %v, msg: %v\n", ins.config.GroupId, err, e.String())
				}
				return err
			}

		case kafka.Error:
			if ins.s.o.logger != nil {
				ins.s.o.logger.Errorf("[%s] Kafka code(%v) error: %v\n", ins.config.GroupId, e.Code(), e)
			}
			if e.Code() == kafka.ErrAllBrokersDown {
				ins.s.run = false
			}
		default:
			if ins.s.o.logger != nil {
				ins.s.o.logger.Debugf("[%s] Ignored %v\n", ins.config.GroupId, e)
			}
		}
	}

	return nil
}

func (ins *instance) rebalancedCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		if ins.s.o.logger != nil {
			ins.s.o.logger.Debugf("[%s] %s reBalance: %d new partition(s) assigned: %v\n", ins.config.GroupId, c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)
		}

		for _, p := range ev.Partitions {
			if _, ok := ins.pools[*p.Topic]; !ok {
				ins.pools[*p.Topic] = make(map[int32]*ants.Pool)
			}
			size := int(ins.config.PerCoroutineCnt)
			if strings.HasPrefix(*p.Topic, pkf.PrefixRetry) || strings.HasPrefix(*p.Topic, pkf.PrefixDelay) {
				size = 1
			}
			pool, err := ants.NewPool(size, ants.WithPreAlloc(true), ants.WithPanicHandler(func(i interface{}) {
				if ins.s.o.logger != nil {
					ins.s.o.logger.Errorf("[%s] partition coroutine panic: %+v, stack:%+v\n", ins.config.GroupId, i, string(debug.Stack()))
				}
			}), ants.WithLogger(NewAntsDefaultLog(ins.s.o.logger)), ants.WithDisablePurge(true))
			if err != nil {
				if ins.s.o.logger != nil {
					ins.s.o.logger.Errorf("[%s] Failed to create partition coroutine: %v\n", ins.config.GroupId, err)
				}
				return err
			}
			ins.pools[*p.Topic][p.Partition] = pool

			ins.ts = append(ins.ts, ins.s.o.timingWheel.ScheduleFunc(&EveryScheduler{1 * time.Second}, func() {
				defer func() {
					if rErr := recover(); rErr != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Failed to create partition coroutine: %v\n", ins.config.GroupId, rErr)
						}
					}
				}()
				messages, err := ins.s.o.message.Obtain(context.Background(), ins.config.GroupId, &p, 1)
				if err != nil {
					if ins.s.o.logger != nil {
						ins.s.o.logger.Errorf("[%s] Schedule message err: %v, messages:%v\n", ins.config.GroupId, err, messages)
					}
					return
				}

				if len(messages) == 0 {
					if _, err = ins.c.Commit(); err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Schedule Commit err: %v, messages:%v\n", ins.config.GroupId, err, messages)
						}
					}
					return
				}

				for _, msg := range messages {
					if _, err = ins.c.CommitOffsets([]kafka.TopicPartition{msg.TopicPartition}); err != nil {
						if ins.s.o.logger != nil {
							ins.s.o.logger.Errorf("[%s] Schedule CommitOffsets err: %v, messages:%v\n", ins.config.GroupId, err, msg)
						}
						return
					}
				}

			}))
		}

		strategy, err := ins.s.o.config.KCM.Get("partition.assignment.strategy", kafka.ConfigValue("range"))
		if err != nil {
			return err
		}
		if strategy == kafka.ConfigValue("cooperative-sticky") {
			// The application may update the start .Offset of each
			// assigned partition and then call IncrementalAssign().
			// Even though this example does not alter the offsets we
			// provide the call to IncrementalAssign() as an example.
			err = c.IncrementalAssign(ev.Partitions)
			if err != nil {
				return err
			}
		} else {

			// The application may update the start .Offset of each assigned
			// partition and then call Assign(). It is optional to call Assign
			// in case the application is not modifying any start .Offsets. In
			// that case we don't, the library takes care of it.
			// It is called here despite not modifying any .Offsets for illustrative
			// purposes.
			err = c.Assign(ev.Partitions)
			if err != nil {
				return err
			}
		}

	case kafka.RevokedPartitions:
		if ins.s.o.logger != nil {
			ins.s.o.logger.Infof("%s rebalance: %d partition(s) revoked: %v\n",
				c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)
		}

		// Usually, the rebalance callback for `RevokedPartitions` is called
		// just before the partitions are revoked. We can be certain that a
		// partition being revoked is not yet owned by any other consumer.
		// This way, logic like storing any pending offsets or committing
		// offsets can be handled.
		// However, there can be cases where the assignment is lost
		// involuntarily. In this case, the partition might already be owned
		// by another consumer, and operations including committing
		// offsets may not work.
		if c.AssignmentLost() {
			// Our consumer has been kicked out of the group and the
			// entire assignment is thus lost.
			if ins.s.o.logger != nil {
				ins.s.o.logger.Infof("Assignment lost involuntarily, commit may fail\n")
			}
		}

		// Since enable.auto.commit is unset, we need to commit offsets manually
		// before the partition is revoked.
		commitedOffsets, err := c.Commit()

		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			if ins.s.o.logger != nil {
				ins.s.o.logger.Errorf("Failed to commit offsets: %s\n", err)
			}
			return err
		}
		if ins.s.o.logger != nil {
			ins.s.o.logger.Infof("Commited offsets to Kafka: %v\n", commitedOffsets)
		}

		// Similar to Assign, client automatically calls Unassign() unless the
		// callback has already called that method. Here, we don't call it.

	default:
		if ins.s.o.logger != nil {
			ins.s.o.logger.Infof("Unxpected event type: %v\n", event)
		}
	}

	return nil
}

type EveryScheduler struct {
	Interval time.Duration
}

func (s *EveryScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}
