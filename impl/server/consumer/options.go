package consumer

import (
	"github.com/x-thooh/kafka-delay/api"
	isp "github.com/x-thooh/kafka-delay/impl/server/producer"
	pkf "github.com/x-thooh/kafka-delay/pkg/kafka"
	"github.com/x-thooh/kafka-delay/pkg/timingwheel"
)

// Option is an application option.
type Option func(o *options)

// options is an application options.
type options struct {
	message     api.IMessage
	consumer    api.IConsumer
	timingWheel *timingwheel.TimingWheel
	producer    *isp.Producer

	logger api.Logger
	config *pkf.Config
}

func WithMessage(message api.IMessage) Option {
	return func(o *options) { o.message = message }
}

func WithConsumer(c api.IConsumer) Option {
	return func(o *options) {
		o.consumer = c
	}
}

func WithProducer(p *isp.Producer) Option {
	return func(o *options) { o.producer = p }
}

func WithLogger(logger api.Logger) Option {
	return func(o *options) { o.logger = logger }
}

func WithConfig(config *pkf.Config) Option {
	return func(o *options) { o.config = config }
}
