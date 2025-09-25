package kafka

type Entity struct {
	CascadeMessage string `json:"c,omitempty"`
	CurrentTopic   string `json:"t,omitempty"`
	*EntityOptions
}

func NewEntity() *Entity {
	return &Entity{
		EntityOptions: &EntityOptions{
			DelayLevel:      DefaultRelayLevel,
			DelayLevelIndex: 1,
		},
	}
}

type EntityOptions struct {
	DelayLevel      []int `json:"l,omitempty"` // 18级别: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	DelayLevelIndex int   `json:"i,omitempty"`
	IsDelay         bool  `json:"-"`
}

type EntityOption func(o *EntityOptions)

func WithDelayLevel(delayLevel ...int) EntityOption {
	return func(o *EntityOptions) {
		o.DelayLevel = delayLevel
	}
}
func WithDelayLevelIndex(index int) EntityOption {
	return func(o *EntityOptions) {
		o.DelayLevelIndex = index
	}
}

func WithDelay(isDelay bool) EntityOption {
	return func(o *EntityOptions) {
		o.IsDelay = isDelay
	}
}
