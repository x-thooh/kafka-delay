package kafka

type TopicType int

const (
	Normal TopicType = iota
	Retry
	Delay
)
