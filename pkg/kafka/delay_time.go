package kafka

import (
	"time"
)

var (
	// 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	mDelayTime = []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
		3 * time.Minute,
		4 * time.Minute,
		5 * time.Minute,
		6 * time.Minute,
		7 * time.Minute,
		8 * time.Minute,
		9 * time.Minute,
		10 * time.Minute,
		20 * time.Minute,
		30 * time.Minute,
		1 * time.Hour,
		2 * time.Hour,
	}

	defaultDelayLevel = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}

	DefaultRelayLevel = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
)

func GetDelayTime(idx int) time.Duration {
	if idx < 0 {
		idx = 0
	}
	return mDelayTime[idx]
}
