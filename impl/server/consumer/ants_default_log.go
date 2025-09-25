package consumer

import (
	"github.com/panjf2000/ants/v2"
	"github.com/x-thooh/kafka-delay/api"
)

type antsDefaultLog struct {
	logger api.Logger
}

func NewAntsDefaultLog(logger api.Logger) ants.Logger {
	return &antsDefaultLog{
		logger: logger,
	}
}

func (a *antsDefaultLog) Printf(format string, args ...any) {
	a.logger.Debugf(format, args...)
}
