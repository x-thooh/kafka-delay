package api

// Logger is used for logging formatted messages.
type Logger interface {
	// Debugf logs a message at debug level.
	Debugf(format string, a ...interface{})

	// Infof logs a message at info level.
	Infof(format string, a ...interface{})

	// Warnf logs a message at warnf level.
	Warnf(format string, a ...interface{})

	// Errorf logs a message at error level.
	Errorf(format string, a ...interface{})
}
