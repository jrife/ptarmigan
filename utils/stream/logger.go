package stream

import "go.uber.org/zap"

// Log logs values as they pass through.
func Log(logger *zap.Logger) Processor {
	return func(stream Stream) Stream {
		return &loggedStream{stream, logger}
	}
}

type loggedStream struct {
	Stream
	logger *zap.Logger
}

func (stream *loggedStream) Next() bool {
	if !stream.Stream.Next() {
		return false
	}

	stream.logger.Debug("next value", zap.Any("value", stream.Value()))

	return true
}
