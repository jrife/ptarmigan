package log

import (
	"context"

	"go.uber.org/zap"
)

type key int

const (
	contextKey key = iota
	loggerKey  key = iota
)

// WithContext enriches the logger with fields from the context
func WithContext(ctx context.Context, logger *zap.Logger) *zap.Logger {
	return logger.With(Fields(ctx)...)
}

// WithFields adds log fields to the context
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	return context.WithValue(ctx, contextKey, append(Fields(ctx), fields...))
}

// Fields extracts log fields from the context
func Fields(ctx context.Context) []zap.Field {
	rawFields := ctx.Value(contextKey)

	if rawFields == nil {
		return []zap.Field{}
	}

	fields, ok := rawFields.([]zap.Field)

	if !ok {
		return []zap.Field{}
	}

	return fields
}

// WithLogger adds a logger to the context
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// Logger extracts a logger from the context
func Logger(ctx context.Context) *zap.Logger {
	rawLogger := ctx.Value(contextKey)

	if rawLogger == nil {
		return nil
	}

	logger, ok := rawLogger.(*zap.Logger)

	if !ok {
		return nil
	}

	return logger
}

// LoggerFromContext attempts to use a logger passed through the context.
// If no logger is passed through the context it uses the default logger and
// attaches the defaultLogger to the context.
func LoggerFromContext(ctx context.Context, defaultLogger *zap.Logger) (*zap.Logger, context.Context) {
	logger := Logger(ctx)

	if logger == nil {
		logger = defaultLogger
		ctx = WithLogger(ctx, logger)
	}

	return logger, ctx
}
