package log

import (
	"context"
	"runtime/debug"

	"github.com/angel-one/go-utils/constants"
	"github.com/angel-one/go-utils/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Level string

func (l Level) zeroLogLevel() zerolog.Level {
	switch l {
	case constants.TraceLevel:
		return zerolog.TraceLevel
	case constants.DebugLevel:
		return zerolog.DebugLevel
	case constants.InfoLevel:
		return zerolog.InfoLevel
	case constants.WarnLevel:
		return zerolog.WarnLevel
	case constants.ErrorLevel:
		return zerolog.ErrorLevel
	case constants.FatalLevel:
		return zerolog.FatalLevel
	case constants.PanicLevel:
		return zerolog.PanicLevel
	default:
		return zerolog.DebugLevel
	}
}

// InitLogger is used to initialize logger
func InitLogger(level Level) {
	zerolog.ErrorStackMarshaler = getErrorStackMarshaller()
	zerolog.SetGlobalLevel(level.zeroLogLevel())
	log.Logger = log.With().Caller().Logger()
}

// Trace is the for trace log
func Trace(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Trace())
}

// Debug is the for debug log
func Debug(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Debug())
}

// Info is the for info log
func Info(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Info())
}

// Warn is the for warn log
func Warn(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Warn())
}

// Error is the for error log
func Error(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Error().Stack())
}

// Panic is the for panic log
func Panic(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Panic().Stack())
}

// Fatal is the for fatal log
func Fatal(ctx context.Context) *zerolog.Event {
	return withIDAndPath(ctx, log.Fatal().Stack())
}

func getErrorStackMarshaller() func(err error) interface{} {
	return func(err error) interface{} {
		if err != nil {
			if e, ok := err.(*errors.Error); ok {
				return map[string]interface{}{
					constants.CodeLogParam:    e.Code,
					constants.MessageLogParam: e.Message,
					constants.DetailsLogParam: e.Details,
					constants.TraceLogParam:   e.GetTrace(),
				}
			}
		}
		return string(debug.Stack())
	}
}

func withIDAndPath(ctx context.Context, event *zerolog.Event) *zerolog.Event {
	if ctx == nil {
		return event
	}
	id := ctx.Value(constants.IDLogParam)
	if id != nil {
		event.Interface(constants.IDLogParam, id)
	}
	path := ctx.Value(constants.PathLogParam)
	if path != nil {
		event.Interface(constants.PathLogParam, path)
	}
	correlationId := ctx.Value(constants.CorrelationLogParam)
	if correlationId != nil {
		event.Interface(constants.CorrelationLogParam, correlationId)
	}
	return event
}
