package redisstreams

import (
	"time"

	"go.uber.org/zap"
)

// TODO: could move this
type Message struct {
	payload    string
	readOffset string
	id         string
}

type Options struct {
	// ReadTimeOut is the timeout needed for read timeout
	ReadTimeOut time.Duration //todo: consider if we need this now that we are reading asynchronously
}

type redisStreamsSource struct {
	//TODO: can probably make these lower case
	Name         string
	Stream       string
	Group        string
	Consumer     string
	PartitionIdx int32

	*RedisClient
	Options
	Log *zap.SugaredLogger
}

type Option func(*redisStreamsSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *redisStreamsSource) error {
		o.Log = l
		return nil
	}
}

// WithReadTimeOut sets the read timeout
func WithReadTimeOut(t time.Duration) Option {
	return func(o *redisStreamsSource) error {
		o.Options.ReadTimeOut = t
		return nil
	}
}
