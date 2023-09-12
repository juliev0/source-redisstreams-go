package redisstreams

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/numaproj-contrib/source-redisstreams-go/pkg/config"
	"github.com/numaproj-contrib/source-redisstreams-go/pkg/utils"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// TODO: could move this
type Message struct {
	payload    string
	readOffset string
	id         string
}

/*type Options struct {
	// ReadTimeOut is the timeout needed for read timeout
	ReadTimeOut time.Duration //todo: consider if we need this now that we are reading asynchronously
}*/

type redisStreamsSource struct {
	//TODO: can probably make these lower case
	Stream   string
	Group    string
	Consumer string
	//PartitionIdx int32

	*RedisClient
	//Options // TODO: make sure these get incorporated but not as options
	Log *zap.SugaredLogger
}

const ReadFromEarliest = "0-0"
const ReadFromLatest = "$"

//type Option func(*redisStreamsSource) error

// WithLogger is used to return logger information
// todo: use like Keran is doing or don't bother?
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *redisStreamsSource) error {
		o.Log = l
		return nil
	}
}

/*
// WithReadTimeOut sets the read timeout
func WithReadTimeOut(t time.Duration) Option {
	return func(o *redisStreamsSource) error {
		o.Options.ReadTimeOut = t
		return nil
	}
}*/

func New(c *config.Config, opts ...Option) (*redisStreamsSource, error) {
	redisClient, err := newRedisClient(c)
	if err != nil {
		return nil, err
	}

	logger, _ := zap.NewDevelopment()

	replica := os.Getenv("NUMAFLOW_REPLICA")
	if replica == "" {
		return nil, fmt.Errorf("NUMAFLOW_REPLICA environment variable not set: can't set Consumer name")
	}

	redisStreamsSource := &redisStreamsSource{
		Stream:      c.Stream,
		Group:       c.ConsumerGroup,
		Consumer:    fmt.Sprintf("%s-%v", c.ConsumerGroup, replica),
		RedisClient: redisClient,
		Log:         logger,
	}

	// create the ConsumerGroup here if not already created
	err = createConsumerGroup(ctx, redisSpec)
	if err != nil {
		return nil, err
	}

	return redisStreamsSource, nil
}

// create a RedisClient
func newRedisClient(c *config.Config) (*RedisClient, error) {
	volumeReader := utils.NewVolumeReader(utils.SecretVolumePath)
	password, _ := volumeReader.GetSecretFromVolume(c.Password)
	opts := &redis.UniversalOptions{
		Username:     c.User,
		Password:     password,
		MasterName:   c.MasterName,
		MaxRedirects: 3,
	}
	if opts.MasterName != "" {
		urls := c.SentinelURL
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
		sentinelPassword, _ := volumeReader.GetSecretFromVolume(c.SentinelPassword)
		opts.SentinelPassword = os.Getenv(sentinelPassword)
	} else {
		urls := c.URL
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
	}

	return NewRedisClient(opts), nil
}

func (rsSource *redisStreamsSource) createConsumerGroup(ctx context.Context, c *config.Config) error {
	// user can configure to read stream either from the beginning or from the most recent messages
	readFrom := ReadFromLatest
	if c.ReadFromBeginning {
		readFrom = ReadFromEarliest
	}
	rsSource.Log.Infof("Creating Redis Stream group %q on Stream %q (readFrom=%v)", rsSource.Group, rsSource.Stream, readFrom)
	err := rsSource.RedisClient.CreateStreamGroup(ctx, rsSource.Stream, rsSource.Group, readFrom)
	if err != nil {
		if IsAlreadyExistError(err) {
			rsSource.Log.Infow("Consumer Group on Stream already exists.", zap.String("group", rsSource.Group), zap.String("stream", rsSource.Stream))
		} else {
			return fmt.Errorf("failed to create consumer group %q on redis stream %q: err=%v", rsSource.Group, rsSource.Stream, err)
		}
	}
	return nil
}

// Pending returns the number of pending records.
func (rsSource *redisStreamsSource) Pending(_ context.Context) uint64 {
	// try calling XINFO GROUPS <stream> and look for 'Lag' key.
	// For Redis Server < v7.0, this always returns 0; therefore it's recommended to use >= v7.0

	result := br.Client.XInfoGroups(RedisContext, br.Stream)
	groups, err := result.Result()
	if err != nil {
		rsSource.Log.Errorf("error calling XInfoGroups: %v", err)
		return isb.PendingNotAvailable
	}
	// find our ConsumerGroup
	for _, group := range groups {
		if group.Name == br.Group {
			return group.Lag, nil
		}
	}
	return isb.PendingNotAvailable, fmt.Errorf("ConsumerGroup %q not found in XInfoGroups result %+v", br.Group, groups)

}
