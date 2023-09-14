package redisstreams

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/numaproj-contrib/source-redisstreams-go/pkg/config"
	"github.com/numaproj-contrib/source-redisstreams-go/pkg/utils"
	sourcesdk "github.com/numaproj/numaflow-go/pkg/sourcer"
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
	checkBackLog bool

	*RedisClient
	//Options // TODO: make sure these get incorporated but not as options
	Log *zap.SugaredLogger
}

const ReadFromEarliest = "0-0"
const ReadFromLatest = "$"

//type Option func(*redisStreamsSource) error

/*
// WithReadTimeOut sets the read timeout
func WithReadTimeOut(t time.Duration) Option {
	return func(o *redisStreamsSource) error {
		o.Options.ReadTimeOut = t
		return nil
	}
}*/

func New(c *config.Config) (*redisStreamsSource, error) {
	redisClient, err := newRedisClient(c)
	if err != nil {
		return nil, err
	}

	replica := os.Getenv("NUMAFLOW_REPLICA")
	if replica == "" {
		return nil, fmt.Errorf("NUMAFLOW_REPLICA environment variable not set: can't set Consumer name")
	}

	redisStreamsSource := &redisStreamsSource{
		Stream:       c.Stream,
		Group:        c.ConsumerGroup,
		Consumer:     fmt.Sprintf("%s-%v", c.ConsumerGroup, replica),
		checkBackLog: true,
		RedisClient:  redisClient,
		Log:          utils.NewLogger(),
	}

	// create the ConsumerGroup here if not already created
	err = redisStreamsSource.createConsumerGroup(ctx, c)
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
func (rsSource *redisStreamsSource) Pending(_ context.Context) int64 {
	// try calling XINFO GROUPS <stream> and look for 'Lag' key.
	// For Redis Server < v7.0, this always returns 0; therefore it's recommended to use >= v7.0

	result := rsSource.Client.XInfoGroups(RedisContext, rsSource.Stream)
	groups, err := result.Result()
	if err != nil {
		rsSource.Log.Errorf("error calling XInfoGroups: %v", err)
		return isb.PendingNotAvailable
	}
	// find our ConsumerGroup
	for _, group := range groups {
		if group.Name == rsSource.Group {
			return group.Lag
		}
	}
	return isb.PendingNotAvailable, fmt.Errorf("ConsumerGroup %q not found in XInfoGroups result %+v", rsSource.Group, groups)

}

func (rsSource *redisStreamsSource) Read(_ context.Context, readRequest sourcesdk.ReadRequest, messageCh chan<- sourcesdk.Message) {

	functionStartTime := time.Now()
	//blocking := (int64(readRequest.TimeOut()) > 0)
	var finalTime time.Time
	//if blocking {
	finalTime = functionStartTime.Add(readRequest.TimeOut())
	//}
	remainingMsgs := readRequest.Count()

	// if there are messages previously delivered to us that we didn't acknowledge, repeatedly check for that until there are no
	// messages returned, or until we reach timeout
	// "0-0" means messages previously delivered to us that we didn't acknowledge

	for rsSource.checkBackLog {

		xstreams, err := rsSource.processXReadResult("0-0", remainingMsgs, 0*time.Second)
		if err != nil {
			return rsSource.processReadError(xstreams, origMessages, err)
		}

		// NOTE: If all messages have been delivered and acknowledged, the XREADGROUP 0-0 call returns an empty
		// list of messages in the stream. At this point we want to read everything from last delivered which would be indicated by ">"
		if len(xstreams) == 1 {
			if len(xstreams[0].Messages) == 0 {
				rsSource.Log.Infow("We have delivered and acknowledged all PENDING msgs, setting checkBacklog to false")
				rsSource.checkBackLog = false
				break
			} else {
				remainingMsgs -= len(xstreams[0].Messages)
			}
		} else {
			//todo: shouldn't need this statement - verify
		}

		if time.Now().Compare(finalTime) >= 0 || remainingMsgs <= 0 {
			// todo: return
		}
	}

	// get undelivered messages up to the count we want and block until the timeout
	if !rsSource.checkBackLog {
		remainingTime := 0 * time.Second
		if readRequest.TimeOut() > 0 {
			remainingTime = time.Now().Sub(finalTime)
			if int64(remainingTime) < 0 {
				// todo: return
			}
		}
		// this call will block until either the "remainingMsgs" count has been fulfilled or the remainingTime has elapsed
		// note: if we ever need true "streaming" here, we should instead repeatedly call with no timeout
		xstreams, err := rsSource.processXReadResult(">", remainingMsgs, remainingTime)
		if err != nil {
			return rsSource.processReadError(xstreams, origMessages, err)
		}
	}

}

// Ack acknowledges the data from the source.
func (rsSource *redisStreamsSource) Ack(_ context.Context, request sourcesdk.AckRequest) {

}

func (rsSource *redisStreamsSource) Close() error {
	rsSource.Log.Info("Shutting down redis streams source server...")
	rsSource.Log.Info("Redis streams source server shutdown")
	return nil
}

// processXReadResult is used to process the results of XREADGROUP
// for any messages read, stream them back on message channel
func (rsSource *redisStreamsSource) processXReadResult(startIndex string, count int64, blockDuration time.Duration, messageCh chan<- sourcesdk.Message) ([]redis.XStream, error) {

	result := rsSource.Client.XReadGroup(RedisContext, &redis.XReadGroupArgs{
		Group:    rsSource.Group,
		Consumer: rsSource.Consumer,
		Streams:  []string{rsSource.Stream, startIndex},
		Count:    count,
		Block:    blockDuration, // todo: verify that passing in 0 effectively causes it not to block
	})
	xstreams, err := result.Result()
	if err != nil {
		return xstreams, err
	}
	if len(xstreams) > 1 {
		rsSource.Log.Warnf("redisStreamsSource shouldn't have more than one Stream; xstreams=%+v", xstreams)
		return xstreams, nil
	} else if len(xstreams) == 1 {
		msgs, err := rsSource.xStreamToMessages(xstreams[0])
		if err != nil {
			return xstreams, err
		}

		for _, m := range msgs {
			messageCh <- *m
		}
	}

	//return result.Result()
}

func (rsSource *redisStreamsSource) xStreamToMessages(xstream redis.XStream) ([]*sourcesdk.Message, error) {
	messages := []*sourcesdk.Message{}
	for _, message := range xstream.Messages {
		var outMsg *sourcesdk.Message
		var err error
		if len(message.Values) >= 1 {
			outMsg, err = rsSource.produceMsg(message)
			if err != nil {
				return nil, err
			}
		} else {
			// don't think there should be a message with no values, but if there is...
			rsSource.Log.Warnf("unexpected: RedisStreams message has no values? message=%+v", message)
			continue
		}
		messages = append(messages, outMsg)
	}

	return messages, nil
}
