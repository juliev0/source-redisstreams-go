package redisstreams

import (
	"context"
	"os"
	"testing"
	"time"

	sourcesdk "github.com/numaproj/numaflow-go/pkg/sourcer"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj-contrib/source-redisstreams-go/pkg/config"
	"github.com/numaproj-contrib/source-redisstreams-go/pkg/utils"
	"github.com/redis/go-redis/v9"
)

/*
todo:
add tests for:
- Pending()
- maybe processXReadResult() (unless we feel it's covered by Read())
- possibly some of the smaller helper functions at the end
*/

var (
	redisURI = ":6379"

	consumerGroupName = "my-group"

	redisOptions = &redis.UniversalOptions{
		Addrs: []string{redisURI},
	}

	multipleKeysValues = map[string]string{"test-msg-1": "test-val-1", "test-msg-2": "test-val-2"}
)

func init() {
	os.Setenv("NUMAFLOW_DEBUG", "true")
}

type readRequest struct {
	count   uint64
	timeout time.Duration
}

func (r *readRequest) TimeOut() time.Duration {
	return r.timeout
}

func (r *readRequest) Count() uint64 {
	return r.count
}

// ackRequest implements the AckRequest interface and is used in the ack handler.
type ackRequest struct {
	offsets []sourcesdk.Offset
}

// Offsets returns the offsets of the records to ack.
func (a *ackRequest) Offsets() []sourcesdk.Offset {
	return a.offsets
}

func Test_Read_MultiConsumer(t *testing.T) {

	streamName := "test-stream-multi-consumer"

	os.Setenv("NUMAFLOW_REPLICA", "1")
	config := &config.RedisStreamsSourceConfig{
		URL:               redisURI,
		Stream:            streamName,
		ConsumerGroup:     consumerGroupName,
		ReadFromBeginning: true,
	}
	source1, err := New(config, utils.NewLogger())
	assert.NoError(t, err)

	os.Setenv("NUMAFLOW_REPLICA", "2")
	source2, err := New(config, utils.NewLogger())

	publishClient := NewRedisClient(redisOptions)
	writeTestMessages(t, publishClient, []string{"1692632086370-0", "1692632086371-0", "1692632086372-0"}, streamName)

	// Source reads the 1 message but doesn't Ack
	numMsgs := read(source1, 2, 5*time.Second)
	assert.Equal(t, 2, numMsgs)

	numMsgs = read(source2, 2, 5*time.Second)
	assert.Equal(t, 1, numMsgs)

}

func Test_Read_WithBacklog(t *testing.T) {
	os.Setenv("NUMAFLOW_REPLICA", "1")
	streamName := "test-stream-backlog"

	// new RedisStreamsSource with ConsumerGroup Reads but does not Ack
	config := &config.RedisStreamsSourceConfig{
		URL:               redisURI,
		Stream:            streamName,
		ConsumerGroup:     consumerGroupName,
		ReadFromBeginning: true,
	}
	source, err := New(config, utils.NewLogger())
	assert.NoError(t, err)

	// 1 new message published
	publishClient := NewRedisClient(redisOptions)
	writeTestMessages(t, publishClient, []string{"1692632086370-0"}, streamName)

	// Source reads the 1 message but doesn't Ack
	numMsgs := readDefault(source)
	assert.Equal(t, 1, numMsgs)

	// no Ack

	// another 2 messages published
	writeTestMessages(t, publishClient, []string{"1692632086371-0", "1692632086372-0"}, streamName)

	// second RedisStreamsSource with same ConsumerGroup, and same Consumer (imitating a Pod that got restarted) reads and gets
	// 1 backlog message, and on subsequent Read, gets 2 new messages
	// this time it Acks all messages
	source, err = New(config, utils.NewLogger())
	assert.NoError(t, err)
	numMsgs = readDefault(source)
	assert.Equal(t, 1, numMsgs)
	// ack
	offset := sourcesdk.NewOffset([]byte("1692632086370-0"), "0")
	source.Ack(context.Background(), &ackRequest{offsets: []sourcesdk.Offset{offset}})

	numMsgs = readDefault(source)
	assert.Equal(t, 2, numMsgs)
	offset = sourcesdk.NewOffset([]byte("1692632086371-0"), "0")
	source.Ack(context.Background(), &ackRequest{offsets: []sourcesdk.Offset{offset}})
	offset = sourcesdk.NewOffset([]byte("1692632086372-0"), "0")
	source.Ack(context.Background(), &ackRequest{offsets: []sourcesdk.Offset{offset}})

	// imitate the Pod getting restarted again: this time there should be no messages to read
	source, err = New(config, utils.NewLogger())
	assert.NoError(t, err)
	numMsgs = readDefault(source)
	assert.Equal(t, 0, numMsgs)
}

// returns the number of messages read in
func read(source *redisStreamsSource, count uint64, duration time.Duration) int {
	msgChannel := make(chan sourcesdk.Message, 50)
	source.Read(context.Background(), &readRequest{count: count, timeout: duration}, msgChannel)
	close(msgChannel)
	return countMessages(msgChannel)
}

func readDefault(source *redisStreamsSource) int {
	return read(source, 10, 5*time.Second)
}

// channel must be closed for this to work
func countMessages(msgChannel chan sourcesdk.Message) int {
	msgCounts := 0
	for range msgChannel {
		msgCounts++
	}
	return msgCounts
}

func writeTestMessages(t *testing.T, publishClient *redisClient, ids []string, streamName string) {
	for _, id := range ids {
		err := publishClient.Client.XAdd(context.Background(), &redis.XAddArgs{
			ID:     id,
			Stream: streamName,
			Values: multipleKeysValues,
		}).Err()
		assert.NoError(t, err)
	}
}
