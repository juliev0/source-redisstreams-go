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
Main methods to test:
Read()
Pending()
processXReadResult() (unless we feel it's covered by Read())
possibly some of the smaller helper functions at the end

For Read():
- have a very short time out which causes us to run out of time (only 0 can work due to timing differences) - actually is 0 allowed?
- if above test is no good, another thing we could do is loop doing Read() and just make sure we can get everything in a reasonable amount of time
- need to have something received and not acked and then a new redisStreamsSource comes up to Read(), and after that can read new messages

*/

var (
	redisURI = ":6379"

	streamName = "test-stream"

	consumerGroupName = "my-group"

	redisOptions = &redis.UniversalOptions{
		Addrs: []string{redisURI},
	}

	multipleKeysValues = map[string]string{"test-msg-1": "test-val-1", "test-msg-2": "test-val-2"}

	//multipleKeysValuesJson = json.Marshal(multipleKeysValues)
)

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

func Test_Read(t *testing.T) {

}

func Test_Read_WithBacklog(t *testing.T) {
	os.Setenv("NUMAFLOW_REPLICA", "1")
	os.Setenv("NUMAFLOW_DEBUG", "true")
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
	err = publishClient.Client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: multipleKeysValues,
	}).Err()
	assert.NoError(t, err)

	// Source reads the 1 message but doesn't Ack
	msgChannel := make(chan sourcesdk.Message, 50)
	source.Read(context.Background(), &readRequest{count: 10, timeout: 5 * time.Second}, msgChannel)
	close(msgChannel)
	numMsgs := countMessages(msgChannel)
	assert.Equal(t, 1, numMsgs)

	// no Ack

	// another 2 messages published
	for i := 0; i < 2; i++ {
		err = publishClient.Client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: multipleKeysValues,
		}).Err()
		assert.NoError(t, err)
	}

	// second RedisStreamsSource with same ConsumerGroup, and same Consumer (imitating a Pod that got restarted) Reads and gets
	// 1 backlog message plus 2 new messages
	source, err = New(config, utils.NewLogger())
	assert.NoError(t, err)
	msgChannel = make(chan sourcesdk.Message, 50)
	source.Read(context.Background(), &readRequest{count: 10, timeout: 5 * time.Second}, msgChannel)
	close(msgChannel)
	numMsgs = countMessages(msgChannel)
	assert.Equal(t, 3, numMsgs)

	// this time it Acks all 3 messages

	// imitate the Pod getting restarted again: this time there should be no messages to read
}

// channel must be closed for this to work
func countMessages(msgChannel chan sourcesdk.Message) int {
	msgCounts := 0
	for range msgChannel {
		msgCounts++
	}
	return msgCounts
}
