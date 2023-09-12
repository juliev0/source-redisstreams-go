/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// TODO: this is copy pasted code - are we okay with that?
// Actually, I did remove the NewInClusterRedisClient() function for greater simplicity, though
package redisstreams

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

const ReadFromEarliest = "0-0"
const ReadFromLatest = "$"

// RedisContext is used to pass the context specifically for REDIS operations.
// A cancelled context during SIGTERM or Ctrl-C that is propagated down will throw a context cancelled error because redis uses context to obtain connection from the connection pool.
// All redis operations will use the below no-op context.Background() to try to process in-flight messages that we have received prior to the cancellation of the context.
var RedisContext = context.Background()

// RedisClient datatype to hold redis client attributes.
// TODO: can probably make lowercase
type RedisClient struct {
	Client redis.UniversalClient
}

// NewRedisClient returns a new Redis Client.
func NewRedisClient(options *redis.UniversalOptions) *RedisClient {
	client := new(RedisClient)
	client.Client = redis.NewUniversalClient(options)
	return client
}

// CreateStreamGroup creates a redis stream group and creates an empty stream if it does not exist.
func (cl *RedisClient) CreateStreamGroup(ctx context.Context, stream string, group string, start string) error {
	return cl.Client.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

// DeleteStreamGroup deletes the redis stream group.
func (cl *RedisClient) DeleteStreamGroup(ctx context.Context, stream string, group string) error {
	return cl.Client.XGroupDestroy(ctx, stream, group).Err()
}

// DeleteKeys deletes a redis keys
func (cl *RedisClient) DeleteKeys(ctx context.Context, keys ...string) error {
	return cl.Client.Del(ctx, keys...).Err()
}

// StreamInfo returns redis stream info
func (cl *RedisClient) StreamInfo(ctx context.Context, streamKey string) (*redis.XInfoStream, error) {
	return cl.Client.XInfoStream(ctx, streamKey).Result()
}

// StreamGroupInfo returns redis stream group info
func (cl *RedisClient) StreamGroupInfo(ctx context.Context, streamKey string) ([]redis.XInfoGroup, error) {
	return cl.Client.XInfoGroups(ctx, streamKey).Result()
}

// IsStreamExists check the redis keys exists
func (cl *RedisClient) IsStreamExists(ctx context.Context, streamKey string) bool {
	_, err := cl.StreamInfo(ctx, streamKey)
	return err == nil
}

// PendingMsgCount returns how many messages are pending.
func (cl *RedisClient) PendingMsgCount(ctx context.Context, streamKey, consumerGroup string) (int64, error) {
	cmd := cl.Client.XPending(ctx, streamKey, consumerGroup)
	pending, err := cmd.Result()
	if err != nil {
		return 0, err
	}
	return pending.Count, nil
}

// IsStreamGroupExists check the stream group exists
func (cl *RedisClient) IsStreamGroupExists(ctx context.Context, streamKey string, groupName string) bool {
	result, err := cl.StreamGroupInfo(ctx, streamKey)
	if err != nil {
		return false
	}
	if len(result) == 0 {
		return false
	}
	for _, groupInfo := range result {
		if groupInfo.Name == groupName {
			return true
		}
	}
	return false
}

func IsAlreadyExistError(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}

func NotFoundError(err error) bool {
	return strings.Contains(err.Error(), "requires the key to exist")
}

func GetRedisStreamName(s string) string {
	return fmt.Sprintf("{%s}", s)
}
