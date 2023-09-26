# source-redisstreams-go
User-defined Source for Numaflow for Redis Streams



# To run Unit tests
Redis needs to be brought up independently in a Docker container. You can run:

```
docker run -p 6379:6379 -e REDIS_ARGS="--health-interval 10s --health-timeout 5s --health-retries 5" redis:6.2
```
