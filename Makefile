.PHONY: test
test:
	go test ./... -race -short -v -timeout 60s

.PHONY: build
build: test
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -o ./dist/redisstreams-source main.go

# todo: don't hardcode image tag here - instead use Git tag
.PHONY: image
image: build
	docker build -t "quay.io/numaio/numaflow-source/redisstreams-source-go:v0.1.0" --target redisstreams-source .

clean:
	-rm -rf ./dist