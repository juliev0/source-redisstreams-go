package main

import (
	"context"
	"fmt"
	"os"

	"github.com/numaproj/numaflow-go/pkg/sourcer"

	"github.com/numaproj-contrib/source-redisstreams-go/pkg/config"
	"github.com/numaproj-contrib/source-redisstreams-go/pkg/redisstreams"
	"github.com/numaproj-contrib/source-redisstreams-go/pkg/utils"
)

func main() {
	logger := utils.NewLogger()

	config, err := getConfigFromFile() // todo: maybe turn into an environment variable
	if err != nil {
		logger.Panic("Failed to parse config file : ", err)
	} else {
		logger.Infof("Successfully parsed config file: %+v", config)
	}
	redisStreamsSource, err := redisstreams.New(config, logger)

	if err != nil {
		logger.Panic("Failed to create redis streams source : ", err)
	}

	defer redisStreamsSource.Close()
	err = sourcer.NewServer(redisStreamsSource).Start(context.Background())

	if err != nil {
		logger.Panic("Failed to start source server : ", err)
	}
}

func getConfigFromFile() (*config.RedisStreamsSourceConfig, error) {
	parser := &config.YAMLConfigParser{}
	content, err := os.ReadFile(fmt.Sprintf("%s/config.yaml", utils.ConfigVolumePath))
	if err != nil {
		return nil, err
	}
	return parser.Parse(string(content))
}
