package config

import (
	"gopkg.in/yaml.v2"
)

// Parser is an interface for parsing a string into a Config object
type Parser interface {
	// Parse parses a string into a Config object
	Parse(configString string) (*RedisStreamsSourceConfig, error)
	// UnParse converts a Config object into a string
	UnParse(config *RedisStreamsSourceConfig) (string, error)
}

type YAMLConfigParser struct{}

func (p *YAMLConfigParser) Parse(configString string) (*RedisStreamsSourceConfig, error) {
	c := &RedisStreamsSourceConfig{}
	err := yaml.Unmarshal([]byte(configString), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (p *YAMLConfigParser) UnParse(config *RedisStreamsSourceConfig) (string, error) {
	b, err := yaml.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
