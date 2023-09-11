package config

import (
	"encoding/json"

	"gopkg.in/yaml.v2"
)

//TODO: move to shared location

// Parser is an interface for parsing a string into a Config object
type Parser interface {
	// Parse parses a string into a Config object
	Parse(configString string) (*Config, error)
	// UnParse converts a Config object into a string
	UnParse(config *Config) (string, error)
}

type YAMLConfigParser struct{}

func (p *YAMLConfigParser) Parse(configString string) (*Config, error) {
	c := &Config{}
	err := yaml.Unmarshal([]byte(configString), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (p *YAMLConfigParser) UnParse(config *Config) (string, error) {
	b, err := yaml.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type JSONConfigParser struct{}

func (p *JSONConfigParser) Parse(configString string) (*Config, error) {
	c := &Config{}
	err := json.Unmarshal([]byte(configString), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (p *JSONConfigParser) UnParse(config *Config) (string, error) {
	b, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
