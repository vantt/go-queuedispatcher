package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Configuration structure
type Configuration struct {
	Brokers []broker `yaml:"brokers"`
}

type broker struct {
	Scheduler  string           `yaml:"scheduler"`
	WorkerPool workerpool       `yaml:"workerpool"`
	Queues     map[string]queue `yaml:"queues"`
}

type workerpool struct {
	WorkerType string `yaml:"workertype"`
	Endpoint   string `yaml:"endpoint"`
	Concurrent uint   `yaml:"concurrent"`
}

type queue struct {
	Priority uint     `yaml:"priority"`
	Topics   []string `yaml:"topics"`
}

// ParseConfig will find and Parse Config
func ParseConfig() Configuration {
	cfg := viper.New()
	cfg.SetConfigName("config")
	cfg.AddConfigPath("/etc/go-queuedispatcher")
	cfg.AddConfigPath("$HOME/.go-queuedispatcher")
	cfg.AddConfigPath(".") // optionally look for config in the working directory

	err := cfg.ReadInConfig() // Find and read the config file
	if err != nil {           // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}

	var config Configuration

	err = cfg.Unmarshal(&config)
	if err != nil {
		panic("Unable to unmarshal config")
	}

	return config
}
