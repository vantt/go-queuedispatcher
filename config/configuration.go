package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Configuration structure
type Configuration struct {
	Brokers []BrokerConfig `yaml:"brokers"`
}

// BrokerConfig ...
type BrokerConfig struct {
	Scheduler      string   `yaml:"scheduler"`
	Host           string   `yaml:"host"`
	WorkerType     string   `yaml:"workertype"`
	WorkerEndpoint []string `yaml:"workerendpoint"`
	Concurrent     uint16   `yaml:"concurrent"`
	Queues         []queue  `yaml:"queues"`
	queuesPriority map[string]uint64
}

type queue struct {
	Priority   uint64   `yaml:"priority"`
	QueueNames []string `yaml:"queuenames"`
}

// GetTopicPriority ...
func (bc *BrokerConfig) GetTopicPriority(topicName string) uint64 {

	if bc.queuesPriority == nil {
		bc.queuesPriority = make(map[string]uint64)

		for _, queue := range bc.Queues {
			for _, qName := range queue.QueueNames {
				bc.queuesPriority[qName] = queue.Priority
			}
		}
	}

	// spew.Dump(bc.queuesPriority)
	// panic("out")
	var (
		priority uint64
		found    bool
	)

	if priority, found = bc.queuesPriority[topicName]; !found {
		if priority, found = bc.queuesPriority["others"]; !found {
			priority = 0
		}
	}

	return priority
}

// ParseConfig will find and Parse Config
func ParseConfig() *Configuration {
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

	return &config
}
