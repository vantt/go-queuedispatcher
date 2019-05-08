package config

import (
	"github.com/spf13/viper"
	"github.com/spf13/pflag"

	//"github.com/fsnotify/fsnotify"
)

// Configuration structure
type Configuration struct {
	Logging LoggerConfig `yaml:"logging"`
	Brokers []BrokerConfig `yaml:"brokers"`
	Viper *viper.Viper
}

// LoggerConfig ....
type LoggerConfig struct {
	Filename  string `yaml:"filename"`
 	MaxSize  int  `yaml:"MaxSize"` // megabytes
	MaxBackups int `yaml:"MaxBackups"`
	MaxAge int     `yaml:"MaxAge"` // days
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
func ParseConfig() (config *Configuration, err error) {
	cfg := viper.New()

	filepath := pflag.StringP("config", "c", "na", "the path of the configuration file")
	pflag.Parse()

	if (*filepath) != "na" {				
		cfg.SetConfigFile(*filepath)
	} else {
		cfg.SetConfigName("godispatcher")
		cfg.AddConfigPath("/etc/godispatcher")
		cfg.AddConfigPath("$HOME/.godispatcher")
		cfg.AddConfigPath(".") // optionally look for config in the working directory
	}
	
	// Find and read the config file
	if err = cfg.ReadInConfig(); err != nil {
		return nil, err
	} 

	// cfg.WatchConfig()
	// cfg.OnConfigChange(func(e fsnotify.Event) {
	// 	fmt.Println("Config file changed:", e.Name)
	// })

	if err = cfg.Unmarshal(&config); err == nil {
		config.Viper = cfg
	}

	return
}
