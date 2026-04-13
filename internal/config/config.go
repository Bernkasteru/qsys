package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	Env         string        `yaml:"env"`  // dev, test, prod
	Port        int           `yaml:"port"` // cli_svr 监听端口
	SimPort     int           `yaml:"sim_port"`
	DBBatchSize int           `yaml:"db_batch_size"`
	DBBatchTout time.Duration `yaml:"db_batch_tout"`
}

type KafkaConfig struct {
	Brokers      []string      `yaml:"brokers"`  // Kafka broker list
	Topic        string        `yaml:"topic"`    // Kafka topic name
	GroupId      string        `yaml:"group_id"` // consumer group id, upd_svr 需要
	WorkerNum    int           `yaml:"worker_num"`
	MaxQueueSize int           `yaml:"max_queue_size"`
	SendTimeout  time.Duration `yaml:"send_timeout"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

type MySQLConfig struct {
	DSN string `yaml:"dsn"` // user:pass@tcp(host:port)/dbname..
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type Config struct {
	App   AppConfig   `yaml:"app"`
	Kafka KafkaConfig `yaml:"kafka"`
	MySQL MySQLConfig `yaml:"mysql"`
	Redis RedisConfig `yaml:"redis"`
}

var GlobalConfig *Config

func override(cfg *Config) {
	// App 配置覆盖
	if env := os.Getenv("QSYS_ENV"); env != "" {
		cfg.App.Env = env
	}
	if portStr := os.Getenv("QSYS_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			cfg.App.Port = p
		} else {
			fmt.Printf("Warning! Invalid QSYS_PORT env var: %s\n", portStr)
		}
	}
	if spStr := os.Getenv("QSYS_SIM_PORT"); spStr != "" {
		if p, err := strconv.Atoi(spStr); err == nil {
			cfg.App.SimPort = p
		} else {
			fmt.Printf("Warning! Invalid QSYS_SIM_PORT env var: %s\n", spStr)
		}
	}

	if b := os.Getenv("QSYS_KAFKA_BROKERS"); b != "" {
		cfg.Kafka.Brokers = strings.Split(b, ",")
	}
	if t := os.Getenv("QSYS_KAFKA_TOPIC"); t != "" {
		cfg.Kafka.Topic = t
	}
	if dsn := os.Getenv("QSYS_MYSQL_DSN"); dsn != "" {
		cfg.MySQL.DSN = dsn
	}
	if addr := os.Getenv("QSYS_REDIS_ADDR"); addr != "" {
		cfg.Redis.Addr = addr
	}

	// Kafka 运行参数
	if wn := os.Getenv("QSYS_KAFKA_WORKER_NUM"); wn != "" {
		if v, err := strconv.Atoi(wn); err == nil {
			cfg.Kafka.WorkerNum = v
		}
	}
	if mqs := os.Getenv("QSYS_KAFKA_MAX_QUEUE_SIZE"); mqs != "" {
		if v, err := strconv.Atoi(mqs); err == nil {
			cfg.Kafka.MaxQueueSize = v
		}
	}
	if st := os.Getenv("QSYS_KAFKA_SEND_TIMEOUT"); st != "" {
		if v, err := time.ParseDuration(st); err == nil {
			cfg.Kafka.SendTimeout = v
		}
	}
	if bs := os.Getenv("QSYS_KAFKA_BATCH_SIZE"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			cfg.Kafka.BatchSize = v
		}
	}
	if bt := os.Getenv("QSYS_KAFKA_BATCH_TIMEOUT"); bt != "" {
		if v, err := time.ParseDuration(bt); err == nil {
			cfg.Kafka.BatchTimeout = v
		}
	}
}

func setDefault(cfg *Config) {
	cfg.Kafka.Topic = "conorder"
	cfg.Kafka.WorkerNum = 30
	cfg.Kafka.MaxQueueSize = 300
	cfg.Kafka.SendTimeout = 2 * time.Second
	cfg.Kafka.BatchSize = 50
	cfg.Kafka.BatchTimeout = 60 * time.Millisecond
	cfg.Redis.Addr = "redis:6379"
}

func LoadConfig(path string) (*Config, error) {
	_ = godotenv.Load() // 加载 .env 文件中的环境变量
	cfg := &Config{}
	setDefault(cfg)
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Read config failed: %w", err)
	}
	if err := yaml.Unmarshal(file, cfg); err != nil {
		return nil, fmt.Errorf("Unmarshal config failed: %w", err)
	}
	override(cfg) // 环境变量覆盖
	GlobalConfig = cfg

	return cfg, nil
}
