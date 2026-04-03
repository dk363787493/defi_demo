package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Chains   []ChainConfig  `mapstructure:"chains"`
	Log      LogConfig      `mapstructure:"log"`
}

type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type DatabaseConfig struct {
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
	DBName          string `mapstructure:"dbname"`
	MaxOpenConns    int    `mapstructure:"max_open_conns"`
	MaxIdleConns    int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetime string `mapstructure:"conn_max_lifetime"`
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		d.User, d.Password, d.Host, d.Port, d.DBName)
}

func (d DatabaseConfig) ConnMaxLifetimeDuration() time.Duration {
	dur, err := time.ParseDuration(d.ConnMaxLifetime)
	if err != nil {
		return 5 * time.Minute
	}
	return dur
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers []string          `mapstructure:"brokers"`
	Topics  KafkaTopicsConfig `mapstructure:"topics"`
}

type KafkaTopicsConfig struct {
	ChainEvents   string `mapstructure:"chain_events"`
	PriceUpdates  string `mapstructure:"price_updates"`
	Notifications string `mapstructure:"notifications"`
}

type ContractsConfig struct {
	LendingPool string `mapstructure:"lending_pool"`
}

type ChainConfig struct {
	Name          string          `mapstructure:"name"`
	ChainID       int64           `mapstructure:"chain_id"`
	RPCURLs       []string        `mapstructure:"rpc_urls"`
	WSURL         string          `mapstructure:"ws_url"`
	BlockTime     int             `mapstructure:"block_time"`
	Confirmations int             `mapstructure:"confirmations"`
	Contracts     ContractsConfig `mapstructure:"contracts"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetEnvPrefix("DEFI")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
