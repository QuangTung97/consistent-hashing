package config

import (
	"fmt"
	"sharding/core"

	"github.com/spf13/viper"
)

// NodeConfig for configure node info
type NodeConfig struct {
	ID   core.NodeID `mapstructure:"id"`
	Hash core.Hash   `mapstructure:"hash"`
	Host string      `mapstructure:"host"`
	Port uint16      `mapstructure:"port"`
}

// ProxyConfig for configure proxy
type ProxyConfig struct {
	Port uint16 `mapstructure:"port"`
}

// Config for app config
type Config struct {
	Nodes []NodeConfig `mapstructure:"nodes"`
	Proxy ProxyConfig  `mapstructure:"proxy"`
}

// ToAddress constructs a full address
func (c NodeConfig) ToAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ToGatewayAddress constructs a full address of grpc gateway
func (c NodeConfig) ToGatewayAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port+100)
}

// ToListenAddr to form :5000
func (c NodeConfig) ToListenAddr() string {
	return fmt.Sprintf(":%d", c.Port)
}

// ToGatewayListenAddr to form :5100
func (c NodeConfig) ToGatewayListenAddr() string {
	return fmt.Sprintf(":%d", c.Port+100)
}

func initConfig(vip *viper.Viper) Config {
	cfg := Config{}

	err := vip.Unmarshal(&cfg)
	if err != nil {
		panic(err)
	}

	return cfg
}

// LoadConfig loads the config from file
func LoadConfig() Config {
	vip := viper.New()
	vip.SetConfigName("config")
	vip.SetConfigType("yml")
	vip.AddConfigPath(".")

	err := vip.ReadInConfig()
	if err != nil {
		panic(err)
	}

	return initConfig(vip)
}
