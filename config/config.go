package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type NodeConfig struct {
	ID   uint32 `mapstructure:"id"`
	Hash uint32 `mapstructure:"hash"`
	Host string `mapstructure:"host"`
	Port uint16 `mapstructure:"port"`
}

type Config struct {
	Node NodeConfig `mapstructure:"node"`
}

func (c NodeConfig) ToAddress() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func initConfig(vip *viper.Viper) Config {
	cfg := Config{}

	err := vip.Unmarshal(&cfg)
	if err != nil {
		panic(err)
	}

	return cfg
}

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
