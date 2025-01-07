package postgres

import (
	"time"
)

type Config struct {
	Host           string
	Port           uint16
	Database       string
	User           string
	Password       string
	ConnectTimeout time.Duration
	PollingTimeout time.Duration
	OutputPlugin   string

	ReplicationOptions []ReplicationOption
}

func NewConfig() *Config {
	c := new(Config)
	c.init()
	return c
}

func (c *Config) init() {
	if len(c.Host) == 0 {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.PollingTimeout < 0 {
		c.PollingTimeout = 0
	}
	if c.OutputPlugin == "" {
		c.OutputPlugin = Wal2JsonPlugin
	}
}
