package clients

import (
	"errors"
	"time"
)

// Config is the general configuration for all kafka clients
type Config struct {
	Brokers []string
	Topic   string
	Timeout time.Duration
}

// Validate determines if the configuration is correct or not
func (c *Config) Validate() error {
	if c.Brokers == nil || len(c.Brokers) < 1 {
		return errors.New("brokers not specified")
	}
	if c.Topic == "" {
		return errors.New("topic not specified")
	}
	if c.Timeout < 1 {
		return errors.New("timeout not specified")
	}

	return nil
}