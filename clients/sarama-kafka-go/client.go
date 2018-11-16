// Package sarama uses the sarama kafka library
// github.com/Shopify/sarama
package sarama

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"

	"github.com/kngu9/kafka-benchmark/clients"
)

// Client is the confluent-kafka-go client
type Client struct {
	clients.Config

	producer sarama.SyncProducer
}

// New creates a new confluent-kafka-go client
func New(cfg clients.Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Trace(err)
	}

	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(
		cfg.Brokers,
		producerCfg,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
		producer: producer,
		Config:   cfg,
	}, nil
}

// Name returns the name of the current library
func (c *Client) Name() string {
	return "shopify-sarama"
}

// Produce is the implementation of producing in the sarama kafka go library
func (c *Client) Produce(buff []byte) error {
	deliveryChan := make(chan error)

	go func() {
		_, _, err := c.producer.SendMessage(
			&sarama.ProducerMessage{
				Topic: c.Topic,
				Value: sarama.ByteEncoder(buff),
			},
		)
		deliveryChan <- err
	}()

	select {
	case err := <-deliveryChan:
		if err != nil {
			return errors.Annotate(err, "shopify-sarama event error")
		}
		return nil
	case <-time.After(c.Timeout):
		return errors.New("shopify-sarama timed out")
	}
}

// Close terminates the connection
func (c *Client) Close() error {
	return errors.Trace(c.producer.Close())
}
