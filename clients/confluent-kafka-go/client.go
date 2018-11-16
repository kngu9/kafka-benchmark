// Package confluent uses the confluent kafka go library
// github.com/confluentinc/confluent-kafka-go
package confluent

import (
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/juju/errors"

	"github.com/kngu9/kafka-benchmark/clients"
)

// Client is the confluent-kafka-go client
type Client struct {
	clients.Config

	producer *kafka.Producer
}

// New creates a new confluent-kafka-go client
func New(cfg clients.Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Trace(err)
	}

	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": strings.Join(cfg.Brokers, ","),
		},
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
	return "confluent-kafka-go"
}

// Produce is the implementation of producing in the confluent-kafka-go client
func (c *Client) Produce(buff []byte) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	c.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &c.Topic,
				Partition: kafka.PartitionAny,
			},
			Value: buff,
		},
		deliveryChan,
	)

	select {
	case evt := <-deliveryChan:
		msg := evt.(*kafka.Message)

		if msg.TopicPartition.Error != nil {
			return errors.Annotate(msg.TopicPartition.Error, "confluent-kafka-go event error")
		}
		return nil
	case <-time.After(c.Timeout):
		return errors.New("confluent-kafka-go timed out")
	}
}

// Close terminates the connection
func (c *Client) Close() error {
	c.producer.Close()
	return nil
}
