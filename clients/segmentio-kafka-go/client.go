// Package segmentio uses the segmentio kafka go library
// github.com/segmentio/kafka-go
package segmentio

import (
	"context"
	"time"

	"github.com/juju/errors"
	"github.com/segmentio/kafka-go"

	"github.com/kngu9/kafka-benchmark/clients"
)

// Client is the confluent-kafka-go client
type Client struct {
	clients.Config

	producer *kafka.Writer
}

// New creates a new confluent-kafka-go client
func New(cfg clients.Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	})

	return &Client{
		producer: producer,
		Config:   cfg,
	}, nil
}

// Name returns the name of the current library
func (c *Client) Name() string {
	return "segmentio-kafka-go"
}

// Produce is the implementation of producing in the segment kafka go client
func (c *Client) Produce(buff []byte) error {
	deliveryChan := make(chan error)
	go func() {
		deliveryChan <- c.producer.WriteMessages(context.Background(), kafka.Message{
			Value: buff,
		})
	}()

	select {
	case err := <-deliveryChan:
		if err != nil {
			return errors.Annotate(err, "segmentio-kafka-go event error")
		}
		return nil
	case <-time.After(c.Timeout):
		return errors.New("segmentio-kafka-go timed out")
	}
}

// Close terminates the connection
func (c *Client) Close() error {
	return errors.Trace(c.producer.Close())
}
