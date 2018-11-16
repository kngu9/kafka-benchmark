package main_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/juju/errors"

	"github.com/kngu9/kafka-benchmark/clients"
	confluent "github.com/kngu9/kafka-benchmark/clients/confluent-kafka-go"
	shopify "github.com/kngu9/kafka-benchmark/clients/sarama-kafka-go"
	segmentio "github.com/kngu9/kafka-benchmark/clients/segmentio-kafka-go"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func randBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

var (
	brokers = []string{"127.0.0.1:9092"}
	topic   = "test"
	timeout = time.Second * 5
	size    = 1024
)

func newProducers() ([]clients.Client, error) {
	var producers []clients.Client

	confluentKafkaGo, err := confluent.New(
		clients.Config{
			Brokers: brokers,
			Topic:   topic,
			Timeout: timeout,
		},
	)
	if err != nil {
		return nil, errors.Annotate(err, "unable to create confluent-kafka-go client")
	}

	segmentKafkaGo, err := segmentio.New(
		clients.Config{
			Brokers: brokers,
			Topic:   topic,
			Timeout: timeout,
		},
	)
	if err != nil {
		return nil, errors.Annotate(err, "unable to create confluent-kafka-go client")
	}

	saramaKafkaGo, err := shopify.New(
		clients.Config{
			Brokers: brokers,
			Topic:   topic,
			Timeout: timeout,
		},
	)
	if err != nil {
		return nil, errors.Annotate(err, "unable to create confluent-kafka-go client")
	}

	producers = append(producers, confluentKafkaGo, segmentKafkaGo, saramaKafkaGo)

	return producers, nil
}

func BenchmarkProducers(b *testing.B) {
	producers, err := newProducers()
	if err != nil {
		b.Errorf("unable to set up kafka clients, error: %s", err)
	}

	for _, p := range producers {
		b.ResetTimer()

		b.Run(
			fmt.Sprintf("%s-%d bytes", p.Name(), size),
			func(pb *testing.B) {
				for n := 0; n < pb.N; n++ {
					pb.ReportAllocs()

					if err := p.Produce(randBytes(size)); err != nil {
						pb.Logf("error while producing, client: %s, error: %s", p.Name(), err)
					}
				}
			},
		)

		if err := p.Close(); err != nil {
			b.Logf("error while closing, client: %s, error: %s", b.Name(), err)
		}
	}
}
