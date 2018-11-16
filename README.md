
# Kafka Benchmarking
This is a benchmark that provides a wireframe of testing out different Kafka producers

As of right now it benchmarks
* https://github.com/segmentio/kafka-go
* https://github.com/confluentinc/confluent-kafka-go
* https://github.com/Shopify/sarama


## Requirements
* Go >= 1.11
* pkg-config
* librdkafka
* Docker & docker-compose (you don't need this if you run Kafka and Zookeeper locally)

## Run
There are two options, you can either use Docker to spin up Kafka and Zookeeper and run it on your local machine. If you want to spin up the docker images, the repo comes with a docker compose.

    docker-compose up
    go test -bench=. -benchtime=10s
