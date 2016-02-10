# stockpile

**stockpile** is a mesos executor for go_kafka_client scheduler that mirros data from Apache Kafka to Apache Cassandra

## Build
```bash
$ git clone git@github.com:elodina/stockpile.git
$ cd stockpile
$ export GO15VENDOREXPERIMENT=1
$ go build
```

## Prepare Cassandra

Create table with schema for cassandra producer.

## Run

To run stockpile executor, copy it's binary to go-kafka-client-mesos directory, add new consumer task using cli and start it.

Options:
```
  brokers
        Kafka broker list (default "localhost:9092")
  cassandra
        Cassandra cluster (default "localhost:9042")
  keyspace
        Cassandra keyspace (required)
  partitions
        Kafka partitions list (required)
  schema
        Schema registry URL (required)
  topics
        Kafka topic list (required)
```

Example:

```bash
$ ./cli add consumer 1 --executor=stockpile
$ ./cli update 1 --mem 64 --cpu 0.1
$ ./cli update 1 --options="topics=test;partitions=0;keyspace=test;schema=localhost:8081"
$ ./cli start 1
```
