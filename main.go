package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	stockpile "github.com/elodina/stockpile/executor"
)

var (
	// Kafka
	brokers    = flag.String("brokers", "localhost:9092", "Kafka broker list")
	topics     = flag.String("topics", "", "Kafka topic list")
	partitions = flag.String("partitions", "", "Kafka partitions list")

	// Cassandra
	cassandra = flag.String("cassandra", "localhost:9042", "Cassandra cluster")
	keyspace  = flag.String("keyspace", "", "Cassandra keyspace")

	// Schema registry
	schema = flag.String("schema", "", "Schema registry URL")

	logLevel = flag.String("log-level", "debug", "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
)

func main() {
	flag.Parse()
	err := stockpile.InitLogging(*logLevel)
	if err != nil {
		fmt.Printf("Logging init failed: %s", err.Error())
	}

	// Validate flags
	if *topics == "" || *partitions == "" || *keyspace == "" || *schema == "" {
		fmt.Println("'topics', 'partitions', 'keyspace' and 'schema' parameters required.")
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	topicList := strings.Split(*topics, ",")
	partitionStrings := strings.Split(*partitions, ",")
	var partitionList []int32
	for _, partitionStr := range partitionStrings {
		partition, err := strconv.Atoi(partitionStr)
		if err != nil {
			fmt.Printf("Can't parse partition list: %s\n", err)
			os.Exit(1)
		}
		partitionList = append(partitionList, int32(partition))
	}

	consumer := stockpile.NewKafkaConsumer(brokerList, topicList, partitionList)
	producer := stockpile.NewCassandraProducer(*cassandra, *keyspace, *schema)
	messages, err := consumer.Start()
	if err != nil {
		panic(fmt.Sprintf("Can't start consumer. Error: %s", err))
	}
	err = producer.Start(messages)
	if err != nil {
		panic(err)
	}
}
