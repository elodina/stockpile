package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	// kafkamesos "github.com/elodina/go-kafka-client-mesos/framework"
	stockpile "github.com/elodina/stockpile/executor"
	"github.com/mesos/mesos-go/executor"
	"github.com/yanzay/log"
)

var (
	// Kafka
	brokers    = flag.String("brokers", "localhost:9092", "Kafka broker list")
	topics     = flag.String("topics", "", "Kafka topic list")
	partitions = flag.String("partitions", "", "Kafka partitions list")

	// Cassandra
	cassandra    = flag.String("cassandra", "localhost:9042", "Cassandra cluster")
	keyspace     = flag.String("keyspace", "", "Cassandra keyspace")
	cqlVersion   = flag.String("cql-version", "3.2.1", "Cassandra source cluster CQL version, defaults to 3.2.1")
	protoVersion = flag.Int("proto-version", 3, "Cassandra source cluster proto version, defaults to 3")

	// Schema registry
	schema = flag.String("schema", "", "Schema registry URL")

	// Launching as a mesos executor
	executorType = flag.String("type", "", "Executor type")

	logLevel = flag.String("log.level", "debug", "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
)

func main() {
	flag.Parse()

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
	producer := stockpile.NewCassandraProducer(*cassandra, *keyspace, *schema, *cqlVersion, *protoVersion)
	app := stockpile.NewApp(consumer, producer)

	// if *executorType == kafkamesos.TaskTypeConsumer {
	// runExecutor(app)
	// } else {
	runService(app)
	// }
}

func runExecutor(app *stockpile.App) {
	taskExecutor := stockpile.NewExecutor(app)
	driverConfig := executor.DriverConfig{
		Executor: taskExecutor,
	}
	driver, err := executor.NewMesosExecutorDriver(driverConfig)
	if err != nil {
		log.Error(err)
		panic(err)
	}
	_, err = driver.Start()
	if err != nil {
		log.Error(err)
		panic(err)
	}
	driver.Run()
}

func runService(app *stockpile.App) {
	err := app.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
