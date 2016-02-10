package stockpile

import (
	"fmt"
	"strings"
	"time"

	kafkaavro "github.com/elodina/go-kafka-avro"
	"github.com/elodina/gonzo"
	"github.com/gocql/gocql"
)

const (
	CassandraRetryTimeout = 2 * time.Second
)

type CassandraProducer struct {
	stopChan chan struct{}
	cluster  string
	keyspace string
	decoder  *kafkaavro.KafkaAvroDecoder

	session    *gocql.Session
	insertions map[string]func(*gonzo.MessageAndMetadata) error
}

func NewCassandraProducer(cluster string, keyspace string, schema string) *CassandraProducer {
	cp := &CassandraProducer{
		stopChan: make(chan struct{}),
		cluster:  cluster,
		keyspace: keyspace,
		decoder:  kafkaavro.NewKafkaAvroDecoder(schema),
	}
	cp.insertions = make(map[string]func(*gonzo.MessageAndMetadata) error)

	return cp
}

func (cp *CassandraProducer) Start(messages <-chan *gonzo.MessageAndMetadata) error {
	nodes := strings.Split(cp.cluster, ",")
	cluster := gocql.NewCluster(nodes...)
	cluster.ProtoVersion = 4
	cluster.Keyspace = cp.keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	cp.session = session
	defer cp.session.Close()
	for {
		select {
		case message := <-messages:
			err = cp.insertMessage(message)
			if err != nil {
				return err
			}
		case <-cp.stopChan:
			Logger.Infof("Stopping Cassandra producer")
			return nil
		}
	}
}

func (cp *CassandraProducer) insertMessage(message *gonzo.MessageAndMetadata) error {
	fun, ok := cp.insertions[message.Topic]
	if !ok {
		return fmt.Errorf("Can't find insert function for %s", message.Topic)
	}
	for {
		err := fun(message)
		if err == nil {
			return nil
		}
		Logger.Errorf("Error produce to cassandra: %s", err)
		time.Sleep(CassandraRetryTimeout)
	}
}

func (cp *CassandraProducer) stop() {
	cp.stopChan <- struct{}{}
}
