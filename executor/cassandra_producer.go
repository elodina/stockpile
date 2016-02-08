package stockpile

import (
	"fmt"
	"strings"

	kafkaavro "github.com/elodina/go-kafka-avro"
	"github.com/elodina/gonzo"
	"github.com/elodina/ulysses/avro"
	"github.com/gocql/gocql"
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
	cp.insertions = map[string]func(*gonzo.MessageAndMetadata) error{
		"cassandra_transform_user_fact_store": cp.insertUserFact,
	}

	return cp
}

func (cp *CassandraProducer) Start(messages <-chan *gonzo.MessageAndMetadata) error {
	nodes := strings.Split(cp.cluster, ",")
	cluster := gocql.NewCluster(nodes...)
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
	return fun(message)
}

func (cp *CassandraProducer) insertUserFact(message *gonzo.MessageAndMetadata) error {
	fact := &avro.UserFact{}
	err := cp.decoder.DecodeSpecific(message.Value, fact)
	if err != nil {
		return err
	}
	query := "INSERT INTO user_fact_store (userid, type, factdata) VALUES (?, ?, ?)"
	return cp.session.Query(query, fact.UserID, fact.Type, fact.Factdata).Exec()
}

func (cp *CassandraProducer) stop() {
	cp.stopChan <- struct{}{}
}
