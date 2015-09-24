package stockpile

import (
	"fmt"

	"github.com/gocql/gocql"
	kafka "github.com/stealthly/go_kafka_client"
	kafkamesos "github.com/stealthly/go_kafka_client/mesos/framework"
)

type CassandraProducer struct {
	stopChan chan struct{}
}

func NewCassandraProducer() *CassandraProducer {
	return &CassandraProducer{
		stopChan: make(chan struct{}),
	}
}

func (kc *CassandraProducer) start(taskConfig kafkamesos.TaskConfig, messages <-chan *kafka.Message) error {
	cluster := gocql.NewCluster(taskConfig["cassandra.cluster"])
	cluster.Keyspace = taskConfig["cassandra.keyspace"]
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
		return err
	}
	defer session.Close()
	insertQuery := fmt.Sprintf("INSERT INTO %s (partition, topic, key, value, offset) VALUES (?, ?, ?, ?, ?)", taskConfig["cassandra.table"])
	for {
		select {
		case message := <-messages:
			insertValue(insertQuery, session, message)
		case <-kc.stopChan:
			return nil
		}
	}
}

func insertValue(query string, session *gocql.Session, message *kafka.Message) {
	err := session.Query(query,
		message.Partition,
		message.Topic,
		string(message.Key),
		string(message.Value),
		message.Offset,
	).Exec()
	if err != nil {
		Logger.Errorf("Can't insert value to cassandra: %s", err.Error())
		panic(err)
	}
}

func (kc *CassandraProducer) stop() {
	kc.stopChan <- struct{}{}
}
