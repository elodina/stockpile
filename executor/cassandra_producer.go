package stockpile

import (
	"github.com/gocql/gocql"
	kafka "github.com/stealthly/go_kafka_client"
)

type CassandraProducer struct {
	stopChan chan struct{}
}

func NewCassandraProducer() *CassandraProducer {
	return &CassandraProducer{
		stopChan: make(chan struct{}),
	}
}

func (kc *CassandraProducer) start(cassandraConfig string, messages <-chan *kafka.Message) error {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
		return err
	}
	defer session.Close()
	for {
		select {
		case message := <-messages:
			insertValue(session, message)
		case <-kc.stopChan:
			return nil
		}
	}
}

func insertValue(session *gocql.Session, message *kafka.Message) {
	err := session.Query(`INSERT INTO stats (partition, topic, key, value, offset) VALUES (?, ?, ?, ?, ?)`,
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
