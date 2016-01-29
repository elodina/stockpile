package stockpile

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	kafka "github.com/elodina/go_kafka_client"
	kafkamesos "github.com/elodina/go-kafka-client-mesos/framework"
	"time"
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
	nodes := strings.Split(taskConfig["cassandra.cluster"], ",")
	cluster := gocql.NewCluster(nodes...)
	cluster.Keyspace = taskConfig["cassandra.keyspace"]
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	insertQuery := fmt.Sprintf("INSERT INTO %s (partition, topic, key, value, offset, timeid, hour) VALUES (?, ?, ?, ?, ?, ?, ?)", taskConfig["cassandra.table"])
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
	curTime := time.Now()
	err := session.Query(query,
		message.Partition,
		message.Topic,
		message.Key,
		message.Value,
		message.Offset,
		gocql.UUIDFromTime(curTime),
		curTime.Format("2006-01-02 15") + ":00:00",
	).Exec()
	if err != nil {
		Logger.Errorf("Can't insert value to cassandra: %s", err.Error())
		panic(err)
	}
}

func (kc *CassandraProducer) stop() {
	kc.stopChan <- struct{}{}
}
