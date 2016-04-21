package stockpile

import (
	"fmt"
	"strings"
	"time"

	kafkaavro "github.com/elodina/go-kafka-avro"
	"github.com/elodina/gonzo"
	"github.com/gocql/gocql"
	"github.com/yanzay/log"
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
	cp.insertions["syslog"] = cp.insertSyslog

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
	log.Printf("Session created: %v", cp.session)
	defer cp.session.Close()
	for {
		select {
		case message := <-messages:
			err = cp.insertMessage(message)
			if err != nil {
				return err
			}
		case <-cp.stopChan:
			log.Infof("Stopping Cassandra producer")
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
		log.Errorf("Error produce to cassandra: %s", err)
		time.Sleep(CassandraRetryTimeout)
	}
}

func (cp *CassandraProducer) stop() {
	cp.stopChan <- struct{}{}
}

func (cp *CassandraProducer) insertSyslog(message *gonzo.MessageAndMetadata) error {
	mes := &SyslogMessage{}
	err := cp.decoder.DecodeSpecific(message.Value, mes)
	if err != nil {
		log.Error(err)
		return err
	}
	return cp.session.Query(`INSERT INTO logs (priority, severity, facility, timestamp, hostname, tag, pid, message, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		mes.Priority,
		mes.Severity,
		mes.Facility,
		mes.Timestamp,
		mes.Hostname,
		mes.Tag,
		mes.Pid,
		mes.Message,
		mes.Tags,
	).Exec()
}
