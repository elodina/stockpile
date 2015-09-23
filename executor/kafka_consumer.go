package stockpile

import (
	"runtime"
	"time"

	kafka "github.com/stealthly/go_kafka_client"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	messages chan *kafka.Message
}

func NewKafkaConsumer() *KafkaConsumer {
	return &KafkaConsumer{
		messages: make(chan *kafka.Message, 100),
	}
}

func (kc *KafkaConsumer) start(consumerConfig string) (<-chan *kafka.Message, error) {
	Logger.Debugf("Starting KafkaConsumer with config: %s", consumerConfig)
	config, err := kafka.ConsumerConfigFromFile(consumerConfig)
	if err != nil {
		return nil, err
	}
	Logger.Debugf("Low level client created")
	config.Strategy = kc.messageCallback
	config.WorkerFailureCallback = func(*kafka.WorkerManager) kafka.FailedDecision {
		Logger.Debug("WorkerFailureCallback")
		return kafka.CommitOffsetAndContinue
	}
	config.WorkerFailedAttemptCallback = func(*kafka.Task, kafka.WorkerResult) kafka.FailedDecision {
		Logger.Debug("WorkerFailedAttemptCallback")
		return kafka.CommitOffsetAndContinue
	}
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{"localhost:2181"}
	zkConfig.MaxRequestRetries = 10
	zkConfig.ZookeeperTimeout = 30 * time.Second
	zkConfig.RequestBackoff = 3 * time.Second
	config.Coordinator = kafka.NewZookeeperCoordinator(zkConfig)
	kc.consumer = kafka.NewConsumer(config)
	Logger.Debug("Starting consumer...")
	go kc.consumer.StartWildcard(kafka.NewWhiteList("stats"), runtime.NumCPU())
	Logger.Debug("Consumer started")
	return kc.messages, nil
}

func (kc *KafkaConsumer) messageCallback(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
	Logger.Debugf("Value received: %s", string(msg.Value))
	kc.messages <- msg
	return kafka.NewSuccessfulResult(id)
}

func (kc *KafkaConsumer) stop() {
	Logger.Debug("Stoping consumer...")
	kc.consumer.Close()
	Logger.Debug("Consumer stopped")
}
