package stockpile

import (
	"errors"

	kafka "github.com/stealthly/go_kafka_client"
	kafkamesos "github.com/stealthly/go_kafka_client/mesos/framework"
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

func (kc *KafkaConsumer) start(taskConfig kafkamesos.TaskConfig) (<-chan *kafka.Message, error) {
	consumerConfig := taskConfig["consumer.config"]
	config, err := kafka.ConsumerConfigFromFile(consumerConfig)
	if err != nil {
		return nil, err
	}
	config.Strategy = kc.messageCallback
	config.WorkerFailureCallback = failureCallback
	config.WorkerFailedAttemptCallback = failedAttemptCallback
	config.Coordinator, err = getZookeeper(consumerConfig)
	if err != nil {
		return nil, err
	}
	kc.consumer = kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	filter, err := getFilter(taskConfig)
	if err != nil {
		return nil, err
	}
	go kc.consumer.StartWildcard(filter, config.NumConsumerFetchers)
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

func getZookeeper(consumerConfig string) (*kafka.ZookeeperCoordinator, error) {
	zkConfig, err := kafka.ZookeeperConfigFromFile(consumerConfig)
	if err != nil {
		return nil, err
	}
	return kafka.NewZookeeperCoordinator(zkConfig), nil
}

func failureCallback(*kafka.WorkerManager) kafka.FailedDecision {
	Logger.Debug("WorkerFailureCallback")
	return kafka.CommitOffsetAndContinue
}

func failedAttemptCallback(*kafka.Task, kafka.WorkerResult) kafka.FailedDecision {
	Logger.Debug("WorkerFailedAttemptCallback")
	return kafka.CommitOffsetAndContinue
}

func getFilter(taskConfig kafkamesos.TaskConfig) (kafka.TopicFilter, error) {
	if whitelist, ok := taskConfig["whitelist"]; ok {
		return kafka.NewWhiteList(whitelist), nil
	}
	if blacklist, ok := taskConfig["blacklist"]; ok {
		return kafka.NewBlackList(blacklist), nil
	}
	return nil, errors.New("Whitelist or Blacklist should be present.")
}
