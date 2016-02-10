package stockpile

import (
	"github.com/elodina/gonzo"
	"github.com/elodina/siesta"
)

type KafkaConsumer struct {
	consumer   gonzo.Consumer
	messages   chan *gonzo.MessageAndMetadata
	brokerList []string
	topics     []string
	partitions []int32
}

func NewKafkaConsumer(brokerList []string, topics []string, partitions []int32) *KafkaConsumer {
	return &KafkaConsumer{
		messages:   make(chan *gonzo.MessageAndMetadata),
		brokerList: brokerList,
		topics:     topics,
		partitions: partitions,
	}
}

func (kc *KafkaConsumer) Start() (<-chan *gonzo.MessageAndMetadata, error) {
	config := siesta.NewConnectorConfig()
	config.BrokerList = kc.brokerList

	client, err := siesta.NewDefaultConnector(config)
	if err != nil {
		return nil, err
	}

	consumerConfig := gonzo.NewConsumerConfig()
	kc.consumer = gonzo.NewConsumer(client, consumerConfig, kc.messageCallback)
	for _, partition := range kc.partitions {
		for _, topic := range kc.topics {
			kc.consumer.Add(topic, partition)
		}
	}
	return kc.messages, nil
}

func (kc *KafkaConsumer) messageCallback(data *gonzo.FetchData, _ *gonzo.KafkaPartitionConsumer) {
	if data.Error != nil {
		Logger.Errorf("Fetch error: %s", data.Error)
		return
	}
	Logger.Debugf("Received %d messages.", len(data.Messages))
	for _, msg := range data.Messages {
		kc.messages <- msg
	}
}

func (kc *KafkaConsumer) stop() {
	Logger.Debug("Stoping consumer...")
	kc.consumer.Stop()
	Logger.Debug("Consumer stopped")
}
