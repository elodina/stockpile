package stockpile

import (
	"fmt"
)

type App struct {
	consumer *KafkaConsumer
	producer *CassandraProducer
}

func NewApp(consumer *KafkaConsumer, producer *CassandraProducer) *App {
	return &App{
		consumer: consumer,
		producer: producer,
	}
}

func (a *App) Start() error {
	messages, err := a.consumer.Start()
	if err != nil {
		return fmt.Errorf("Can't start consumer. Error: %s", err)
	}
	err = a.producer.Start(messages)
	if err != nil {
		return fmt.Errorf("Can't start producer. Error: %s", err)
	}
	return nil
}

func (a *App) Stop() error {
	a.consumer.stop()
	a.producer.stop()
	return nil
}
