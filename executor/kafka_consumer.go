package stockpile

type KafkaConsumer struct{}

func NewKafkaConsumer() *KafkaConsumer {
	return &KafkaConsumer{}
}

func (kc *KafkaConsumer) start() {}
func (kc *KafkaConsumer) stop()  {}
