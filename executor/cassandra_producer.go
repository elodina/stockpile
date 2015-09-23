package stockpile

type CassandraProducer struct{}

func NewCassandraProducer() *CassandraProducer {
	return &CassandraProducer{}
}

func (kc *CassandraProducer) start() {}
func (kc *CassandraProducer) stop()  {}
