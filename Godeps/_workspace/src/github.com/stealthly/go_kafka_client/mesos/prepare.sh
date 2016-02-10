export KAFKA_PATH=/Users/alexeygrachov/projects/cogniance/kafka_2.10-0.8.2.1

$KAFKA_PATH/bin/zookeeper-server-start.sh $KAFKA_PATH/config/zookeeper.properties 1>zk.out 2>zk.err &
$KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/server.properties 1>kafka.out 2>kafka.err &
