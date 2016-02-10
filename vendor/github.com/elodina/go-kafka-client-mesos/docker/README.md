Go Kafka Client on Mesos using Docker
====================================

```
# cd $GOPATH/src/github.com/elodina/go-kafka-client-mesos
# sudo docker build -t elodina/go-kafka-client-mesos --file Dockerfile.mesos .
# docker run --net=host -i -t elodina/go-kafka-client-mesos ./cli scheduler --master 192.168.3.5:5050 --log.level debug --api http://192.168.3.1:6666
```