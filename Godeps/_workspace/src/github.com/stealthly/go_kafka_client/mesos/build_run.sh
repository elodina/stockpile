go install github.com/stealthly/go_kafka_client/mesos/framework && \
go build cli.go && \
go build executor.go && \
./cli scheduler --master 10.1.13.244:5050 --api http://10.1.13.244:6666
