# stockpile

**stockpile** is a mesos executor for go_kafka_client scheduler.

## Build
```bash
$ git clone git@github.com:elodina/stockpile.git
$ cd stockpile
$ godep restore
$ go build
```

## Prepare Cassandra

Create table with schema, described in stats.cql.

## Run

To run stockpile executor, copy it's binary to go_kafka_client scheduler directory, add new consumer task using cli and start it.

Example:

```bash
$ ./cli add consumer 1 --executor=stockpile
$ ./cli update 1 --consumer.config consumer.config --whitelist ".*" --mem 512 --cpu 0.5
$ ./cli update 1 --options="cassandra.cluster=127.0.0.1;cassandra.keyspace=test;cassandra.table=stats"
$ ./cli start 1
```
