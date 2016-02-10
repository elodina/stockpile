#!/bin/sh

printf '\n>> get json from s3\n'
wget https://s3-us-west-2.amazonaws.com/stockpile-mesos/stockpile-marathon.json

printf '\n>> put json to marathon\n'
curl -X PUT -d@stockpile-marathon.json -H "Content-Type: application/json" http://leader.mesos.service.server:18080/v2/apps/stockpile

printf '\n>> sleep 1 minute to ensure DNS entries are created\n'
sleep 60

printf '\n>> export GM_API\n'
export GM_API=http://stockpile.service.server:6666

printf '\n>> get cli binary\n'
wget https://s3-us-west-2.amazonaws.com/stockpile-mesos/stockpile.tar.gz
tar -xzf stockpile.tar.gz

printf '\n>> chmod +x\n'
chmod +x cli

printf '\n>> add servers\n'
./cli add consumer 0 --executor stockpile
./cli update 0 --consumer.config consumer.config --whitelist ".*" --mem 128 --cpu 0.1 --options="cassandra.cluster=cassandra-node-0.service,cassandra-node-1.service,cassandra-node-2.service;cassandra.keyspace=test;cassandra.table=stats"

printf '\n>> start servers\n'
./cli start 0
