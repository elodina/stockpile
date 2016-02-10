export GM_API=http://10.1.13.244:6666
cp ../../../elodina/stockpile/stockpile .
./cli add consumer 1 --executor=stockpile
./cli update 1 --consumer.config consumer.config --whitelist ".*" --mem 512 --cpu 0.5
./cli update 1 --options="cassandra.cluster=127.0.0.1;cassandra.keyspace=test;cassandra.table=stats"
./cli start 1
