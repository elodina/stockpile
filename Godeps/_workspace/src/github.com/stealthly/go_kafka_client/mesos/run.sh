export GM_API=http://10.1.13.244:6666
./cli add mirrormaker 0 --executor=executor
./cli update 0 --producer.config producer.config --consumer.config consumer.config --whitelist ".*"
./cli update 0 --mem 128 --cpu 0.1
./cli start 0
