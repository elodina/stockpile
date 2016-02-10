CREATE KEYSPACE test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE test;

CREATE TABLE stats (
  partition int,
  topic varchar,
  key varchar,
  value varchar,
  offset int,
  PRIMARY KEY (partition, topic, offset),
);
