CREATE TABLE stats (
  partition int,
  topic varchar,
  key varchar,
  value varchar,
  offset int,
  PRIMARY KEY (partition, topic, offset),
);
