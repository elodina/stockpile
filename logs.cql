CREATE KEYSPACE IF NOT EXISTS syslog WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS syslog.logs (
  priority int,
  severity int,
  facility int,
  timestamp varchar,
  hostname varchar,
  tag varchar,
  pid varchar,
  message varchar,
  tags map<text, text>,
  PRIMARY KEY (hostname, timestamp, message),
);
