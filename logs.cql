CREATE TABLE logs (
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
