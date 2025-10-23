CREATE KEYSPACE IF NOT EXISTS price_data
WITH REPLICATION = {
  'class' : 'NetworkTopologyStrategy',
  'replication_factor' : 3
};

CREATE KEYSPACE IF NOT EXISTS price_vector
WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 3
};


CREATE KEYSPACE price_vector WITH replication = {'class': 'NetworkTopologyStrategy', 'dc348': '3'}  AND durable_writes = true;

CREATE KEYSPACE price_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
