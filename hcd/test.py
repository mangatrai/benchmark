from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os

load_dotenv()

data_db_host = os.getenv('CASSANDRA_DATA_NODE_IP', '127.0.0.1')
data_db_user = os.getenv('CASSANDRA_DATA_USER', 'cassandra')
data_db_password = os.getenv('CASSANDRA_DATA_PASSWORD', 'cassandra')
data_db_keyspace = os.getenv('CASSANDRA_DATA_KS', 'price_data')
data_db_table = os.getenv('DATA_TABLE', 'price_data_1')

print("**********", data_db_host, data_db_user, data_db_password, data_db_keyspace, data_db_table, "**********")

auth_provider = PlainTextAuthProvider(data_db_user, data_db_password)
cluster = Cluster([data_db_host], auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")