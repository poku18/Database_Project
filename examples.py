import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("INFLUXDB_TOKEN")
org = "pb.bhandari18@gmail.com"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

#write data
bucket="cosc516"

start = "2022-11-01T00:00:00Z"
stop = "2022-11-09T00:00:00Z"
delete_api = client.delete_api()
delete_api.delete(start, stop, '_measurement="measurement1"', bucket, org)

write_api = client.write_api(write_options=SYNCHRONOUS)


for value in range(3):
  point = (
    Point("measurement1")
    .tag("tagname1", "tagvalue1")
    .field("field1", value)
  )
  write_api.write(bucket=bucket, org="pb.bhandari18@gmail.com", record=point)
  time.sleep(1) # separate points by 1 second

#query data
query_api = client.query_api()

query = """from(bucket: "cosc516")
 |> range(start: -10m)
 |> filter(fn: (r) => r._measurement == "measurement1")"""
tables = query_api.query(query, org="pb.bhandari18@gmail.com")

for table in tables:
  for record in table.records:
    print(record)


#Execute an aggregating query
query_api = client.query_api()

query = """from(bucket: "cosc516")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "measurement1")
  |> mean()"""
tables = query_api.query(query, org="pb.bhandari18@gmail.com")

for table in tables:
    for record in table.records:
        print(record)