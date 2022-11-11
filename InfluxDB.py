import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from collections import OrderedDict
from csv import DictReader
import reactivex as rx
from reactivex import operators as ops

token = os.environ.get("INFLUXDB_TOKEN")
org = "pb.bhandari18@gmail.com"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"
bucket="cosc516"

def connect():
    global client
    client=InfluxDBClient(url=url, token=token, org=org, debug=False)
    health = client.ping()
    print(health)
    return client

def parse_row(row: OrderedDict):
    """Parse row of CSV file into Point with structure:
        financial-analysis,type=vix-daily close=18.47,high=19.82,low=18.28,open=19.82 1198195200000000000
    CSV format:
        Date,VIX Open,VIX High,VIX Low,VIX Close\n
        2004-01-02,17.96,18.68,17.54,18.22\n
        2004-01-05,18.45,18.49,17.44,17.49\n
        2004-01-06,17.66,17.67,16.19,16.73\n
        2004-01-07,16.72,16.75,15.5,15.5\n
        2004-01-08,15.42,15.68,15.32,15.61\n
        2004-01-09,16.15,16.88,15.57,16.75\n
        ...
    :param row: the row of CSV file
    :return: Parsed csv row to [Point]
    """
    return Point("financial-analysis") \
        .tag("type", "vix-daily") \
        .field("open", float(row['VIX Open'])) \
        .field("high", float(row['VIX High'])) \
        .field("low", float(row['VIX Low'])) \
        .field("close", float(row['VIX Close'])) \
        .time(row['Date'])

def drop():
    delete_api=client.delete_api()
    delete_api.delete("1970-01-01T00:00:00Z", "2022-11-10T22:39:12.107968366Z", '_measurement="financial-analysis"', bucket, org)

def load():
    data = rx \
            .from_iterable(DictReader(open('data/vix-daily.csv', 'r'))) \
            .pipe(ops.map(lambda row: parse_row(row)))

    """
    Create client that writes data in batches with 50_000 items.
    """
    with client.write_api(write_options=WriteOptions(batch_size=50_000, flush_interval=10_000)) as write_api:
        """
        Write data into InfluxDB
        """
        write_api.write(bucket="cosc516", record=data)

#Query to give out the maximum high, open, close and low values from the whole data.
def query1():
    query = 'from(bucket:"cosc516")' \
                ' |> range(start: 0, stop: now())' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> max()'
    result = client.query_api().query(query=query)
    return result

def query2():
    query= 'from(bucket:"cosc516")' \
                ' |> range(start: 2006-12-26, stop: 2007-01-08)' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> filter(fn: (r) => r._field == "high")'\
                ' |> sort(columns:["_value"],desc:true)'
    result = client.query_api().query(query=query) 
    return result

def query3():
    query= 'from(bucket:"cosc516")' \
                ' |> range(start: 2006-01-01, stop: 2006-12-31)' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> filter(fn: (r) => r._field == "low")'\
                ' |> aggregateWindow(every:1mo ,fn: mean, createEmpty: true)'
    result = client.query_api().query(query=query)
    return result

def query4():
    query='from(bucket:"cosc516")' \
                ' |> range(start: 0, stop: now())' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> count()'
    result = client.query_api().query(query=query)
    return result

def result_process(query):
    """
    Processing results
    """
    print("=== results ===")
    print()
    for table in query:
        for record in table.records:
            print('{0},{1},{2}'.format(record.get_time().date(),record.get_field(), round(record.get_value(),3)))
    print()

client=connect()
drop()

load()
print("Maximum Values")
result_process(query1())

print("VIX High only")
result_process(query2())

print("Mean of low values per month")
result_process(query3())

print("Count=========")
true={}
for table in query4():
    for record in table.records:
        true[str(record.get_field())]=str(round(record.get_value(),3))
print(true)
