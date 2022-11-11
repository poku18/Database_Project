from InfluxDB import connect,load,drop,query1,query2,query3
import unittest
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions


class TestInfluxDB(unittest.TestCase):
    def test_connect(self):
        """
        Test that connect is successful
        """
        client=connect()
        health = client.ping()
       
        self.assertEqual(health, True)
    
    def test_drop(self):
        "Test to check if drop() is successful"
        client=connect()
        drop()
        query='from(bucket:"cosc516")' \
                ' |> range(start: 0, stop: now())' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> count()'
        result = client.query_api().query(query=query)
        self.assertEqual(result,[])

    def test_load(self):
        "Test to check if load() is successful"
        client=connect()
        load()
        query='from(bucket:"cosc516")' \
                ' |> range(start: 0, stop: now())' \
                ' |> filter(fn: (r) => r._measurement == "financial-analysis")' \
                ' |> count()'
        result = client.query_api().query(query=query)
        count={}
        for table in result:
            for record in table.records:
                count[str(record.get_field())]=str(round(record.get_value(),3))
        self.assertEqual(count,{'low': '3952', 'high': '3952', 'close': '3952', 'open': '3952'})

if __name__ == '__main__':
    unittest.main()
