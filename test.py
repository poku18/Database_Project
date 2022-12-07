from InfluxDB import connect,load,drop,query1,query2,query3,result_process,query0,query4
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

    def test_query0(self):
        client=connect()
        final_string=result_process(query0())
        expected=['2018-01-04,open,9.01',
                    '2018-01-05,open,9.1',
                    '2017-10-06,open,9.23',
                    '2017-07-26,open,9.25',
                    '2017-10-03,open,9.3']
        self.assertCountEqual(final_string,expected)

    def test_query1(self):
        "Test to check if query1 is successful and as expected"
        client=connect()
        final_string=result_process(query1())
        expected=['2008-10-24,high,89.53', '2008-11-21,open,80.74', '2008-11-20,close,80.86', '2008-11-20,low,72.76']
        
        self.assertCountEqual(final_string,expected)

    def test_query2(self):
        client=connect()
        final_string=result_process(query2())
        expected=['2007-01-03,high,12.75', 
                    '2007-01-04,high,12.42', 
                    '2007-01-05,high,12.25', 
                    '2006-12-26,high,12.03', 
                    '2006-12-29,high,11.65', 
                    '2006-12-27,high,11.12', 
                    '2006-12-28,high,11.06']
        self.assertCountEqual(final_string,expected)

    def test_query3(self):
        "Test to check if query3 is successful and as expected"
        client=connect()
        final_string=result_process(query3())     
        expected=['2006-02-01,low,11.708', 
                    '2006-03-01,low,12.165', 
                    '2006-04-01,low,11.334', 
                    '2006-05-01,low,11.54', 
                    '2006-06-01,low,13.634', 
                    '2006-07-01,low,16.024', 
                    '2006-08-01,low,14.652', 
                    '2006-09-01,low,13.084', 
                    '2006-10-01,low,11.912', 
                    '2006-11-01,low,11.128', 
                    '2006-12-01,low,10.555', 
                    '2006-12-31,low,10.603']   
        self.assertCountEqual(final_string,expected)

    def test_query4(self):
        client=connect()
        result=query4()
        count={}
        for table in result:
            for record in table.records:
                count[str(record.get_field())]=str(round(record.get_value(),3))
        self.assertEqual(count,{'low': '3952', 'high': '3952', 'close': '3952', 'open': '3952'})
        
    
if __name__ == '__main__':
    unittest.main()
