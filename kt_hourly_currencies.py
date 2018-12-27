# -*- coding: utf-8 -*-
import argparse
from kafka import KafkaConsumer
from influxdb import InfluxDBClient

def main(host, port, dbname):

    consumer_hour = KafkaConsumer(kt_hourly_currencies, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer_hour:
        json_body_AggregatedPoints = [
                {"measurement": "AggregatedPoints",
                'time': consumer_hour['timestamp'],
                "tags":{
                    'currency': consumer_hour['currency'],
                    'reference_currency': consumer_hour['reference_currency'],
                    'API': consumer_hour['api']
                },
                "fields":{
                    'value': consumer_hour['value'],
                    }
                }
        ]
        client = InfluxDBClient(host, port)
        print("Create database: " + dbname)
        client.create_database(dbname)
        print("Create a retention policy")
        client.create_retention_policy('awesome_policy', '30d', replication='1', database=dbname, default=True)
        client.write_points(json_body_AggregatedPoints, database=dbname, protocol='json')

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(description='example code to play with InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    parser.add_argument('--dbname', type=str, required=False, default='currency',
                        help='dbname of InfluxDB')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    print(args.host, args.port, args.dbname)
    main(host=args.host, port=args.port, dbname=args.dbname)


