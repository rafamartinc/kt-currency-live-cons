#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File description.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

import argparse

__author__ = "Rubén Sainz, Rafael Martín-Cuevas"
__credits__ = ["Rubén Sainz", "Rafael Martín-Cuevas"]
__version__ = "0.1.0"
__status__ = "Development"

from .kingston_live_consumer import KingstonLiveConsumer

def main():

    parser = argparse.ArgumentParser(description='KafkaConsumer that streams live data to InfluxDB')

    parser.add_argument('-k', '--kafka_servers',
                        default='localhost:9092',
                        type=str,
                        help='Kafka servers, separated by colons (default: localhost:9092)')
    parser.add_argument('-t', '--kafka_topic',
                        default='kt_currencies',
                        type=str,
                        help='Kafka topic to retrieve data from (default: kt_currencies)')
    parser.add_argument('-i', '--influx_host',
                        default='localhost',
                        type=str,
                        help='Hostname of InfluxDB HTTP API (default: localhost)')
    parser.add_argument('-q', '--influx_port',
                        default=8086,
                        type=int,
                        help='Port of InfluxDB HTTP API (default: 8086)')
    parser.add_argument('-d', '--influx_db',
                        default='live_currencies',
                        type=str,
                        help='Name of database within InfluxDB (default: currencies)')

    args = parser.parse_args()
    print(str(args.kafka_servers.split(',')))

    KingstonLiveConsumer(kafka_servers=args.kafka_servers, kafka_topic=args.kafka_topic,
                         influx_host=args.influx_host, influx_port=args.influx_port, influx_db=args.influx_db)


if __name__ == '__main__':

    main()
