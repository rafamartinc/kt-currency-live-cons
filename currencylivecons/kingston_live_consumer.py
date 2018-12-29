#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File description.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

from kafka import KafkaConsumer
import time
import json

__author__ = "Rubén Sainz, Rafael Martín-Cuevas"
__credits__ = ["Rubén Sainz", "Rafael Martín-Cuevas"]
__version__ = "0.1.0"
__status__ = "Development"

from .apis.influx import InfluxConnection


class KingstonLiveConsumer:

    def __init__(self, kafka_servers='localhost:9092', kafka_topic='kt_currencies',
                 influx_host='localhost', influx_port=9092, influx_db='currencies'):

        self._kafka_servers = kafka_servers
        self._kafka_topic = kafka_topic

        self._influx = InfluxConnection(influx_host, influx_port, influx_db)

        self._kafka = None
        self._connect()
        self._stream_data()

    def _connect(self):

        # Kafka.
        try:
            print('[INFO] Trying to connect to Kafka...')
            self._kafka = KafkaConsumer(self._kafka_topic, auto_offset_reset='earliest',
                                        bootstrap_servers=self._kafka_servers.split(','),
                                        api_version=(0, 9))#, group_id='live_consumers')
        except Exception as ex:
            print('Exception while connecting Kafka, retrying in 1 second')
            print(str(ex))

            self._kafka = None
            time.sleep(1)
        else:
            print('[INFO] Connected to Kafka.')

    def _stream_data(self):

        if self._kafka is not None:

            print('[INFO] Initializing...')

            for msg in self._kafka:

                msg = json.loads(msg.value.decode('utf-8'))
                print(msg)

                document = [
                    {
                        'measurement': 'live_points',
                        'time': msg['timestamp'],
                        'tags': {
                            'currency': msg['currency'],
                            'reference_currency': msg['reference_currency'],
                            'api': msg['api']
                        },
                        'fields': {
                            'value': msg['value'],
                        }
                    }
                ]

                if self._influx.send(document):
                    print(document)
