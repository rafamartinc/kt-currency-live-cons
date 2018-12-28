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

    def __init__(self, kafka_host='localhost', kafka_port=9092, kafka_topic='kt_currencies',
                 influx_host='localhost', influx_port=9092, influx_db='currencies'):

        self._kafka_host = kafka_host
        self._kafka_port = kafka_port
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
                                        bootstrap_servers=[str(self._kafka_host) + ':' + str(self._kafka_port)],
                                        api_version=(0, 9), group_id='live_consumers')
        except Exception as ex:
            print('Exception while connecting Kafka, retrying in 1 second')
            print(str(ex))

            self._producer = None
            time.sleep(1)
        else:
            print('[INFO] Connected to Kafka.')

    def _stream_data(self):

        if self._kafka is not None:

            for msg in self._kafka:

                msg = json.loads(msg.value.decode('utf-8'))

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

                print(document)

                self._influx.send(document)
