#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File description.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

from influxdb import InfluxDBClient
import time

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class InfluxConnection:

    def __init__(self, influx_host='localhost', influx_port=9092, influx_db='currencies'):

        self._influx_host = influx_host
        self._influx_port = influx_port
        self._influx_db = influx_db

        self._influx = None
        self._connect()

    def _connect(self):

        while self._influx is None:
            try:
                print('[INFO] Trying to connect to InfluxDB...')
                self._influx = InfluxDBClient(self._influx_host, self._influx_port)
                self._influx.create_database(self._influx_db)
                self._influx.create_retention_policy('policy', '30d', replication='1', database=self._influx_db, default=True)

            except Exception as ex:
                print('Exception while connecting InfluxDB, retrying in 1 second')
                print(str(ex))

                self._influx = None
                time.sleep(1)

            else:
                print('[INFO] Connected to InfluxDB.')

    def send(self, message):

        sent = False

        while not sent:
            try:
                sent = self._influx.write_points(message, database=self._influx_db, protocol='json')
            except Exception as ex:
                print('Exception while sending to InfluxDB')
                print(str(ex))
                self._influx = None

        return sent
