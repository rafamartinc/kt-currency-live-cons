FROM python:3

ADD ./currencylivecons /currencylivecons
ADD ./requirements.txt /

RUN pip install -r requirements.txt

CMD python -u -m currencylivecons.main --kafka_host ${KAFKA_HOST:-localhost} --kafka_port ${KAFKA_PORT:-9093} --kafka_topic ${KAFKA_TOPIC:-kt_currencies} --influx_host ${INFLUX_HOST:-localhost} --influx_port ${INFLUX_PORT:-8086} --influx_db ${INFLUX_DB:-live_currencies}
