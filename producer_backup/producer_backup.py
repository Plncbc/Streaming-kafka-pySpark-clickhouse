from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import logging
import time


KAFKA_BROKER = 'kafka-1:19092'
KAFKA_DATA_TOPIC = 'binance_agg_trade'
KAFKA_STATUS_TOPIC = 'main_producer_status'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_in_flight_requests_per_connection=1,
    acks='all',
    retries=10
)

consumer = KafkaConsumer(
    KAFKA_STATUS_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: v.decode('utf-8').strip('"')
)

is_backup_active = False
my_client = None
msg_time = time.time()


def message_handler(_, message):
    print(f"received message: {message} , {type(message)}")
    try:
        producer.send(KAFKA_DATA_TOPIC, value=message)
        producer.flush()
    except Exception as e:
        logging.error(f"Kafka producer error: {e}")

def start_backup():
    global my_client, is_backup_active
    if is_backup_active:
        return None
    my_client = UMFuturesWebsocketClient(on_message=message_handler)
    my_client.agg_trade(symbol="solusdt")
    my_client.agg_trade(symbol="ethusdt")
    my_client.agg_trade(symbol="bnbusdt")
    is_backup_active = True

def stop_backup():
    global my_client, is_backup_active
    if my_client:
        try:
            my_client.stop()
        except Exception as e:
            logging.error(f"Websocket error: {e}")
        my_client = None
    is_backup_active = False

while True:
    try:
        for msg in consumer:
            if msg.value == "active":
                msg_time = msg.timestamp / 1000
                print(msg.value, msg_time)
                if is_backup_active:
                    stop_backup()

        if time.time() - msg_time >= 10:
            start_backup()
        else:
            stop_backup()

    except Exception as e:
        logging.error(f"Kafka consumer error: {e}")
