from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from kafka import KafkaProducer
import json
import logging


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

def message_handler(_, message):
    print(f"received message: {message} , {type(message)}")
    try:
        producer.send(KAFKA_DATA_TOPIC, value=message)
        producer.send(KAFKA_STATUS_TOPIC, value='active')
        producer.flush()
    except Exception as e:
        logging.error(f"Kafka & producer send error: {e}")
        

my_client = UMFuturesWebsocketClient(on_message=message_handler)
my_client.agg_trade(symbol="solusdt")
my_client.agg_trade(symbol="ethusdt")
my_client.agg_trade(symbol="bnbusdt")

