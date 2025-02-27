from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from kafka import KafkaProducer
import json
import time
import logging


KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'binance_agg_trade'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def message_handler(_, message):
    print(f"received message: {message} , {type(message)}")
    logging.info(message)
    producer.send(KAFKA_TOPIC, value=message, )
    #try:
    #    producer.send(KAFKA_TOPIC, value=message)
    #    producer.flush()
    #except Exception as e:
    #    logging.error(f"Kafka send error: {e}")

my_client = UMFuturesWebsocketClient(on_message=message_handler)
my_client.agg_trade(symbol="solusdt")

#time.sleep(5)
#logging.info("closing ws connection")
#my_client.stop()
#producer.close()
