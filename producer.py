import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from blockOperations import generate_block_message, get_last_round

TOPIC_NAME = 'transactions'
KAFKA_SERVER = 'localhost:9092'


# Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':
    last_round = get_last_round()
    print(last_round)
    message = generate_block_message(last_round)
    producer.send('transactions', message)
    while True:
        if last_round != get_last_round():
            print("____________________" + str(last_round) + "______________________")
            last_round = get_last_round()
            generate_block_message(last_round)
            producer.send('transactions', message)
