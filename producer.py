import time
import urllib.error

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from blockOperations import generate_block_message, get_last_round

TOPIC_NAME = 'transactions'
KAFKA_SERVER = 'localhost:9092'


# Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')


# Kafka Producer Instantiation
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

if __name__ == '__main__':
    # Gets the last round the Algorand Network has processed
    last_round = get_last_round()  # get_last_round()
    # TODO: build catchup functionality in case of outage

    # Generates message to produce from the block data
    message = generate_block_message(last_round)

    # Send the message to the 'transactions' topic
    producer.send('transactions', message)

    while True:

        if last_round != get_last_round():  # If the last round recorded isn't the current last round, update and
            # process the next block
            last_round = get_last_round()
            print("____________________" + str(last_round) + "______________________")
            # Handling bad gateway error that occurs sometimes. The program will simply decrement the last round
            # variable and try again until it is able to go through
            try:
                message = generate_block_message(last_round)
                producer.send('transactions', message)
            except urllib.error.HTTPError:
                last_round = last_round - 1

