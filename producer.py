import time
import urllib.error

import algosdk.error
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

import database_utils
from block_operations import generate_block_message, get_last_round
from database_utils import get_current_block_progress, get_currently_processing

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


def initialize_producer():
    # # Gets the last round the Algorand Network has processed
    # network_current_round = get_last_round()  # get_last_round()
    #
    # # Getting the last block sent and processed by the system
    #
    # print("Last processed: " + str(last_processed))
    #
    # # print("____________________" + str(last_processed + 1) + "______________________")
    #
    # # Generates message to produce from the block data starting at the next block after the highest processed
    # message = generate_block_message(last_processed + 1)
    # print(message)
    #
    # # Send the message to the 'transactions' topic
    # producer.send('transactions', message)

    # Getting current processing state before producing
    last_processed = get_current_block_progress()
    last_sent = last_processed
    while True:
        # print("last processed: " + str(last_processed) + ", last sent: " + str(last_sent))
        last_processed = get_current_block_progress()

        if last_processed + 1 <= get_last_round():

            if last_processed != get_last_round() and last_processed == last_sent:
                try:
                    message = generate_block_message(last_processed + 1)
                    producer.send('transactions', message)
                    last_sent = last_processed + 1

                    print("last processed: " + str(last_processed) + ", last sent: " + str(last_sent) +
                          ", Network current block status: " + str(get_last_round()))
                except urllib.error.HTTPError and algosdk.error.AlgodHTTPError:
                    # Continuing here will send the same block that previously failed due to api gateway issues
                    # Simply will keep trying until there is no more http 504 error
                    continue


if __name__ == '__main__':
    initialize_producer()
