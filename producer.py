import time
import urllib.error

import algosdk.error
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from block_operations import generate_block_message, get_last_round
from database_utils import get_current_block_progress

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
    network_current_round = get_last_round()  # get_last_round()

    # Getting the last block sent and processed by the system
    last_processed = get_current_block_progress()  # TODO: CHANGE THIS BACK TO THE DB VALUE

    print("Last processed: " + str(last_processed))

    print("____________________" + str(last_processed + 1) + "______________________")

    # Generates message to produce from the block data starting at the next block after the highest processed
    message = generate_block_message(last_processed + 1)
    print(message)

    # Send the message to the 'transactions' topic
    producer.send('transactions', message)
    last_sent = last_processed + 1
    last_processed = get_current_block_progress()

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


    # if last_processed != network_current_round:
    #     # TODO: build catchup functionality in case of outage
    #
    # else:
    #     # Generates message to produce from the block data
    #     message = generate_block_message(network_current_round)
    #
    #     # Send the message to the 'transactions' topic
    #     producer.send('transactions', message)
    #
    # while True:
    #
    #     if network_current_round != get_last_round():  # If the last round recorded isn't the current last round, update and
    #         # process the next block
    #         network_current_round = get_last_round()
    #         print("____________________" + str(network_current_round) + "______________________")
    #         # Handling bad gateway error that occurs sometimes. The program will simply decrement the last round
    #         # variable and try again until it is able to go through
    #         try:
    #             message = generate_block_message(network_current_round)
    #             producer.send('transactions', message)
    #         except urllib.error.HTTPError:
    #             network_current_round = network_current_round - 1
    #
