import urllib.error
import algosdk.error
import json
from kafka import KafkaProducer
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


def start_producer():
    # Getting current processing state before producing
    last_processed = get_current_block_progress()
    # Upon producer startup, the last sent is the last block processed before previous system shutdown
    last_sent = last_processed
    while True:
        # Check this every iteration to always have the current processing progress
        last_processed = get_current_block_progress()

        # If the system has not caught up to the algorand network
        if last_processed + 1 <= get_last_round():
            # If we haven't processed the latest block and the last processed by the consumer is also
            # the last block sent to the consumer then we know to send a new block
            if last_processed != get_last_round() and last_processed == last_sent:

                # Try generating and sending the block message
                try:
                    message = generate_block_message(last_processed + 1)
                    producer.send('transactions', message)
                    last_sent = last_processed + 1

                    print("last processed: " + str(last_processed) + ", last sent: " + str(last_sent) +
                          ", Network current block status: " + str(get_last_round()))
                # Excepting API errors
                except urllib.error.HTTPError and algosdk.error.AlgodHTTPError:
                    # Continuing here will send the same block that previously failed due to api gateway issues
                    # Simply will keep trying until there is no http 504 error
                    continue


if __name__ == '__main__':
    start_producer()
