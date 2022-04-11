import ast
import sys

import database_utils
from kafka import KafkaConsumer
import json


def handle_message(message):
    message_dict = ast.literal_eval(json.loads(message.value))
    # message_dict = json.loads(message.value)
    current_block = message_dict['block']
    current_transaction_num = 0
    transactions = message_dict['transactions']

    if str(current_block) <= str(database_utils.get_current_block_progress()):
        return

    # While there are still transactions left to process
    while str(current_transaction_num) in transactions:
        # Instantiate the next transaction to process
        current_transaction_dict = transactions[str(current_transaction_num)]

        # Pull info out of dictionary before use for readability
        sender = current_transaction_dict['sender']
        receiver = current_transaction_dict['receiver']
        fee = current_transaction_dict['fee']
        amount = current_transaction_dict['transaction amount']

        database_utils.handle_transaction(sender, receiver, amount, fee)

        current_transaction_num += 1

    database_utils.set_current_block_progress(current_block)
    print("Processed block " + current_block)

    # print(database_utils.get_current_block_progress())


def start_consumer():
    # Kafka Consumer listening to the 'transactions' topic on localhost:9092 starting at latest offset
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest')

    # Iterating through received messages and processing them with handle_message()
    for message in consumer:
        handle_message(message)


if __name__ == '__main__':
    start_consumer()
