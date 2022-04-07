import ast
import database_utils
from kafka import KafkaConsumer
import json

if __name__ == '__main__':
    # Kafka Consumer listening to the 'transactions' topic on localhost:9092 starting at latest offset
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest'
    )

    for message in consumer:
        print("________________________New Block___________________________")
        message_dict = ast.literal_eval(json.loads(message.value))
        current_block = message_dict['block']
        print(current_block)
        current_transaction_num = 0
        transactions = message_dict['transactions']
        # While there are still transactions left to process
        while str(current_transaction_num) in transactions:
            # Instantiate the next transaction to process
            current_transaction_dict = transactions[str(current_transaction_num)]

            # Pull info out of dictionary before use for readability
            sender = current_transaction_dict['sender']
            receiver = current_transaction_dict['receiver']
            fee = current_transaction_dict['fee']
            amount = current_transaction_dict['transaction amount']

            # Change balance of sender
            database_utils.remove_from_balance(current_transaction_dict["sender"], fee + amount)

            # Change Balance of receiver
            database_utils.add_to_balance(current_transaction_dict["receiver"], amount)

            current_transaction_num += 1

        database_utils.set_current_block_progress(current_block)
        print("updated last processed")
        print(database_utils.get_current_block_progress())
