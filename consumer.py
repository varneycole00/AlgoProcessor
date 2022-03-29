from kafka import KafkaConsumer
import json

if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))
