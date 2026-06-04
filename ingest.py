# ingest.py
import json
import time
import random
from kafka import KafkaProducer
import click

# A list of mock financial news headlines
SAMPLE_MESSAGES = [
    "BIGCORP INC reports record profits for Q3, shares surge.",
    "REGULATOR announces investigation into SMALLCO's accounting practices.",
    "Market sentiment is bearish on tech stocks following interest rate hikes.",
    "NEWTECH launches revolutionary product, analysts are optimistic.",
    "OLDGUARD CORP misses earnings estimates, stock price plummets.",
]

def create_producer(bootstrap_servers: str):
    """Creates a Kafka producer, configured for JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

@click.command()
@click.option('--topic', default='raw_text_stream', help='The Kafka topic to stream data to.')
@click.option('--broker', default='localhost:9092', help='The Kafka broker address.')
def stream_data(topic: str, broker: str):
    """
    A CLI process to stream raw text data into a Kafka topic.
    This simulates a live data feed.
    """
    producer = create_producer(broker)
    print(f"✅ Starting to stream data to topic '{topic}' at {broker}...")
    while True:
        message = {
            'timestamp': time.time(),
            'text': random.choice(SAMPLE_MESSAGES)
        }
        producer.send(topic, value=message)
        print(f"Sent: {message['text']}")
        time.sleep(random.uniform(1, 4)) # Simulate irregular data arrival

if __name__ == '__main__':
    stream_data()
