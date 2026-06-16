# ingest.py
import json
import time
import websocket
from kafka import KafkaProducer
import click
import os
from dotenv import load_dotenv

load_dotenv()

# Load your Alpaca API Keys from the .env file
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"

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
    This streams live financial news from Alpaca.
    """
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("❌ Error: Alpaca API keys not found. Please check your .env file.")
        return

    producer = create_producer(broker)
    print(f"✅ Starting Alpaca live stream to topic '{topic}' at {broker}...")

    def on_open(ws):
        print("🔗 Connected to Alpaca WebSocket. Authenticating...")
        auth_data = {
            "action": "auth",
            "key": ALPACA_API_KEY,
            "secret": ALPACA_SECRET_KEY
        }
        ws.send(json.dumps(auth_data))
        
        # Subscribe to all news headlines
        sub_data = {
            "action": "subscribe",
            "news": ["*"]
        }
        ws.send(json.dumps(sub_data))
        print("📡 Subscribed to live news stream.")

    def on_message(ws, message):
        events = json.loads(message)
        for event in events:
            if event.get('T') == 'n':  # 'n' means News
                headline = event.get('headline', '')
                if headline:
                    payload = {
                        'timestamp': time.time(),
                        'text': headline
                    }
                    producer.send(topic, value=payload)
                    print(f"Sent: {headline}")

    def on_error(ws, error):
        print(f"🚨 WebSocket Error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("🔌 Alpaca WebSocket closed")

    ws = websocket.WebSocketApp(ALPACA_WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

if __name__ == '__main__':
    stream_data()
