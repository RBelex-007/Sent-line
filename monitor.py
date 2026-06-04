# monitor.py
import click
import json
from kafka import KafkaConsumer
from rich.live import Live
from rich.table import Table
from datetime import datetime

def generate_table(data_rows: list) -> Table:
    """Generates a Rich table from a list of recent messages."""
    table = Table(title="VibeFactor Engine: Real-Time Sentiment Monitor")
    table.add_column("Timestamp", style="cyan", no_wrap=True)
    table.add_column("Text", style="magenta", width=80)
    table.add_column("Compound Score", justify="right", style="green")

    for row in data_rows:
        ts = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        score = row['sentiment']['compound']
        
        # Color-code the score for quick visual scanning
        color = "bold green" if score > 0.05 else "bold red" if score < -0.05 else "yellow"
        score_str = f"[{color}]{score:+.4f}[/{color}]"
        
        table.add_row(ts, row['text'], score_str)
        
    return table

@click.command()
@click.option('--topic', default='sentiment_stream', help='The Kafka topic to monitor.')
@click.option('--broker', default='localhost:9092', help='The Kafka broker address.')
def monitor(topic: str, broker: str):
    """
    A real-time CLI monitor for the sentiment analysis pipeline,
    built with Rich and Click.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest' # Only show new messages
    )
    print("⏳ Connecting to Kafka stream for monitoring...")
    
    recent_messages = []
    max_rows = 15 # Number of rows to display in the terminal

    with Live(generate_table(recent_messages), refresh_per_second=4, screen=True) as live:
        live.console.print("✅ Connected. Waiting for sentiment signals...")
        for message in consumer:
            recent_messages.append(message.value)
            if len(recent_messages) > max_rows:
                recent_messages.pop(0)
            live.update(generate_table(recent_messages))

if __name__ == '__main__':
    monitor()
