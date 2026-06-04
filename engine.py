# engine.py
import faust
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize VADER once to be reused
analyzer = SentimentIntensityAnalyzer()

# Define the Faust application, connecting to the Kafka broker
app = faust.App('VibeFactor-Engine', broker='kafka://localhost:9092', value_serializer='json')

# Define the data models (schemas) for input and output messages
class RawTextMessage(faust.Record):
    timestamp: float
    text: str

class SentimentScore(faust.Record):
    timestamp: float
    text: str
    sentiment: dict # VADER returns a dict: {'neg', 'neu', 'pos', 'compound'}

QUESTDB_CONN_STR = "user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb"

def persist_to_questdb(data: SentimentScore):
    """Writes a sentiment score record to QuestDB."""
    try:
        # In production, use a connection pool (e.g., psycopg2.pool)
        # instead of connecting on every message.
        with psycopg2.connect(QUESTDB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO sentiment_signals (ts, text, compound_score) VALUES (%s, %s, %s)",
                    (
                        int(data.timestamp * 1_000_000), # QuestDB uses microsecond precision
                        data.text,
                        data.sentiment['compound']
                    )
                )
    except Exception as e:
        print(f"🚨 Error writing to QuestDB: {e}")

# Define the Kafka topics
raw_text_topic = app.topic('raw_text_stream', value_type=RawTextMessage)
sentiment_topic = app.topic('sentiment_stream', value_type=SentimentScore)

@app.agent(raw_text_topic)
async def process_sentiment(messages):
    """
    This Faust agent consumes raw text, analyzes sentiment with VADER,
    and produces the result to another Kafka topic.
    """
    async for msg in messages:
        # 1. Perform the sentiment analysis
        sentiment_scores = analyzer.polarity_scores(msg.text)

        # 2. Create the enriched output record
        output = SentimentScore(
            timestamp=msg.timestamp,
            text=msg.text,
            sentiment=sentiment_scores
        )

        # 3. Send the result to the output topic for the monitor to consume
        await sentiment_topic.send(value=output)
        print(f"Processed: {output.text[:40]}... -> {output.sentiment['compound']:.2f}")

        # 4. Persist to the database
        persist_to_questdb(output)
