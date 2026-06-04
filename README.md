# Sent-line: Real-Time Sentiment Pipeline

Sent-line is a high-throughput, low-latency command-line interface (CLI) pipeline designed for real-time sentiment analysis of financial text and alternative data. It ingests simulated live feeds, processes the sentiment of the text using Natural Language Processing (NLP), persists the analytics to a time-series database, and visualizes the risk factors in a live terminal dashboard.

## 🏗️ System Architecture

The project is built using a decoupled architecture, connected via a distributed event streaming platform.

1. **Ingestion Service (`ingest.py`)**: Simulates a live data feed (e.g., financial news, tweets) and streams raw text JSON logs into an Apache Kafka topic.
2. **Processing Engine (`engine.py`)**: A Faust stream processor that consumes raw text, computes sentiment scores using VADER, publishes the enriched data to a downstream Kafka topic, and writes historical records to QuestDB.
3. **Live Monitor (`monitor.py`)**: A rich, real-time terminal dashboard that consumes the sentiment stream and displays a color-coded, rolling time-series view of the data.

## 🛠️ Technology Stack

- **Language**: Python 3.x
- **Event Streaming**: Apache Kafka
- **Stream Processing**: Faust-Streaming
- **NLP / Quant Logic**: VADER Sentiment Analysis
- **Time-Series Database**: QuestDB
- **CLI & UI**: Click, Rich
- **Infrastructure**: Docker & Docker Compose

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.8+**
- **Docker Desktop** (Must be installed and running)

### 1. Environment Setup

Open a terminal in the project directory and create a virtual environment:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment (Windows)
venv\Scripts\activate

# (Optional) Activate the virtual environment (Mac/Linux)
# source venv/bin/activate
```

Install the required dependencies:

```bash
pip install kafka-python vaderSentiment faust-streaming click rich psycopg2-binary
```

### 2. Infrastructure Setup (Kafka & QuestDB)

Ensure Docker Desktop is running. Start the required infrastructure in the background using Docker Compose:

```bash
docker compose up -d
```

### 3. Initialize the Database

Before the engine can persist data, you need to create the table in QuestDB.

1. Open your web browser and navigate to **http://localhost:9000**.
2. Paste the following SQL query into the top input box and click **Run**:

```sql
CREATE TABLE sentiment_signals (
  ts TIMESTAMP,
  text STRING,
  compound_score DOUBLE
) timestamp(ts) PARTITION BY DAY;
```

---

## 🏃‍♂️ Running the Pipeline

To see the pipeline in action, you will need to open **three separate terminal windows**. 

> **⚠️ Important:** You must activate the virtual environment (`venv\Scripts\activate`) in **every** terminal window before running the commands.

**Terminal 1: Start the Monitor Dashboard**
Start the dashboard first so it is ready to visualize the data stream.
```bash
python monitor.py
```

**Terminal 2: Start the Processing Engine**
Start the Faust worker to actively listen for incoming data, calculate sentiment, and save to QuestDB.
```bash
faust -A engine worker -l info
```

**Terminal 3: Start the Ingestion Firehose**
Finally, start the mock data producer to flood the pipeline with events.
```bash
python ingest.py
```

Navigate back to **Terminal 1** to watch your live sentiment pipeline in action! To stop any of the processes, press `Ctrl + C` in their respective terminal windows.