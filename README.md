# Sent-line: Real-Time Sentiment Pipeline (C++ Edition)

Sent-line is an ultra-fast, low-latency pipeline built entirely in C++ for real-time sentiment analysis of financial text streams. It ingests simulated live feeds via Kafka, processes the sentiment text utilizing native speeds, persists the analytics to QuestDB, and visualizes the risk factors in a live terminal dashboard.

## 🏗️ System Architecture

The project is built using a decoupled architecture, connected via a distributed event streaming platform.

1. **Ingestion Service (`ingest.cpp`)**: Simulates a live data feed and securely streams raw text JSON logs to Kafka utilizing `librdkafka`.
2. **Processing Engine (`engine.cpp`)**: A high-speed native consumer that calculates text sentiment, publishes enriched data back into Kafka, and rapidly inserts historical records to QuestDB via `libpqxx`.
3. **Live Monitor (`monitor.cpp`)**: A terminal UI that continuously consumes the processed stream and visualizes the color-coded flow.

## 🛠️ Technology Stack

- **Language**: C++ 17
- **Event Streaming**: Apache Kafka (`librdkafka`)
- **Database API**: PostgreSQL Protocol (`libpqxx`)
- **Serialization**: Modern C++ JSON (`nlohmann/json`)
- **Time-Series Database**: QuestDB
- **Build System**: CMake & vcpkg
- **Infrastructure**: Docker & Docker Compose

---

## 🚀 Getting Started

### Prerequisites

- **C++ Compiler** (MSVC or MinGW)
- **CMake**
- **vcpkg** (Microsoft C++ package manager)
- **Docker Desktop** (Must be installed and running)

### 1. Build & Compilation

Using `vcpkg`, install dependencies, then configure and build the C++ executables using CMake:

```bash
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=[path-to-your-vcpkg]/scripts/buildsystems/vcpkg.cmake
cmake --build build --config Release
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