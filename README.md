
# Bluesky Real-Time Streaming Pipeline

A real-time data streaming pipeline built using **Python, Apache Kafka, PostgreSQL, WebSockets, and React** to collect, process, store, and visualize live activity from the Bluesky social network.

The project demonstrates real-time data ingestion, distributed event streaming, stream processing, database integration, reliability, and live dashboard visualization.

---

## Project Overview

This project captures live activity from the Bluesky social network and processes it through an Apache Kafka-based streaming architecture.

It uses two independent data sources:

1. **Bluesky Jetstream:** A live WebSocket event stream that provides real-time social interactions.
2. **Bluesky Public API:** An API endpoint that provides trending topics, polled at regular intervals.

The collected data is published to separate Kafka topics, processed by independent Python consumers, and stored in PostgreSQL.

WebSockets transmit live metrics and trending topic information to a React dashboard, allowing users to monitor the pipeline in real time without refreshing the browser.

### Data Collected

The project processes the following social interactions:

- Posts
- Likes
- Reposts
- Follows

It also retrieves trending topic information from Bluesky, including topic names and associated metadata.

---

## Data Sources

### 1. Bluesky Jetstream – Real-Time Events

**Source:** [Bluesky Jetstream Documentation](https://docs.bsky.app/blog/jetstream)

Bluesky Jetstream is a public service that provides a real-time stream of events from the Bluesky network through a WebSocket connection.

The project connects to the following Jetstream endpoint:

```text
wss://jetstream2.us-east.bsky.network/subscribe
```

The `bluesky_consumer.py` script:

1. Establishes a WebSocket connection to Bluesky Jetstream.
2. Continuously receives incoming JSON events.
3. Parses the incoming messages.
4. Publishes the events to the Kafka topic `bluesky-events`.

Examples of incoming events include:

- A user publishing a post.
- A user liking a post.
- A user reposting content.
- A user following another account.

### 2. Bluesky Public API – Trending Topics

**Source:** [Bluesky API Documentation](https://docs.bsky.app/)

The project retrieves trending topics using the following public API endpoint:

```text
https://public.api.bsky.app/xrpc/app.bsky.unspecced.getTrends?limit=10
```

The `trending_topics.py` script:

1. Sends a request to the Bluesky public API.
2. Retrieves the top 10 trending topics.
3. Extracts the required information.
4. Publishes the resulting data to the Kafka topic `bluesky-trends`.
5. Repeats the process every 30 seconds.

The trending topics pipeline operates independently of the Jetstream event pipeline.

---

## System Architecture

The project consists of two independent data ingestion and processing pipelines.

### 1. Real-Time Events Pipeline

```text
             BLUESKY JETSTREAM
                     |
                     v
             Python Producer
             (bluesky_consumer.py)
                     |
                     v
               Apache Kafka
              bluesky-events
                     |
                     v
             Events Processor
             (events_processor.py)
                     |
              +------+------+
              |             |
              v             v
          PostgreSQL     WebSocket
                            |
                            v
                      React Dashboard
```

### 2. Trending Topics Pipeline

```text
             BLUESKY PUBLIC API
                     |
                     v
             Python Producer
             (trending_topics.py)
                     |
                     v
               Apache Kafka
              bluesky-trends
                     |
                     v
             Trends Processor
             (trends_processor.py)
                     |
              +------+------+
              |             |
              v             v
          PostgreSQL     WebSocket
                            |
                            v
                      React Dashboard
```

### 3. Overall Data Flow

```text
                 BLUESKY DATA SOURCES
                  /              \
                 /                \
                v                  v
          Jetstream API        Public API
                |                  |
                v                  v
        Event Producer       Trends Producer
                |                  |
                v                  v
          Kafka Topic         Kafka Topic
        bluesky-events       bluesky-trends
                |                  |
                v                  v
       Events Processor      Trends Processor
                |                  |
                v                  v
           PostgreSQL         PostgreSQL
                |                  |
                v                  v
           WebSocket          WebSocket
                |                  |
                +--------+---------+
                         |
                         v
                   React Dashboard
```

---

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data ingestion and processing |
| Bluesky Jetstream | Real-time event source |
| Bluesky Public API | Trending topic data source |
| Apache Kafka | Distributed event streaming |
| PostgreSQL | Persistent data storage |
| SQLite | Local event buffering |
| WebSockets | Real-time communication |
| React | Interactive frontend dashboard |
| Vite | Frontend development and build tooling |
| Streamlit | Additional dashboard |
| Docker | Running Kafka |

---

## Project Structure

```text
bluesky-realtime-streaming-pipeline/
│
├── bluesky_consumer.py
├── events_processor.py
├── trending_topics.py
├── trends_processor.py
├── dashboard.py
├── requirements.txt
├── .env
│
├── frontend/
│   ├── public/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Accuracy.jsx
│   │   │   ├── DatabaseMetrics.jsx
│   │   │   ├── Latency.jsx
│   │   │   ├── MetricsCards.jsx
│   │   │   ├── Throughput.jsx
│   │   │   └── TrendingTopics.jsx
│   │   │
│   │   ├── App.jsx
│   │   ├── App.css
│   │   ├── index.css
│   │   └── main.jsx
│   │
│   ├── package.json
│   └── vite.config.js
│
├── .gitignore
└── LICENSE
```

---

# Reproducing the Project

Follow the steps below to set up and run the pipeline locally.

## Prerequisites

Install the following software before starting:

- Python 3.12 or later
- Docker
- PostgreSQL
- Node.js and npm
- Git

You will also need an internet connection to access the Bluesky public services.

## Step 1: Clone the Repository

Clone the GitHub repository:

```bash
git clone https://github.com/SyedAfnanHasan/bluesky-realtime-streaming-pipeline.git

cd bluesky-realtime-streaming-pipeline
```

## Step 2: Set Up the Python Environment

Create a virtual environment:

```bash
python3 -m venv venv
```

Activate it on macOS or Linux:

```bash
source venv/bin/activate
```

On Windows:

```powershell
venv\Scripts\activate
```

## Step 3: Install Python Dependencies

The repository includes a `requirements.txt` file containing the Python dependencies and their versions.

Install the dependencies using:

```bash
pip install -r requirements.txt
```

## Step 4: Configure Environment Variables

Create a `.env` file in the project root:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

This variable configures the Kafka bootstrap server used by the event producer.

The other Python scripts currently use their own Kafka connection settings, so ensure those settings also point to the correct broker.

The `.env` file is excluded from Git and should be created locally.

## Step 5: Start Apache Kafka

The project uses Apache Kafka running through Docker.

If you are setting up Kafka for the first time, start a Kafka container:

```bash
docker run --name kafka-docker -p 9092:9092 apache/kafka:4.4.0
```

Make sure the broker is accessible at:

```text
localhost:9092
```

If the Kafka container already exists but is stopped, start it using:

```bash
docker start kafka-docker
```

### Kafka Topics

The project uses two Kafka topics:

| Kafka Topic | Description |
|---|---|
| bluesky-events | Incoming Bluesky social events |
| bluesky-trends | Trending topic snapshots |

Create the topics if they do not already exist:

```bash
docker exec kafka-docker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic bluesky-events \
  --partitions 1 \
  --replication-factor 1
```

```bash
docker exec kafka-docker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic bluesky-trends \
  --partitions 1 \
  --replication-factor 1
```

Verify the topics:

```bash
docker exec kafka-docker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## Step 6: Set Up PostgreSQL

Start your PostgreSQL server.

### 6.1 Create the Project Database

Open PostgreSQL using your terminal:

```bash
psql -U postgres
```

Create the project database:

```sql
CREATE DATABASE bluesky_streaming;
```

Exit PostgreSQL:

```sql
\q
```

### 6.2 Connect to the Project Database

Connect to the newly created database:

```bash
psql -h localhost -p 5432 -U YOUR_USERNAME -d bluesky_streaming
```

Replace `YOUR_USERNAME` with your PostgreSQL username.

### 6.3 Create the Required Tables

The project requires two PostgreSQL tables:

| Table | Purpose |
|---|---|
| event_metrics | Stores periodic activity-rate metrics |
| trend_metrics | Stores trending-topic snapshots |

Run the following SQL commands inside the `bluesky_streaming` database.

**Table 1: event_metrics**

This table stores the average number of posts, likes, reposts, and follows per second.

```sql
CREATE TABLE IF NOT EXISTS event_metrics (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    avg_posts_per_second INTEGER NOT NULL,
    avg_likes_per_second INTEGER NOT NULL,
    avg_reposts_per_second INTEGER NOT NULL,
    avg_follows_per_second INTEGER NOT NULL
);
```

**Table 2: trend_metrics**

This table stores trending-topic snapshots received from the Bluesky public API.

```sql
CREATE TABLE IF NOT EXISTS trend_metrics (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    rank INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    post_count BIGINT,
    category TEXT,
    status TEXT
);
```

### 6.4 Verify Table Creation

To verify that both tables were created successfully, run:

```sql
\dt
```

To inspect the structure of the event metrics table:

```sql
\d event_metrics
```

To inspect the structure of the trending topics table:

```sql
\d trend_metrics
```

You should now have both tables available in your `bluesky_streaming` database.

### 6.5 PostgreSQL Connection Configuration

The Python processors connect to PostgreSQL using the following settings:

| Setting | Value |
|---|---|
| Database | bluesky_streaming |
| Host | localhost |
| Port | 5432 |
| User | Your PostgreSQL username |
| Password | Your PostgreSQL password, if configured |

Make sure the PostgreSQL connection settings in the Python scripts match your local configuration.

**Important:** Create the database and tables before starting the Python processors.

## Step 7: Install Frontend Dependencies

Navigate to the frontend directory:

```bash
cd frontend
```

Install the Node.js dependencies:

```bash
npm install
```

Return to the project root:

```bash
cd ..
```

---

# Running the Pipeline

Start the backend components in separate terminal sessions.

Make sure Kafka and PostgreSQL are running before launching the Python scripts.

### Terminal 1 – Events Processor

```bash
python events_processor.py
```

The events processor:

- Consumes incoming messages from `bluesky-events`.
- Processes incoming social events.
- Stores periodic metrics in PostgreSQL.
- Calculates event and processing metrics.
- Publishes real-time updates through WebSockets.

### Terminal 2 – Trends Processor

```bash
python trends_processor.py
```

The trends processor:

- Consumes trending topic snapshots from `bluesky-trends`.
- Processes incoming trend information.
- Stores changed trend snapshots in PostgreSQL.
- Publishes trending topic updates through WebSockets.

### Terminal 3 – Bluesky Event Producer

```bash
python bluesky_consumer.py
```

The event producer:

- Connects to Bluesky Jetstream.
- Continuously receives live social events.
- Publishes incoming events to Kafka.

### Terminal 4 – Trending Topics Producer

```bash
python trending_topics.py
```

The trending topics producer:

- Connects to the Bluesky public API.
- Retrieves the top trending topics.
- Publishes the data to Kafka every 30 seconds.

### Terminal 5 – React Dashboard

Navigate to the frontend directory:

```bash
cd frontend
```

Start the development server:

```bash
npm run dev
```

Open the local URL displayed by Vite in your browser.

The React dashboard establishes WebSocket connections to receive live updates from the backend.

### Optional – Streamlit Dashboard

The project also includes a Streamlit dashboard.

From the project root:

```bash
streamlit run dashboard.py
```

---

## Dashboard and Monitoring

The React dashboard provides a real-time view of the streaming pipeline.

### Activity Metrics

Displays the rate of incoming social interactions:

- Posts per second
- Likes per second
- Reposts per second
- Follows per second

### Processing Throughput

Displays the number of events processed per second by the pipeline.

This helps monitor the processing capacity of the system.

### Event Latency

Displays sampled event processing latency, providing an indication of the time taken to process incoming events.

### Accuracy Metrics

Displays event processing success and failure statistics.

### Database Metrics

Provides information about the average activity rates stored in PostgreSQL.

### Trending Topics

Displays the latest trending topics retrieved from the Bluesky public API.

---

## WebSocket Endpoints

The project uses separate WebSocket servers to transmit backend metrics and data to the React frontend.

| Port | Purpose |
|---|---|
| 8765 | Event and activity metrics |
| 8766 | Trending topics |
| 8767 | Incoming processing throughput |

WebSockets provide continuous communication between the backend and frontend without requiring repeated HTTP requests or browser refreshes.

---

## Data Storage

PostgreSQL is used as the persistent storage layer for processed metrics and trending topic information.

The project stores periodic activity metrics snapshots in the `event_metrics` table.

Trending topic snapshots are stored in the `trend_metrics` table whenever changes are detected.

SQLite is used locally by the event ingestion component as a buffer for handling temporary Kafka publishing interruptions.

This provides an additional mechanism for protecting incoming events when the Kafka broker is temporarily unavailable, depending on the buffering and recovery logic implemented by the producer.

---

## Scalability

The pipeline uses Apache Kafka to decouple data ingestion from event processing.

This allows producers and consumers to operate independently.

Kafka topics can be configured with additional partitions to distribute incoming events across multiple consumers.

By increasing the number of partitions and running additional consumer instances within the same consumer group, the system can process events in parallel and increase throughput.

This architecture provides a foundation for scaling the pipeline as incoming event volume grows.

---

## Reliability and Data Consistency

Kafka acts as an intermediary between the data producers and processing components.

This provides several advantages:

- Producers and consumers operate independently.
- Incoming events can be buffered in Kafka.
- Consumers can process messages independently of the ingestion process.
- Consumer offsets help track message processing progress.
- Duplicate event handling helps prevent repeated processing of recently seen events.
- Local SQLite buffering provides an additional mechanism for handling temporary Kafka publishing interruptions.

The overall reliability of the pipeline depends on producer buffering, Kafka configuration, consumer offset handling, and database operations.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

**Built as a real-time data engineering project using the Bluesky social network as a live event source.**
