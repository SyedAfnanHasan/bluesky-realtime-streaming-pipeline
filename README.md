# Bluesky Real-Time Streaming Pipeline

A real-time data streaming pipeline built using **Python, Apache Kafka, PostgreSQL, WebSockets, and React** to collect, process, store, and visualize live activity from the Bluesky social network.

The project demonstrates real-time data ingestion, distributed event streaming, stream processing, database integration, and live dashboard visualization.

## Project Overview

This project captures live events from Bluesky using its Jetstream firehose and processes them through an Apache Kafka-based streaming architecture.

The pipeline tracks social interactions such as:

- Posts
- Likes
- Reposts
- Follows

It also collects trending topic information and processes it separately.

Processed data is stored in PostgreSQL, while WebSockets transmit live metrics to a React dashboard without requiring the browser to refresh.

## Architecture

```text
                 BLUESKY JETSTREAM
                        |
                        v
                Python Event Producer
                        |
                        v
                 Apache Kafka
                 (bluesky-events)
                        |
                        v
                Events Processor
                        |
                +-------+-------+
                |               |
                v               v
            PostgreSQL       WebSockets
                                |
                                v
                         React Dashboard


              TRENDING TOPICS PIPELINE

                Trending Topics Producer
                        |
                        v
                   Apache Kafka
                 (bluesky-trends)
                        |
                        v
                 Trends Processor
                        |
                +-------+-------+
                |               |
                v               v
            PostgreSQL       WebSockets
                                |
                                v
                         React Dashboard
