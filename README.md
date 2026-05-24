# Real-Time E-Commerce Analytics Pipeline

Production-grade streaming data pipeline processing 100K+ e-commerce events
using Kafka, Spark Structured Streaming, Delta Lake, and Snowflake.

## Architecture
Olist Dataset → Kafka → Spark Streaming → Delta Lake (Bronze/Silver/Gold) → Snowflake → Dashboard

## Stack
- Apache Kafka (event streaming)
- PySpark + Spark Structured Streaming (processing)
- Delta Lake (Medallion Architecture)
- Snowflake (data warehouse)
- Apache Airflow (orchestration)
- Great Expectations (data quality)
- Docker Compose (local infrastructure)

## Status
🚧 In progress
