# Financial Data Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-FF6B6B.svg)](https://redpanda.com/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B.svg)](https://www.getdbt.com/)

Real-time market data pipeline using Kafka, PostgreSQL, and dbt. Built to explore stream processing patterns and exactly-once semantics.

> ⚠️ **Note**: Uses simulated stock data for demonstration purposes.

## What this does

- **Ingests** real-time stock prices from Yahoo Finance
- **Streams** data through Kafka with Avro serialization
- **Processes** and validates data with exactly-once delivery
- **Stores** in PostgreSQL for downstream analytics
- **Transforms** using dbt for business-ready datasets

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Yahoo     │     │   Kafka     │     │   Stream    │     │  PostgreSQL │
│  Finance    │────▶│  (Redpanda) │────▶│  Processor  │────▶│   + dbt     │
│   API       │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────┐
                                               │    Kafka    │
                                               │     UI      │
                                               └─────────────┘
```

## Key Features

**Stream Processing**
- Exactly-once semantics with idempotent producers
- Avro schema registry for data contracts
- Manual offset management for reliable processing
- Batch inserts to minimize database load

**Data Quality**
- Schema validation at ingestion
- Automated data quality checks in dbt
- Gap detection and stale data monitoring
- Anomaly detection (price spikes > 10%)

**Observability**
- Structured logging with structlog
- Data quality dashboard via dbt models
- Kafka UI for topic monitoring

## Quick Start

```bash
# Clone and start infrastructure
git clone https://github.com/CR7REX/financial-data-platform.git
cd financial-data-platform
docker-compose up -d

# Wait for services to be ready
sleep 10

# Start producing data
docker-compose logs -f producer

# In another terminal, watch processing
docker-compose logs -f processor

# Access Kafka UI
curl http://localhost:8080
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Message Queue | Redpanda (Kafka-compatible) |
| Schema Registry | Built-in Redpanda |
| Database | PostgreSQL |
| Transformation | dbt |
| Serialization | Avro |
| Orchestration | Airflow (for batch tasks) |

## Data Flow

1. **Producer** polls Yahoo Finance every 10 seconds
2. **Avro serialization** enforces schema contracts
3. **Kafka** persists messages with replication
4. **Consumer** processes batches with manual commits
5. **PostgreSQL** stores validated records
6. **dbt** transforms raw data to analytics-ready tables

## Project Structure

```
.
├── producer/           # Kafka producer (real-time ingestion)
│   ├── producer.py
│   └── Dockerfile
├── processor/          # Stream processor (consumer)
│   ├── processor.py
│   └── Dockerfile
├── dbt/                # dbt models
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
├── airflow/            # DAGs for batch processing
│   └── dags/
└── docker-compose.yml  # Infrastructure setup
```

## dbt Models

- **stg_stock_prices** - Cleaned and validated price data
- **fct_hourly_prices** - OHLC aggregation by hour
- **fct_data_quality** - Data quality monitoring dashboard

## Configuration

Environment variables:
```bash
KAFKA_BROKER=redpanda:9092
SCHEMA_REGISTRY_URL=http://redpanda:8081
DATABASE_URL=postgresql://airflow:airflow@postgres:5432/financial_data
```

## Lessons Learned

- Exactly-once is harder than it looks. Enable idempotence, use transactions.
- Schema evolution needs planning. Avro + Schema Registry helps.
- Batch inserts beat individual INSERTs by 10x.
- Kafka consumer lag is the metric that matters.

## Future Work

- [ ] Add Kafka Streams for windowed aggregations
- [ ] Implement dead letter queue for failed records
- [ ] Add Prometheus metrics export
- [ ] CDC (Change Data Capture) with Debezium

## Disclaimer

For educational purposes only. Not for trading.

---

*Built to understand stream processing at scale. The data flows, the pipeline holds.*