# PulseFlow - Real-Time Data Intelligence Platform

A production-style event streaming pipeline that ingests, validates, deduplicates, and analyzes high-volume e-commerce events in real time. Processes **1M+ synthetic events** with schema validation, idempotent consumers, dead-letter handling, and a live analytics API.

**Full stack:** Python + Kafka + PostgreSQL + FastAPI + Docker

---

## The Problem

E-commerce platforms generate millions of events per day across product views, searches, cart actions, checkouts, and payments. Processing this data reliably is non-trivial:

- **Clients retry on failure**, sending the same event multiple times
- **Mobile devices sync when reconnected**, sending events hours late
- **Buggy clients send malformed data** that would corrupt analytics
- **Partitioned tables and proper indexes** are required to keep queries fast at scale

PulseFlow solves all of these while running entirely on Docker - no cloud account needed.

---

## Architecture

```
Event Generator
      |
      v
    Kafka (6 partitions)
      |
      +------------------+
      v                  v
Stream Consumer     Dead Letter Queue
      |
      v
Schema Validation
      |
      +------------------+
      v                  v
Deduplication        DLQ (Postgres)
Check
      |
      v
PostgreSQL (partitioned by month)
      |
      v
FastAPI Analytics API
      |
      v
Kafka UI + /docs
```

See `architecture/` for detailed diagrams.

---

## Key Engineering Features

| Feature | Implementation |
|---|---|
| Schema validation | Pydantic V2 with cross-field business rules |
| Deduplication | `processed_event_ids` ledger, checked before every INSERT |
| Idempotent writes | `ON CONFLICT DO NOTHING` at the DB layer |
| Late event handling | Accepted and flagged with `_late_arriving`, not rejected |
| Dead letter queue | Failed events written to Postgres DLQ + Kafka DLQ topic |
| Partitioning | `raw_events` partitioned by month for query performance |
| Batch inserts | Configurable batch size (default 100) with time-based flushing |
| At-least-once delivery | Manual Kafka offset commit after successful DB write |

---

## Demo

Start the full stack:

```bash
make up
```

Open the Kafka UI to watch events flow: `http://localhost:8080`

Open the API docs: `http://localhost:8000/docs`

Generate 1 million events:

```bash
make generate
```

Check pipeline health:

```bash
make health
```

```json
{
  "status": "healthy",
  "total_events": 980241,
  "events_last_hour": 980241,
  "valid_rate_pct": 96.8,
  "duplicate_rate_pct": 2.1,
  "dlq_unresolved": 9847,
  "unique_users_last_hour": 44821
}
```

View conversion funnel:

```bash
make funnel
```

```json
[
  {"step": "Product Views",   "count": 294803, "conversion_pct": null},
  {"step": "Add to Cart",     "count": 118210, "conversion_pct": 40.1},
  {"step": "Checkout",        "count": 78492,  "conversion_pct": 66.4},
  {"step": "Payment Success", "count": 49018,  "conversion_pct": 62.5},
  {"step": "Order Created",   "count": 19801,  "conversion_pct": 40.4}
]
```

---

## Engineering Challenges

**Why partition by month?**
Without partitioning, a query like `WHERE event_timestamp >= NOW() - INTERVAL '1 hour'` scans the full table. With monthly partitions, Postgres prunes to a single partition. Query time on 10M rows goes from several seconds to under 200ms.

**Why a separate deduplication table?**
Checking `SELECT 1 FROM raw_events WHERE event_id = ?` requires scanning a large table. The `processed_event_ids` table has only one column, fits in memory, and keeps dedup checks fast as event volume grows.

**Why batch inserts instead of row-by-row?**
A single INSERT per event means one network round-trip per event. At 500 events/sec, that's 500 round-trips/sec. Batching 100 events per INSERT reduces that to 5 round-trips/sec, dramatically reducing DB CPU and improving throughput.

**Why manual Kafka offset commits?**
Auto-commit would mark events as processed before they're written to Postgres. If the consumer crashes between consuming and writing, those events are lost. Manual commit after a successful DB write gives at-least-once delivery - combined with deduplication, that gives effectively-exactly-once processing.

---

## Benchmarks

Consumer throughput on a 16GB MacBook Pro (M-series):

| Batch Size | Events/sec processed |
|---|---|
| 10  | ~320 |
| 50  | ~820 |
| 100 | ~1,400 |
| 500 | ~2,100 |

See `benchmarks/` for scripts to reproduce these numbers.

**Data quality across 1M synthetic events:**

| Metric | Value |
|---|---|
| Valid events | ~96.5% |
| Duplicates detected | ~2.0% |
| Malformed (DLQ) | ~1.0% |
| Late-arriving (flagged) | ~0.5% |

---

## Running Locally

**Prerequisites:** Docker Desktop, Make

```bash
git clone https://github.com/yourusername/pulseflow
cd pulseflow

# Start infrastructure + API + consumer
make up

# Wait ~30 seconds for Kafka to initialize, then generate events
make generate

# Watch it process
make logs-consumer

# Query the API
make health
make funnel
make dlq
```

---

## Tests

```bash
# Install deps
pip install -r requirements.txt

# Run all tests
make test

# With coverage
make test-cov
```

Tests cover the production-critical scenarios: malformed event rejection, duplicate detection, late-arriving event handling, and business rule validation.

---

## Design Decisions

**Why Kafka instead of Redis Streams or RabbitMQ?**
Kafka's log-based storage means we can replay events from any point in time. This is critical for backfills - if we add a new transformation rule, we can replay the last 7 days of raw events through it without re-generating data.

**Why PostgreSQL instead of a dedicated warehouse?**
At the scale this project targets (1-10M events), partitioned Postgres with the right indexes is more than sufficient. Adding DuckDB or ClickHouse for OLAP queries would be the right next step at 100M+ events/day.

**Why not use Kafka Connect for the sink?**
Kafka Connect is the production answer. For this project, a Python consumer gives full visibility into exactly what the processing logic does - better for learning and for demonstrating the concepts in an interview.

---

## What's Next (Phase 2)

- dbt models for incremental aggregations
- Anomaly detection on event rate spikes
- Parquet export for historical analysis
- Grafana dashboard connected to pipeline_metrics
- Apache Spark for batch reprocessing of historical data
