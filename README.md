# PulseFlow - Real-Time Data Intelligence Platform

A production-style event streaming pipeline that ingests, validates, deduplicates, and analyzes high-volume e-commerce events in real time.

**1,000,000 events processed | 405 events/sec | 98.97% valid rate | 2% duplicate detection**

**Stack:** Python + Kafka + PostgreSQL + FastAPI + Docker

---

## Architecture

![Architecture](architecture/architecture.png)

---

## Live Demo

![Demo](docs/demo.png)

**Conversion Funnel (24 hours):**
- 293,146 product views
- 40.1% added to cart
- 67% reached checkout
- 62.5% payment success
- 40.1% order created

---

## Key Engineering Features

| Feature | Implementation |
|---|---|
| Schema validation | Pydantic V2 with cross-field business rules |
| Deduplication | `processed_event_ids` ledger checked before every INSERT |
| Idempotent writes | `ON CONFLICT DO NOTHING` at the DB layer |
| Late event handling | Accepted and flagged with `_late_arriving`, not rejected |
| Dead letter queue | Failed events written to Postgres DLQ + Kafka DLQ topic |
| Partitioning | `raw_events` partitioned by month for query performance |
| Batch inserts | Configurable batch size with time-based flushing |
| At-least-once delivery | Manual Kafka offset commit after successful DB write |

---

## Run It Locally

**Prerequisites:** Docker Desktop, Make

```bash
git clone https://github.com/vinay27-code/pulseflow
cd pulseflow
make up          # Start all services
make generate    # Generate 1M events
make health      # Check pipeline health
make funnel      # View conversion funnel
```

Open http://localhost:8000/docs for the live API.
Open http://localhost:8080 for the Kafka UI.

---

## Engineering Challenges

**Why partition by month?**
Without partitioning, queries scan the full table. With monthly partitions, Postgres prunes to a single partition. Query time on 10M rows drops from several seconds to under 200ms.

**Why a separate deduplication table?**
Checking `processed_event_ids` (single column, fits in memory) is far faster than scanning `raw_events` as the table grows.

**Why manual Kafka offset commits?**
Auto-commit marks events as processed before they're written to Postgres. If the consumer crashes mid-write, events are lost. Manual commit after a successful DB write gives at-least-once delivery - combined with deduplication, that's effectively exactly-once processing.

---

## Tests

```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

23 tests covering malformed event rejection, duplicate detection, late-arriving event handling, and business rule validation.