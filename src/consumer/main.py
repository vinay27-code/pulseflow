"""
PulseFlow Stream Consumer

Reads events from Kafka and processes them through a validation and
storage pipeline. Key behaviors:

1. Schema validation   - rejects events that don't match EcommerceEvent
2. Deduplication       - idempotent writes using a processed_event_ids ledger
3. Late event handling - accepts and flags events older than 24 hours
4. Dead letter queue   - malformed events written to DLQ for later review
5. Metrics             - throughput and error rates written to pipeline_metrics

Interview talking points this code enables:
  "What happens if Kafka delivers an event twice?"
    -> processed_event_ids table checked before INSERT; duplicate skipped
  "How do you handle malformed data?"
    -> ValidationResult wraps every event; failures routed to DLQ
  "How do you handle late-arriving events?"
    -> event_timestamp vs received_at gap; _late_arriving flag in properties
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError

from src.models.events import EcommerceEvent, ProcessingResult, ValidationResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Configuration from environment
DATABASE_URL           = os.getenv("DATABASE_URL", "postgresql://pulseflow:pulseflow_secret@localhost:5432/pulseflow")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID         = os.getenv("KAFKA_GROUP_ID", "pulseflow-consumer-group")
KAFKA_TOPIC            = os.getenv("KAFKA_TOPIC", "ecommerce-events")
KAFKA_DLQ_TOPIC        = os.getenv("KAFKA_DLQ_TOPIC", "ecommerce-events-dlq")

# Batch size for database inserts - larger = better throughput, higher latency
DB_BATCH_SIZE          = int(os.getenv("DB_BATCH_SIZE", "100"))
# How often to flush incomplete batches (seconds)
DB_FLUSH_INTERVAL      = float(os.getenv("DB_FLUSH_INTERVAL", "2.0"))


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":        KAFKA_BOOTSTRAP_SERVERS,
        "group.id":                 KAFKA_GROUP_ID,
        "auto.offset.reset":        "earliest",
        # Manual commit - we commit AFTER writing to Postgres
        # This gives us at-least-once delivery semantics
        "enable.auto.commit":       False,
        "max.poll.interval.ms":     300000,
        "session.timeout.ms":       30000,
        "fetch.min.bytes":          1024,
        "fetch.wait.max.ms":        500,
    })


def make_dlq_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks":              "1",
    })


def make_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def validate_event(raw_payload: dict) -> ValidationResult:
    """
    Validate a raw event dict against the EcommerceEvent schema.

    Returns a ValidationResult regardless of outcome - never raises.
    The caller decides what to do with invalid events.
    """
    start = time.perf_counter()
    try:
        event = EcommerceEvent(**raw_payload)
        return ValidationResult(
            is_valid=True,
            event=event,
            raw_payload=raw_payload,
            validation_time_ms=(time.perf_counter() - start) * 1000,
        )
    except ValidationError as e:
        errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
        return ValidationResult(
            is_valid=False,
            errors=errors,
            raw_payload=raw_payload,
            validation_time_ms=(time.perf_counter() - start) * 1000,
        )
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[f"Unexpected validation error: {str(e)}"],
            raw_payload=raw_payload,
            validation_time_ms=(time.perf_counter() - start) * 1000,
        )


def is_duplicate(event_id: str, cursor) -> bool:
    """
    Check the deduplication ledger.

    This is a SELECT before INSERT pattern. In high-throughput systems
    you'd use Redis for this check to avoid DB load, but Postgres with
    a proper index is fast enough for this scale.
    """
    cursor.execute(
        "SELECT 1 FROM processed_event_ids WHERE event_id = %s LIMIT 1",
        (event_id,)
    )
    return cursor.fetchone() is not None


def mark_as_processed(event_id: str, cursor) -> None:
    """Write to the deduplication ledger. Called BEFORE inserting the event."""
    partition_key = datetime.now(timezone.utc).strftime("%Y-%m")
    cursor.execute(
        """
        INSERT INTO processed_event_ids (event_id, partition_key)
        VALUES (%s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (event_id, partition_key)
    )


def insert_events_batch(events: list[dict], cursor) -> int:
    """Batch insert valid events into raw_events. Returns count inserted."""
    if not events:
        return 0

    cursor.executemany(
        """
        INSERT INTO raw_events (
            event_id, event_type, user_id, session_id, product_id, order_id,
            amount, currency, properties, client_ip, user_agent,
            event_timestamp, received_at, processed_at,
            is_duplicate, is_valid, validation_errors
        ) VALUES (
            %(event_id)s, %(event_type)s, %(user_id)s, %(session_id)s,
            %(product_id)s, %(order_id)s, %(amount)s, %(currency)s,
            %(properties)s, %(client_ip)s, %(user_agent)s,
            %(event_timestamp)s, %(received_at)s, NOW(),
            %(is_duplicate)s, %(is_valid)s, %(validation_errors)s
        )
        """,
        events
    )
    return len(events)


def insert_dlq_batch(failures: list[dict], cursor) -> None:
    """Write failed events to the dead letter queue."""
    if not failures:
        return

    cursor.executemany(
        """
        INSERT INTO dead_letter_queue (
            event_id, raw_payload, failure_reason, failure_details, retry_count
        ) VALUES (
            %(event_id)s, %(raw_payload)s, %(failure_reason)s,
            %(failure_details)s, %(retry_count)s
        )
        """,
        failures
    )


def record_pipeline_metric(metric_name: str, value: float, tags: dict, cursor) -> None:
    cursor.execute(
        "INSERT INTO pipeline_metrics (metric_name, metric_value, tags) VALUES (%s, %s, %s)",
        (metric_name, value, json.dumps(tags))
    )


class ConsumerStats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.total_received   = 0
        self.total_valid      = 0
        self.total_duplicates = 0
        self.total_invalid    = 0
        self.total_late       = 0
        self.start_time       = time.time()
        self.last_log_time    = time.time()
        self.last_log_count   = 0

    def log_if_due(self):
        now = time.time()
        if now - self.last_log_time < 10:
            return

        elapsed      = now - self.last_log_time
        recent_count = self.total_received - self.last_log_count
        rps          = recent_count / elapsed if elapsed > 0 else 0

        total = self.total_received or 1
        logger.info(
            f"Processed: {self.total_received:>10,} | "
            f"RPS: {rps:>6.0f} | "
            f"Valid: {self.total_valid:,} ({self.total_valid/total*100:.1f}%) | "
            f"Dupes: {self.total_duplicates:,} ({self.total_duplicates/total*100:.1f}%) | "
            f"Invalid: {self.total_invalid:,} ({self.total_invalid/total*100:.1f}%) | "
            f"Late: {self.total_late:,}"
        )
        self.last_log_time  = now
        self.last_log_count = self.total_received


def run():
    logger.info("Starting PulseFlow Consumer")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Group: {KAFKA_GROUP_ID} | Topic: {KAFKA_TOPIC}")

    # Wait for Postgres
    db_conn = None
    for attempt in range(30):
        try:
            db_conn = make_db_connection()
            logger.info("Connected to PostgreSQL")
            break
        except Exception as e:
            logger.warning(f"DB not ready (attempt {attempt + 1}/30): {e}")
            time.sleep(2)
    else:
        logger.error("Could not connect to PostgreSQL. Exiting.")
        sys.exit(1)

    consumer     = make_consumer()
    dlq_producer = make_dlq_producer()
    stats        = ConsumerStats()

    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to {KAFKA_TOPIC}")

    # Pending batches
    valid_batch   = []
    invalid_batch = []
    last_flush    = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=0.1)

            if msg is None:
                # No message - flush if batch is stale
                if time.time() - last_flush >= DB_FLUSH_INTERVAL and (valid_batch or invalid_batch):
                    _flush_batches(valid_batch, invalid_batch, db_conn, stats)
                    consumer.commit(asynchronous=False)
                    valid_batch.clear()
                    invalid_batch.clear()
                    last_flush = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            stats.total_received += 1

            # Decode the raw message
            try:
                raw_payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Could not decode message: {e}")
                invalid_batch.append({
                    "event_id":       None,
                    "raw_payload":    json.dumps({"_raw": str(msg.value())}),
                    "failure_reason": "decode_error",
                    "failure_details": json.dumps({"error": str(e)}),
                    "retry_count":    0,
                })
                stats.total_invalid += 1
                continue

            # Validate against schema
            result = validate_event(raw_payload)

            if not result.is_valid:
                # Route to dead letter queue
                invalid_batch.append({
                    "event_id":       raw_payload.get("event_id"),
                    "raw_payload":    json.dumps(raw_payload, default=str),
                    "failure_reason": "validation_error",
                    "failure_details": json.dumps({"errors": result.errors}),
                    "retry_count":    0,
                })
                # Also send to Kafka DLQ topic for downstream consumers
                dlq_producer.produce(
                    KAFKA_DLQ_TOPIC,
                    value=json.dumps({
                        "original_payload": raw_payload,
                        "errors": result.errors,
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                    }, default=str).encode()
                )
                dlq_producer.poll(0)
                stats.total_invalid += 1
                continue

            event = result.event
            event_id_str = str(event.event_id)

            # Check deduplication
            with db_conn.cursor() as cur:
                if is_duplicate(event_id_str, cur):
                    stats.total_duplicates += 1
                    # Still record it so we have visibility into duplicate rates
                    valid_batch.append({
                        "event_id":         event_id_str,
                        "event_type":       event.event_type,
                        "user_id":          event.user_id,
                        "session_id":       event.session_id,
                        "product_id":       event.product_id,
                        "order_id":         event.order_id,
                        "amount":           float(event.amount) if event.amount else None,
                        "currency":         event.currency,
                        "properties":       json.dumps(event.properties),
                        "client_ip":        str(event.client_ip) if event.client_ip else None,
                        "user_agent":       event.user_agent,
                        "event_timestamp":  event.event_timestamp.isoformat(),
                        "received_at":      datetime.now(timezone.utc).isoformat(),
                        "is_duplicate":     True,
                        "is_valid":         True,
                        "validation_errors": "[]",
                    })
                    continue

            is_late = event.properties.get("_late_arriving", False)
            if is_late:
                stats.total_late += 1

            valid_batch.append({
                "event_id":         event_id_str,
                "event_type":       event.event_type,
                "user_id":          event.user_id,
                "session_id":       event.session_id,
                "product_id":       event.product_id,
                "order_id":         event.order_id,
                "amount":           float(event.amount) if event.amount else None,
                "currency":         event.currency,
                "properties":       json.dumps(event.properties),
                "client_ip":        str(event.client_ip) if event.client_ip else None,
                "user_agent":       event.user_agent,
                "event_timestamp":  event.event_timestamp.isoformat(),
                "received_at":      datetime.now(timezone.utc).isoformat(),
                "is_duplicate":     False,
                "is_valid":         True,
                "validation_errors": "[]",
            })
            stats.total_valid += 1

            # Flush batch when it hits the size limit
            if len(valid_batch) + len(invalid_batch) >= DB_BATCH_SIZE:
                _flush_batches(valid_batch, invalid_batch, db_conn, stats)
                consumer.commit(asynchronous=False)
                valid_batch.clear()
                invalid_batch.clear()
                last_flush = time.time()

            stats.log_if_due()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.exception(f"Fatal consumer error: {e}")
    finally:
        if valid_batch or invalid_batch:
            _flush_batches(valid_batch, invalid_batch, db_conn, stats)
        consumer.close()
        dlq_producer.flush(10)
        db_conn.close()
        logger.info(f"Consumer stopped. Total processed: {stats.total_received:,}")


def _flush_batches(valid_batch, invalid_batch, db_conn, stats):
    """Write pending batches to Postgres in a single transaction."""
    if not valid_batch and not invalid_batch:
        return

    try:
        with db_conn.cursor() as cur:
            # Mark as processed first (deduplication ledger)
            for row in valid_batch:
                if not row["is_duplicate"]:
                    mark_as_processed(row["event_id"], cur)

            insert_events_batch(valid_batch, cur)
            insert_dlq_batch(invalid_batch, cur)

            # Record throughput metric
            record_pipeline_metric(
                "events_processed",
                len(valid_batch) + len(invalid_batch),
                {"valid": len(valid_batch), "invalid": len(invalid_batch)},
                cur,
            )

        db_conn.commit()
    except Exception as e:
        logger.error(f"Batch flush error: {e}")
        db_conn.rollback()
        raise


if __name__ == "__main__":
    run()
