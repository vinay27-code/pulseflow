"""
PulseFlow Event Generator

Produces synthetic e-commerce events at configurable throughput.
Intentionally injects edge cases so the consumer has real problems to handle:
  - ~2% duplicate events (same event_id sent twice)
  - ~1% malformed events (missing required fields, invalid types)
  - ~0.5% late-arriving events (timestamps 25-48 hours in the past)

This mirrors what you'd see in a real system where:
  - Mobile clients retry on network failure (duplicates)
  - Clients with bugs send malformed data (malformed events)
  - Offline devices sync when reconnected (late-arriving events)
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GENERATOR] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC", "ecommerce-events")
EVENTS_PER_SECOND        = int(os.getenv("EVENTS_PER_SECOND", "500"))
TOTAL_EVENTS             = int(os.getenv("TOTAL_EVENTS", "1000000"))

# Realistic event type distribution - product views are most common,
# payment success is rare (conversion funnel)
EVENT_TYPE_WEIGHTS = {
    "page_view":        20,
    "product_view":     30,
    "search":           15,
    "add_to_cart":      12,
    "remove_from_cart":  4,
    "checkout":          8,
    "payment_success":   5,
    "payment_failure":   2,
    "order_created":     2,
    "order_cancelled":   1,
    "user_login":        1,
}

EVENT_TYPES = list(EVENT_TYPE_WEIGHTS.keys())
WEIGHTS     = list(EVENT_TYPE_WEIGHTS.values())

# Simulated product catalog and user pool
PRODUCT_IDS = [f"prod_{i:05d}" for i in range(1, 5001)]
USER_IDS    = [f"user_{i:07d}" for i in range(1, 50001)]
CURRENCIES  = ["USD", "USD", "USD", "USD", "EUR", "GBP", "CAD"]  # USD is most common


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks":               "all",       # wait for all replicas to ack
        "retries":            5,
        "retry.backoff.ms":   200,
        "compression.type":   "snappy",
        "linger.ms":          5,           # micro-batch for throughput
        "batch.size":         65536,       # 64KB batches
    })


def ensure_topic_exists() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics
    if KAFKA_TOPIC not in existing:
        logger.info(f"Creating topic: {KAFKA_TOPIC}")
        futures = admin.create_topics([
            NewTopic(KAFKA_TOPIC, num_partitions=6, replication_factor=1)
        ])
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic created: {topic}")
            except Exception as e:
                logger.warning(f"Topic creation warning (may already exist): {e}")


def generate_event(event_type: str) -> dict:
    now = datetime.now(timezone.utc)
    user_id    = random.choice(USER_IDS)
    session_id = f"sess_{uuid.uuid4().hex[:16]}"
    event = {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "user_id":         user_id,
        "session_id":      session_id,
        "event_timestamp": now.isoformat(),
        "properties":      {},
    }

    # Add fields based on event type
    if event_type in ("product_view", "add_to_cart", "remove_from_cart"):
        event["product_id"] = random.choice(PRODUCT_IDS)
        event["properties"]["price"] = round(random.uniform(5.99, 999.99), 2)
        event["properties"]["category"] = random.choice([
            "electronics", "clothing", "books", "home", "sports", "beauty"
        ])

    if event_type == "search":
        event["properties"]["query"]        = random.choice([
            "laptop", "shoes", "headphones", "coffee maker", "yoga mat",
            "bluetooth speaker", "running shoes", "desk lamp"
        ])
        event["properties"]["results_count"] = random.randint(0, 500)

    if event_type in ("checkout", "payment_success", "payment_failure", "order_created", "order_cancelled"):
        event["order_id"] = f"ord_{uuid.uuid4().hex[:12].upper()}"
        event["amount"]   = str(round(random.uniform(10.00, 2500.00), 2))
        event["currency"] = random.choice(CURRENCIES)
        event["properties"]["item_count"] = random.randint(1, 8)

    if event_type == "payment_failure":
        event["properties"]["failure_reason"] = random.choice([
            "insufficient_funds", "card_declined", "expired_card",
            "invalid_cvv", "fraud_suspected"
        ])

    return event


def inject_duplicate(event: dict) -> dict:
    """Return the same event with the same event_id - consumer must detect this."""
    return dict(event)


def inject_malformed(event_type: str) -> dict:
    """
    Intentionally broken events. The consumer must handle these gracefully
    and route them to the dead letter queue.
    """
    malformed_variants = [
        # Missing required field
        {"event_type": event_type, "user_id": "user_broken"},
        # Wrong type for amount
        {"event_id": str(uuid.uuid4()), "event_type": "payment_success",
         "order_id": "ord_MALFORMED", "amount": "not_a_number",
         "event_timestamp": datetime.now(timezone.utc).isoformat()},
        # Negative amount
        {"event_id": str(uuid.uuid4()), "event_type": "payment_success",
         "order_id": "ord_NEGATIVE", "amount": "-500.00",
         "event_timestamp": datetime.now(timezone.utc).isoformat()},
        # Missing order_id on financial event
        {"event_id": str(uuid.uuid4()), "event_type": "order_created",
         "user_id": "user_missing_order",
         "event_timestamp": datetime.now(timezone.utc).isoformat()},
        # Completely empty
        {},
    ]
    return random.choice(malformed_variants)


def inject_late_arriving(event_type: str) -> dict:
    """Events timestamped 25-72 hours ago - simulates offline device syncing."""
    event = generate_event(event_type)
    hours_late = random.uniform(25, 72)
    late_time  = datetime.now(timezone.utc) - timedelta(hours=hours_late)
    event["event_timestamp"] = late_time.isoformat()
    event["properties"]["_simulated_late"] = True
    return event


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")


def run():
    logger.info(f"Starting PulseFlow Event Generator")
    logger.info(f"Target: {TOTAL_EVENTS:,} events at {EVENTS_PER_SECOND}/sec")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> {KAFKA_TOPIC}")

    # Wait for Kafka to be ready
    for attempt in range(30):
        try:
            ensure_topic_exists()
            break
        except Exception as e:
            logger.warning(f"Kafka not ready yet (attempt {attempt + 1}/30): {e}")
            time.sleep(2)
    else:
        logger.error("Could not connect to Kafka after 30 attempts. Exiting.")
        sys.exit(1)

    producer   = make_producer()
    sleep_time = 1.0 / EVENTS_PER_SECOND

    stats = {
        "sent":       0,
        "duplicates": 0,
        "malformed":  0,
        "late":       0,
        "errors":     0,
    }

    start_time         = time.time()
    last_log_time      = start_time
    last_log_count     = 0
    recent_event_cache = []  # small buffer for generating duplicates

    logger.info("Generating events...")

    while stats["sent"] < TOTAL_EVENTS:
        event_type = random.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0]
        roll       = random.random()

        if roll < 0.02 and recent_event_cache:
            # 2% chance: send a duplicate of a recent event
            payload = inject_duplicate(random.choice(recent_event_cache))
            stats["duplicates"] += 1
        elif roll < 0.03:
            # 1% chance: send a malformed event
            payload = inject_malformed(event_type)
            stats["malformed"] += 1
        elif roll < 0.035:
            # 0.5% chance: send a late-arriving event
            payload = inject_late_arriving(event_type)
            stats["late"] += 1
        else:
            payload = generate_event(event_type)

        # Keep a small cache of recent events for duplicate injection
        recent_event_cache.append(payload)
        if len(recent_event_cache) > 100:
            recent_event_cache.pop(0)

        try:
            producer.produce(
                KAFKA_TOPIC,
                key=payload.get("user_id", "unknown").encode(),
                value=json.dumps(payload, default=str).encode(),
                callback=delivery_callback,
            )
            stats["sent"] += 1
        except Exception as e:
            logger.error(f"Produce error: {e}")
            stats["errors"] += 1

        # Poll to trigger delivery callbacks without blocking
        producer.poll(0)

        # Rate limiting
        time.sleep(sleep_time)

        # Log throughput every 10 seconds
        now = time.time()
        if now - last_log_time >= 10:
            elapsed      = now - start_time
            recent_count = stats["sent"] - last_log_count
            current_rps  = recent_count / (now - last_log_time)
            overall_rps  = stats["sent"] / elapsed
            pct_done     = (stats["sent"] / TOTAL_EVENTS) * 100

            logger.info(
                f"Progress: {stats['sent']:>10,}/{TOTAL_EVENTS:,} ({pct_done:.1f}%) | "
                f"Current: {current_rps:.0f}/s | Overall: {overall_rps:.0f}/s | "
                f"Dupes: {stats['duplicates']:,} | Malformed: {stats['malformed']:,} | "
                f"Late: {stats['late']:,}"
            )
            last_log_time  = now
            last_log_count = stats["sent"]

    # Flush remaining messages
    logger.info("Flushing remaining messages...")
    producer.flush(timeout=30)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Generation complete in {elapsed:.1f}s")
    logger.info(f"Total sent:      {stats['sent']:,}")
    logger.info(f"Duplicates:      {stats['duplicates']:,} ({stats['duplicates']/stats['sent']*100:.1f}%)")
    logger.info(f"Malformed:       {stats['malformed']:,} ({stats['malformed']/stats['sent']*100:.1f}%)")
    logger.info(f"Late-arriving:   {stats['late']:,} ({stats['late']/stats['sent']*100:.1f}%)")
    logger.info(f"Avg throughput:  {stats['sent']/elapsed:.0f} events/sec")
    logger.info("=" * 60)


if __name__ == "__main__":
    run()
