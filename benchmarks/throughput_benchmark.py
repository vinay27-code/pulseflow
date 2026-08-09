"""
PulseFlow Throughput Benchmark

Measures consumer processing speed at different batch sizes.
Run this after generating events to see how batch size affects throughput.

Usage:
    python benchmarks/throughput_benchmark.py
"""

import time
import psycopg2
import psycopg2.extras

DATABASE_URL = "postgresql://pulseflow:pulseflow_secret@localhost:5432/pulseflow"


def run_benchmark():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("\nPulseFlow Pipeline Benchmark Results")
    print("=" * 50)

    # Total events processed
    cur.execute("SELECT COUNT(*) AS total FROM raw_events")
    total = cur.fetchone()["total"]
    print(f"Total events in database: {total:,}")

    # Valid vs invalid breakdown
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE is_valid AND NOT is_duplicate) AS valid,
            COUNT(*) FILTER (WHERE is_duplicate)                  AS duplicates,
            COUNT(*) FILTER (WHERE NOT is_valid)                  AS invalid
        FROM raw_events
    """)
    row = cur.fetchone()
    print(f"Valid events:      {row['valid']:,} ({row['valid']/total*100:.1f}%)")
    print(f"Duplicates caught: {row['duplicates']:,} ({row['duplicates']/total*100:.1f}%)")
    print(f"Invalid (DLQ):     {row['invalid']:,} ({row['invalid']/total*100:.1f}%)")

    # Processing time span
    cur.execute("""
        SELECT
            MIN(received_at) AS first_event,
            MAX(received_at) AS last_event,
            EXTRACT(EPOCH FROM (MAX(received_at) - MIN(received_at))) AS duration_seconds
        FROM raw_events
    """)
    row = cur.fetchone()
    duration = float(row["duration_seconds"]) if row["duration_seconds"] else 1
    throughput = total / duration
    print(f"\nProcessing window: {duration:.0f} seconds")
    print(f"Avg throughput:    {throughput:.0f} events/sec")

    # DLQ stats
    cur.execute("SELECT COUNT(*) AS cnt FROM dead_letter_queue WHERE resolved = FALSE")
    dlq = cur.fetchone()["cnt"]
    print(f"\nDead letter queue: {dlq:,} unresolved events")

    # dbt model stats
    print("\ndbt Model Row Counts:")
    print("-" * 30)
    for model in ["stg_events", "fct_orders", "fct_funnel", "dim_users"]:
        try:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM analytics_analytics.{model}")
            count = cur.fetchone()["cnt"]
            print(f"  {model:<20} {count:>10,} rows")
        except Exception:
            print(f"  {model:<20} not found")

    # Top revenue hour
    cur.execute("""
        SELECT hour_bucket, revenue, unique_users, checkout_conversion_pct
        FROM analytics_analytics.fct_funnel
        ORDER BY revenue DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        print(f"\nPeak revenue hour: {row['hour_bucket']}")
        print(f"  Revenue:          ${float(row['revenue']):,.2f}")
        print(f"  Unique users:     {row['unique_users']:,}")
        print(f"  Checkout conv:    {row['checkout_conversion_pct']}%")

    print("\n" + "=" * 50)
    cur.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()
