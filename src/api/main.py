"""
PulseFlow API

Exposes processed event data and pipeline health metrics.
Designed to be both a functional API and a demo surface for interviews -
every endpoint tells a story about what the pipeline is doing.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pulseflow:pulseflow_secret@localhost:5432/pulseflow"
)

app = FastAPI(
    title="PulseFlow API",
    description="Real-Time Data Intelligence Platform - Event Analytics API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


# --- Response models ---

class PipelineHealth(BaseModel):
    status: str
    total_events: int
    events_last_hour: int
    valid_rate_pct: float
    duplicate_rate_pct: float
    dlq_unresolved: int
    unique_users_last_hour: int
    db_connected: bool


class EventSummary(BaseModel):
    event_type: str
    count: int
    unique_users: int
    total_amount: float
    duplicate_count: int
    invalid_count: int


class FunnelStep(BaseModel):
    step: str
    count: int
    conversion_pct: Optional[float] = None


class DLQItem(BaseModel):
    id: int
    event_id: Optional[str]
    failure_reason: str
    retry_count: int
    first_failed_at: datetime
    raw_payload_preview: str


# --- Endpoints ---

@app.get("/health", response_model=PipelineHealth, tags=["Pipeline"])
def health_check():
    """
    Pipeline health check. Returns processing stats for the last hour.
    This is what you'd show in an interview to demonstrate the pipeline is live.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception:
        return PipelineHealth(
            status="degraded",
            total_events=0,
            events_last_hour=0,
            valid_rate_pct=0,
            duplicate_rate_pct=0,
            dlq_unresolved=0,
            unique_users_last_hour=0,
            db_connected=False,
        )

    with conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total FROM raw_events")
        total = cur.fetchone()["total"]

        cur.execute("""
            SELECT
                COUNT(*)                                            AS total,
                COUNT(*) FILTER (WHERE is_valid AND NOT is_duplicate) AS valid,
                COUNT(*) FILTER (WHERE is_duplicate)               AS dupes,
                COUNT(DISTINCT user_id)                            AS unique_users
            FROM raw_events
            WHERE received_at >= NOW() - INTERVAL '1 hour'
        """)
        row = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS cnt FROM dead_letter_queue WHERE resolved = FALSE")
        dlq_count = cur.fetchone()["cnt"]

    hour_total = row["total"] or 1
    conn.close()

    return PipelineHealth(
        status="healthy" if dlq_count < 1000 else "degraded",
        total_events=total,
        events_last_hour=row["total"],
        valid_rate_pct=round(row["valid"] / hour_total * 100, 2),
        duplicate_rate_pct=round(row["dupes"] / hour_total * 100, 2),
        dlq_unresolved=dlq_count,
        unique_users_last_hour=row["unique_users"],
        db_connected=True,
    )


@app.get("/events/summary", response_model=list[EventSummary], tags=["Events"])
def event_summary(
    hours: int = Query(default=1, ge=1, le=168, description="Lookback window in hours")
):
    """
    Breakdown of events by type for the given time window.
    Shows counts, unique users, revenue, and data quality metrics.
    """
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                event_type,
                COUNT(*)                                                AS count,
                COUNT(DISTINCT user_id)                                 AS unique_users,
                COALESCE(SUM(amount), 0)                                AS total_amount,
                COUNT(*) FILTER (WHERE is_duplicate)                    AS duplicate_count,
                COUNT(*) FILTER (WHERE NOT is_valid)                    AS invalid_count
            FROM raw_events
            WHERE received_at >= NOW() - INTERVAL '%s hours'
            GROUP BY event_type
            ORDER BY count DESC
        """, (hours,))
        rows = cur.fetchall()
    conn.close()

    return [
        EventSummary(
            event_type=r["event_type"],
            count=r["count"],
            unique_users=r["unique_users"],
            total_amount=float(r["total_amount"]),
            duplicate_count=r["duplicate_count"],
            invalid_count=r["invalid_count"],
        )
        for r in rows
    ]


@app.get("/events/funnel", response_model=list[FunnelStep], tags=["Analytics"])
def conversion_funnel(
    hours: int = Query(default=24, ge=1, le=168)
):
    """
    E-commerce conversion funnel: from product view to order creation.
    Classic analytics query that demonstrates why you build a pipeline like this.
    """
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'product_view')    AS product_views,
                COUNT(*) FILTER (WHERE event_type = 'add_to_cart')     AS add_to_carts,
                COUNT(*) FILTER (WHERE event_type = 'checkout')        AS checkouts,
                COUNT(*) FILTER (WHERE event_type = 'payment_success') AS payments,
                COUNT(*) FILTER (WHERE event_type = 'order_created')   AS orders
            FROM raw_events
            WHERE received_at >= NOW() - INTERVAL '%s hours'
              AND is_valid = TRUE
              AND is_duplicate = FALSE
        """, (hours,))
        r = cur.fetchone()
    conn.close()

    views    = r["product_views"] or 0
    carts    = r["add_to_carts"] or 0
    checkout = r["checkouts"] or 0
    payments = r["payments"] or 0
    orders   = r["orders"] or 0

    def pct(numerator, denominator):
        return round(numerator / denominator * 100, 1) if denominator > 0 else None

    return [
        FunnelStep(step="Product Views",   count=views,    conversion_pct=None),
        FunnelStep(step="Add to Cart",     count=carts,    conversion_pct=pct(carts, views)),
        FunnelStep(step="Checkout",        count=checkout, conversion_pct=pct(checkout, carts)),
        FunnelStep(step="Payment Success", count=payments, conversion_pct=pct(payments, checkout)),
        FunnelStep(step="Order Created",   count=orders,   conversion_pct=pct(orders, payments)),
    ]


@app.get("/events/timeseries", tags=["Analytics"])
def event_timeseries(
    event_type: Optional[str] = Query(default=None),
    hours:      int           = Query(default=6, ge=1, le=48),
    interval:   str           = Query(default="5 minutes", description="e.g. '5 minutes', '1 hour'")
):
    """
    Time series of event counts. Useful for spotting traffic spikes and anomalies.
    """
    valid_intervals = {"1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"}
    if interval not in valid_intervals:
        raise HTTPException(400, f"interval must be one of: {valid_intervals}")

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn, conn.cursor() as cur:
        if event_type:
            cur.execute("""
                SELECT
                    DATE_TRUNC('minute', event_timestamp) -
                        (EXTRACT(MINUTE FROM event_timestamp)::INT %% %s) * INTERVAL '1 minute' AS bucket,
                    COUNT(*) AS count
                FROM raw_events
                WHERE received_at >= NOW() - INTERVAL '%s hours'
                  AND event_type = %s
                  AND is_valid = TRUE
                GROUP BY 1
                ORDER BY 1
            """, (5, hours, event_type))
        else:
            cur.execute("""
                SELECT
                    DATE_TRUNC('minute', event_timestamp) -
                        (EXTRACT(MINUTE FROM event_timestamp)::INT %% 5) * INTERVAL '1 minute' AS bucket,
                    event_type,
                    COUNT(*) AS count
                FROM raw_events
                WHERE received_at >= NOW() - INTERVAL '%s hours'
                  AND is_valid = TRUE
                GROUP BY 1, 2
                ORDER BY 1, 2
            """, (hours,))
        rows = cur.fetchall()
    conn.close()

    return [dict(r) for r in rows]


@app.get("/dlq", response_model=list[DLQItem], tags=["Operations"])
def list_dlq(
    limit:    int  = Query(default=20, ge=1, le=100),
    resolved: bool = Query(default=False)
):
    """
    Dead letter queue contents. These are events that failed validation.
    In a production system you'd have a UI here for operations teams to
    review and either fix-and-replay or discard failed events.
    """
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, event_id, failure_reason, retry_count, first_failed_at, raw_payload
            FROM dead_letter_queue
            WHERE resolved = %s
            ORDER BY first_failed_at DESC
            LIMIT %s
        """, (resolved, limit))
        rows = cur.fetchall()
    conn.close()

    return [
        DLQItem(
            id=r["id"],
            event_id=str(r["event_id"]) if r["event_id"] else None,
            failure_reason=r["failure_reason"],
            retry_count=r["retry_count"],
            first_failed_at=r["first_failed_at"],
            raw_payload_preview=json.dumps(r["raw_payload"])[:200],
        )
        for r in rows
    ]


@app.get("/metrics/pipeline", tags=["Pipeline"])
def pipeline_metrics(hours: int = Query(default=1, ge=1, le=24)):
    """
    Internal pipeline performance metrics: throughput over time.
    """
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                DATE_TRUNC('minute', recorded_at) AS minute,
                metric_name,
                SUM(metric_value) AS total_value,
                COUNT(*) AS sample_count
            FROM pipeline_metrics
            WHERE recorded_at >= NOW() - INTERVAL '%s hours'
            GROUP BY 1, 2
            ORDER BY 1 DESC, 2
        """, (hours,))
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/", tags=["Meta"])
def root():
    return {
        "service":     "PulseFlow API",
        "version":     "1.0.0",
        "description": "Real-Time Data Intelligence Platform",
        "docs":        "/docs",
        "endpoints": {
            "health":      "GET /health",
            "summary":     "GET /events/summary",
            "funnel":      "GET /events/funnel",
            "timeseries":  "GET /events/timeseries",
            "dlq":         "GET /dlq",
            "metrics":     "GET /metrics/pipeline",
        }
    }
