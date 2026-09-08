"""
Metrics module — exposes Prometheus metrics for the Notification Microservice.
Tracks: notification counts by channel/status, dispatch latency, Kafka lag.
"""
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter(tags=["Metrics"])

# ── Counters ─────────────────────────────────────────────────────────────────
notifications_total = Counter(
    "notifications_total",
    "Total notifications processed, partitioned by channel and status",
    ["channel", "status"]
)

kafka_messages_consumed = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic"]
)

kafka_messages_failed = Counter(
    "kafka_messages_failed_total",
    "Total Kafka messages that failed processing",
    ["topic"]
)

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Total messages routed to Dead Letter Queue",
    ["topic", "reason"]
)

webhook_requests_total = Counter(
    "webhook_requests_total",
    "Total outbound webhook requests",
    ["channel", "success"]
)

retry_attempts_total = Counter(
    "retry_attempts_total",
    "Total notification retry attempts",
    ["channel"]
)

# ── Histograms ────────────────────────────────────────────────────────────────
dispatch_duration_seconds = Histogram(
    "notification_dispatch_duration_seconds",
    "Time taken to dispatch a notification",
    ["channel"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

kafka_process_duration_seconds = Histogram(
    "kafka_message_process_duration_seconds",
    "Time taken to process a single Kafka message",
    ["topic"]
)

# ── Gauges ────────────────────────────────────────────────────────────────────
pending_notifications_gauge = Gauge(
    "pending_notifications",
    "Current count of PENDING notifications"
)

circuit_breaker_state = Gauge(
    "circuit_breaker_open",
    "Whether the circuit breaker is open (1=open, 0=closed)",
    ["channel"]
)


@router.get("/metrics", summary="Prometheus metrics endpoint")
async def metrics():
    """Exposes Prometheus-format metrics for scraping."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
