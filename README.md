# ⚡ Notification Microservice

> Event-driven notification microservice built with FastAPI + Apache Kafka. Processes system events asynchronously and delivers notifications via Slack and HTTP webhooks — with circuit breaker, dead letter queue, and Prometheus observability.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C?logo=prometheus)](https://prometheus.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)

---

## 📐 Architecture

```
                     ┌─────────────────────┐
  Source Services ──►│   Kafka Producer    │
  (HR Portal, etc)   │ topic: notifications │
                     └──────────┬──────────┘
                                │
                     ┌──────────▼──────────┐
                     │   Kafka Consumer    │◄─── async event loop
                     │  (aiokafka worker)  │
                     │  ┌───────────────┐  │
                     │  │ Retry 1/2/3   │  │
                     │  │  ↓ on fail    │  │
                     │  │ DLQ topic     │  │
                     │  └───────────────┘  │
                     └──────────┬──────────┘
                                │ idempotency check
                     ┌──────────▼──────────┐
                     │ NotificationService  │
                     │  ┌───────────────┐  │
                     │  │ Circuit Breaker│  │
                     │  │ CLOSED→OPEN→  │  │
                     │  │ HALF_OPEN     │  │
                     │  └───────────────┘  │
                     └──────┬───────┬──────┘
                            │       │
              ┌─────────────┘       └──────────────┐
    ┌─────────▼──────┐              ┌──────────────▼──┐
    │  Slack Webhook │              │  HTTP Webhook   │
    └────────────────┘              └─────────────────┘
                            │
                  ┌─────────▼─────────┐
                  │    PostgreSQL     │  ← notification log + audit trail
                  │  (idempotency_key │
                  │   unique index)   │
                  └───────────────────┘
                            │
                  ┌─────────▼─────────┐
                  │  /metrics (Prom.) │  ← scraped by Prometheus/Grafana
                  └───────────────────┘
```

---

## ✨ Key Features

### 🔄 Kafka Consumer with DLQ
- Consumes from `notifications` topic (configurable)
- **3 retry attempts** per message with linear backoff
- On exhaustion → published to `notifications.dlq` for manual inspection/replay
- Exponential backoff reconnection on broker failure

### ⚡ Circuit Breaker
Three-state machine protecting outbound webhook calls:

| State | Behaviour |
|---|---|
| `CLOSED` | Requests flow normally |
| `OPEN` | Fail-fast — no network call, returns error immediately |
| `HALF_OPEN` | Allows one trial request to test recovery |

Configurable: `failure_threshold=5`, `recovery_timeout=60s`, `success_threshold=2`

### 🔑 Message Deduplication
- `idempotency_key = "{topic}:{offset}"` stored with `UNIQUE` constraint
- Duplicate Kafka messages (from at-least-once delivery) are silently skipped

### 📊 Prometheus Metrics (`/metrics`)
| Metric | Type | Description |
|---|---|---|
| `notifications_total` | Counter | By channel + status |
| `kafka_messages_consumed_total` | Counter | By topic |
| `kafka_messages_failed_total` | Counter | Failed processing |
| `dlq_messages_total` | Counter | DLQ routes by reason |
| `webhook_requests_total` | Counter | Outbound requests by channel + success |
| `notification_dispatch_duration_seconds` | Histogram | Dispatch latency |
| `pending_notifications` | Gauge | Current pending count |
| `circuit_breaker_open` | Gauge | 1=OPEN, 0=CLOSED per channel |

### 📬 Notification Channels
- **Slack** — Incoming Webhook with priority-based colour coding + emoji
- **Webhook** — Generic HTTP POST to any endpoint
- **Email** — (interface ready, SMTP implementation pluggable)

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Framework | FastAPI 0.111 + Uvicorn (async ASGI) |
| Message Broker | Apache Kafka 3.7 (aiokafka consumer) |
| Database | PostgreSQL 16 + SQLAlchemy 2.0 (async) + Alembic |
| Validation | Pydantic v2 |
| Observability | prometheus-client — `/metrics` endpoint |
| Resilience | Custom async Circuit Breaker (CLOSED/OPEN/HALF_OPEN) |
| HTTP Client | aiohttp (async outbound webhook calls) |
| Testing | pytest + pytest-asyncio |
| Infrastructure | Docker + docker-compose |

---

## 📁 Project Structure

```
fastapi-notification-service/
├── app/
│   ├── main.py                  # FastAPI app, lifespan (DB init + Kafka worker)
│   ├── config.py                # Pydantic Settings (env vars)
│   ├── database.py              # Async SQLAlchemy engine + session factory
│   ├── metrics.py               # Prometheus counters/histograms/gauges + /metrics route
│   ├── circuit_breaker.py       # Async circuit breaker state machine
│   ├── kafka/
│   │   └── consumer.py          # Kafka consumer + DLQ handler
│   ├── models/
│   │   └── notification.py      # SQLAlchemy Notification model (idempotency_key)
│   ├── schemas/
│   │   └── notification.py      # Pydantic request/response schemas
│   ├── routers/
│   │   └── notifications.py     # REST API routes
│   └── services/
│       ├── notification_service.py  # Core business logic + idempotency check
│       └── webhook_service.py       # Slack + HTTP webhook delivery
├── tests/
│   └── test_api.py              # pytest async test suite
├── requirements.txt
├── docker-compose.yml           # FastAPI + PostgreSQL + Kafka + Zookeeper
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for local dev)

### Option A — Docker (Recommended)

```bash
git clone https://github.com/deepanshu-s18/fastapi-notification-service.git
cd fastapi-notification-service
docker-compose up --build
```

| Service | URL |
|---|---|
| FastAPI | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Prometheus Metrics | http://localhost:8000/metrics |
| Kafka | localhost:9092 |

### Option B — Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy and configure env
cp .env.example .env

# Start only infrastructure
docker-compose up postgres kafka -d

# Run FastAPI
uvicorn app.main:app --reload --port 8000
```

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/notifications` | Create + dispatch a notification |
| `GET` | `/api/v1/notifications` | List notifications (paginated, filterable) |
| `GET` | `/api/v1/notifications/{id}` | Get single notification |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/health` | Health check |

### Example Request

```bash
curl -X POST http://localhost:8000/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Leave Approved",
    "message": "Your annual leave (Sep 10–12) has been approved by your manager.",
    "channel": "SLACK",
    "priority": "NORMAL",
    "recipient": "#hr-notifications",
    "source_service": "hr-leave-portal",
    "event_type": "LEAVE_APPROVED"
  }'
```

### Example Response

```json
{
  "id": 42,
  "title": "Leave Approved",
  "channel": "SLACK",
  "priority": "NORMAL",
  "status": "SENT",
  "idempotency_key": null,
  "retry_count": 0,
  "sent_at": "2026-09-08T21:00:00Z",
  "created_at": "2026-09-08T21:00:00Z"
}
```

---

## 📨 Kafka Integration

### Produce a notification event

```bash
# Using kafka-console-producer
docker exec -it fastapi_kafka kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic notifications

# Paste this JSON:
{
  "title": "System Alert",
  "message": "Deployment completed successfully",
  "channel": "SLACK",
  "priority": "HIGH",
  "source_service": "ci-pipeline",
  "event_type": "DEPLOYMENT_SUCCESS"
}
```

### Dead Letter Queue inspection

```bash
docker exec -it fastapi_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic notifications.dlq \
  --from-beginning
```

---

## ⚙️ Configuration

```env
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/notifications

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_NOTIFICATIONS=notifications
KAFKA_CONSUMER_GROUP=notification-service-group

# Slack
SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK
```

---

## 🧪 Testing

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=app
```

---

## 📄 License

MIT — built for Google Application Engineering Intern application.
