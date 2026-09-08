fastapi-notification-service/
├── app/
│   ├── __init__.py
│   ├── main.py              ← FastAPI app entrypoint
│   ├── config.py            ← Settings (Pydantic BaseSettings)
│   ├── database.py          ← SQLAlchemy async engine
│   ├── models/
│   │   ├── __init__.py
│   │   └── notification.py  ← SQLAlchemy ORM models
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── notification.py  ← Pydantic request/response schemas
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── notifications.py ← REST endpoints
│   │   └── health.py        ← Health check endpoint
│   ├── services/
│   │   ├── __init__.py
│   │   ├── notification_service.py
│   │   └── webhook_service.py  ← Slack webhook integration
│   └── kafka/
│       ├── __init__.py
│       └── consumer.py      ← Kafka consumer (async)
├── alembic/                 ← DB migrations
├── tests/
│   ├── test_api.py
│   └── test_services.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
