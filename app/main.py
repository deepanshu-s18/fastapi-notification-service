import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
from app.database import engine, Base
from app.routers import notifications
from app.metrics import router as metrics_router
from app.kafka.consumer import start_kafka_consumer

settings = get_settings()
logging.basicConfig(level=logging.DEBUG if settings.debug else logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: create tables + launch Kafka consumer. Shutdown: cancel consumer."""
    # Create DB tables (in production, use Alembic migrations)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialized")

    # Launch Kafka consumer as background task
    consumer_task = asyncio.create_task(start_kafka_consumer())
    logger.info("Kafka consumer task started")

    yield  # App is running

    # Shutdown: cancel consumer gracefully
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("Kafka consumer shut down cleanly")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="""
## Notification Microservice

Event-driven notification delivery system integrating:
- **Apache Kafka** consumer for async event processing
- **REST API** (OpenAPI 3.0) for direct notification dispatch
- **Slack webhooks** for team notifications
- **Generic HTTP webhooks** for system-to-system integration
- **PostgreSQL** persistence with retry logic

### Authentication
All endpoints require `X-API-Key` header.

### Kafka Integration
Publish events to the `notifications` topic:
```json
{
  "event_type": "user.registered",
  "source_service": "auth-service",
  "title": "New user registered",
  "message": "User deepanshu joined",
  "channel": "SLACK",
  "priority": "NORMAL"
}
```
""",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(notifications.router)
app.include_router(metrics_router)


@app.get("/health", tags=["Health"])
async def health():
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "kafka_topic": settings.kafka_topic_notifications,
    }
