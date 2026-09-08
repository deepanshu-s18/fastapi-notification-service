import asyncio
import json
import logging
import time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from app.config import get_settings
from app.database import AsyncSessionLocal
from app.schemas.notification import NotificationCreate, KafkaEvent
from app.services.notification_service import NotificationService
from app.metrics import (
    kafka_messages_consumed, kafka_messages_failed,
    dlq_messages_total, kafka_process_duration_seconds
)

logger = logging.getLogger(__name__)
settings = get_settings()

DLQ_TOPIC = "notifications.dlq"
MAX_PROCESSING_ATTEMPTS = 3


async def start_kafka_consumer() -> None:
    """
    Async Kafka consumer that processes notification events.
    Features:
    - Exponential backoff reconnection
    - Per-message error isolation (one bad message never kills the consumer)
    - Dead Letter Queue: messages that fail MAX_PROCESSING_ATTEMPTS times are
      routed to `notifications.dlq` for manual inspection / replay
    - Prometheus metrics for consumed/failed/DLQ messages and processing latency
    """
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_notifications,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
    )

    retry_delay = 5
    while True:
        try:
            await consumer.start()
            logger.info(
                "Kafka consumer started — topic: '%s', group: '%s'",
                settings.kafka_topic_notifications,
                settings.kafka_consumer_group,
            )

            async for msg in consumer:
                await _process_with_dlq(msg)

        except KafkaConnectionError as e:
            logger.error("Kafka connection error: %s — retrying in %ds", e, retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # exponential backoff, cap 60s
        except asyncio.CancelledError:
            logger.info("Kafka consumer cancelled — shutting down gracefully")
            break
        except Exception as e:
            logger.exception("Unexpected Kafka consumer error: %s", e)
            await asyncio.sleep(retry_delay)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


async def _process_with_dlq(msg) -> None:
    """
    Process one Kafka message with DLQ fallback.
    If the message fails after MAX_PROCESSING_ATTEMPTS,
    it is published to the DLQ topic for manual inspection.
    """
    start = time.monotonic()
    kafka_messages_consumed.labels(topic=msg.topic).inc()

    for attempt in range(1, MAX_PROCESSING_ATTEMPTS + 1):
        try:
            await _process_kafka_message(msg)
            kafka_process_duration_seconds.labels(topic=msg.topic).observe(
                time.monotonic() - start
            )
            return
        except Exception as e:
            logger.warning(
                "Message processing attempt %d/%d failed at offset %d: %s",
                attempt, MAX_PROCESSING_ATTEMPTS, msg.offset, e
            )
            if attempt < MAX_PROCESSING_ATTEMPTS:
                await asyncio.sleep(0.5 * attempt)  # brief backoff between retries

    # All attempts exhausted → route to DLQ
    kafka_messages_failed.labels(topic=msg.topic).inc()
    dlq_messages_total.labels(topic=msg.topic, reason="max_retries_exceeded").inc()
    await _send_to_dlq(msg, reason="max_retries_exceeded")


async def _process_kafka_message(msg) -> None:
    """Deserialize → validate → persist → dispatch one Kafka message."""
    logger.debug(
        "Processing Kafka message — topic: %s, partition: %d, offset: %d",
        msg.topic, msg.partition, msg.offset
    )
    event = KafkaEvent(**msg.value)
    payload = NotificationCreate(
        title=event.title,
        message=event.message,
        channel=event.channel,
        priority=event.priority,
        recipient=event.recipient,
        source_service=event.source_service,
        event_type=event.event_type,
        metadata=event.metadata,
    )
    async with AsyncSessionLocal() as db:
        notification = await NotificationService.create_and_send(
            db=db,
            payload=payload,
            kafka_topic=msg.topic,
            kafka_offset=msg.offset,
        )
        logger.info(
            "Processed Kafka event '%s' from '%s' → notification #%d [%s]",
            event.event_type, event.source_service, notification.id, notification.status
        )


async def _send_to_dlq(msg, reason: str) -> None:
    """Publish a failed message to the Dead Letter Queue topic."""
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()
        try:
            dlq_payload = {
                "original_topic": msg.topic,
                "original_partition": msg.partition,
                "original_offset": msg.offset,
                "reason": reason,
                "value": msg.value,
            }
            await producer.send_and_wait(DLQ_TOPIC, value=dlq_payload)
            logger.error(
                "Message offset=%d routed to DLQ topic '%s' [reason: %s]",
                msg.offset, DLQ_TOPIC, reason
            )
        finally:
            await producer.stop()
    except Exception as e:
        logger.critical("Failed to send message to DLQ: %s", e)
