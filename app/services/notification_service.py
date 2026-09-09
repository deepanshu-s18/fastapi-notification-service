import logging
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.notification import Notification, NotificationChannel, NotificationStatus
from app.schemas.notification import NotificationCreate, NotificationListResponse, NotificationResponse
from app.services.webhook_service import WebhookService
from app.metrics import (
    notifications_total, retry_attempts_total, pending_notifications_gauge
)

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class NotificationService:

    @staticmethod
    async def create_and_send(
        db: AsyncSession,
        payload: NotificationCreate,
        kafka_topic: str | None = None,
        kafka_offset: int | None = None
    ) -> Notification:
        """Create a notification record and immediately attempt delivery.
        Idempotency: if kafka_topic+offset is provided and already processed,
        returns the existing notification without re-dispatching (prevents
        duplicate sends from Kafka at-least-once delivery).
        """
        # ── Idempotency check ─────────────────────────────────────────────────
        idempotency_key = None
        if kafka_topic is not None and kafka_offset is not None:
            idempotency_key = f"{kafka_topic}:{kafka_offset}"
            existing = await db.execute(
                select(Notification).where(Notification.idempotency_key == idempotency_key)
            )
            existing_notif = existing.scalar_one_or_none()
            if existing_notif:
                logger.info(
                    "Duplicate message detected (key=%s) — skipping re-dispatch", idempotency_key
                )
                return existing_notif

        notification = Notification(
            title=payload.title,
            message=payload.message,
            channel=payload.channel,
            priority=payload.priority,
            recipient=payload.recipient,
            source_service=payload.source_service,
            event_type=payload.event_type,
            extra_data=payload.metadata,
            kafka_topic=kafka_topic,
            kafka_offset=kafka_offset,
            idempotency_key=idempotency_key,
            status=NotificationStatus.PENDING,
        )
        db.add(notification)
        await db.flush()

        success = await NotificationService._dispatch(notification)

        if success:
            notification.status = NotificationStatus.SENT
            notification.sent_at = datetime.now(timezone.utc)
            logger.info("Notification #%d sent via %s", notification.id, notification.channel)
        else:
            notification.status = NotificationStatus.FAILED
            notification.retry_count += 1
            logger.warning("Notification #%d failed via %s", notification.id, notification.channel)

        notifications_total.labels(
            channel=notification.channel.value,
            status=notification.status.value
        ).inc()
        pending_notifications_gauge.dec()

        await db.commit()
        await db.refresh(notification)
        return notification

    @staticmethod
    async def retry_failed(db: AsyncSession) -> int:
        """Retry all FAILED notifications that haven't exceeded MAX_RETRIES."""
        result = await db.execute(
            select(Notification).where(
                Notification.status == NotificationStatus.FAILED,
                Notification.retry_count < MAX_RETRIES
            )
        )
        failed = result.scalars().all()
        retried = 0

        for notif in failed:
            notif.status = NotificationStatus.RETRYING
            success = await NotificationService._dispatch(notif)
            if success:
                notif.status = NotificationStatus.SENT
                notif.sent_at = datetime.now(timezone.utc)
            else:
                notif.status = NotificationStatus.FAILED
                notif.retry_count += 1
            retried += 1

        await db.commit()
        logger.info("Retried %d failed notifications", retried)
        return retried

    @staticmethod
    async def get_notifications(
        db: AsyncSession,
        page: int = 1,
        page_size: int = 20,
        status: NotificationStatus | None = None,
        channel: NotificationChannel | None = None,
    ) -> NotificationListResponse:
        query = select(Notification)
        count_query = select(func.count()).select_from(Notification)

        if status:
            query = query.where(Notification.status == status)
            count_query = count_query.where(Notification.status == status)
        if channel:
            query = query.where(Notification.channel == channel)
            count_query = count_query.where(Notification.channel == channel)

        total = (await db.execute(count_query)).scalar_one()
        offset = (page - 1) * page_size
        result = await db.execute(query.offset(offset).limit(page_size).order_by(Notification.created_at.desc()))
        items = result.scalars().all()

        return NotificationListResponse(
            items=[NotificationResponse.model_validate(n) for n in items],
            total=total,
            page=page,
            page_size=page_size,
            pages=(total + page_size - 1) // page_size,
        )

    @staticmethod
    async def _dispatch(notification: Notification) -> bool:
        """Route notification to the correct delivery channel."""
        try:
            if notification.channel == NotificationChannel.SLACK:
                return await WebhookService.send_slack(
                    title=notification.title,
                    message=notification.message,
                    priority=notification.priority.value,
                    recipient=notification.recipient,
                )
            elif notification.channel == NotificationChannel.WEBHOOK:
                if not notification.recipient:
                    logger.error("Webhook channel requires a recipient URL")
                    return False
                return await WebhookService.send_generic_webhook(
                    url=notification.recipient,
                    payload={
                        "title": notification.title,
                        "message": notification.message,
                        "event_type": notification.event_type,
                        "priority": notification.priority.value,
                        "metadata": notification.extra_data,
                    }
                )
            elif notification.channel == NotificationChannel.EMAIL:
                # Email integration placeholder
                logger.info("Email notification queued: '%s' → %s", notification.title, notification.recipient)
                return True
            return False
        except Exception as e:
            notification.error_message = str(e)
            logger.exception("Dispatch error for notification #%d: %s", notification.id, e)
            return False
