import enum
from sqlalchemy import String, Text, Enum, JSON, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base
from datetime import datetime


class NotificationChannel(str, enum.Enum):
    SLACK = "SLACK"
    EMAIL = "EMAIL"
    WEBHOOK = "WEBHOOK"


class NotificationStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class NotificationPriority(str, enum.Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[NotificationChannel] = mapped_column(
        Enum(NotificationChannel), nullable=False, index=True
    )
    priority: Mapped[NotificationPriority] = mapped_column(
        Enum(NotificationPriority), default=NotificationPriority.NORMAL, index=True
    )
    status: Mapped[NotificationStatus] = mapped_column(
        Enum(NotificationStatus), default=NotificationStatus.PENDING, index=True
    )
    recipient: Mapped[str | None] = mapped_column(String(200))
    source_service: Mapped[str | None] = mapped_column(String(100))  # which service sent this event
    event_type: Mapped[str | None] = mapped_column(String(100))      # e.g. "user.registered", "order.completed"
    metadata: Mapped[dict | None] = mapped_column(JSON)              # arbitrary event payload
    kafka_topic: Mapped[str | None] = mapped_column(String(200))
    kafka_offset: Mapped[int | None] = mapped_column()
    # Idempotency key — prevents duplicate notifications from Kafka at-least-once delivery.
    # Format: "<topic>:<partition>:<offset>" or caller-supplied UUID
    idempotency_key: Mapped[str | None] = mapped_column(String(300), unique=True, index=True)
    retry_count: Mapped[int] = mapped_column(default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
