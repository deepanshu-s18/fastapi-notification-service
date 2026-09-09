from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Any
from app.models.notification import NotificationChannel, NotificationStatus, NotificationPriority


class NotificationCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="Notification title")
    message: str = Field(..., min_length=1, description="Notification message body")
    channel: NotificationChannel = Field(..., description="Delivery channel: SLACK, EMAIL, WEBHOOK")
    priority: NotificationPriority = Field(NotificationPriority.NORMAL, description="Notification priority")
    recipient: str | None = Field(None, max_length=200, description="Slack channel, email address, or webhook URL")
    source_service: str | None = Field(None, max_length=100, description="Name of the originating service")
    event_type: str | None = Field(None, max_length=100, description="Event type e.g. user.registered")
    metadata: dict[str, Any] | None = Field(None, description="Optional event payload")

    @field_validator("recipient")
    @classmethod
    def validate_recipient(cls, v, info):
        return v


class NotificationResponse(BaseModel):
    id: int
    title: str
    message: str
    channel: NotificationChannel
    priority: NotificationPriority
    status: NotificationStatus
    recipient: str | None
    source_service: str | None
    event_type: str | None
    metadata: dict[str, Any] | None = Field(default=None, validation_alias="extra_data")
    retry_count: int
    error_message: str | None
    sent_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True, "populate_by_name": True}


class NotificationListResponse(BaseModel):
    items: list[NotificationResponse]
    total: int
    page: int
    page_size: int
    pages: int


class KafkaEvent(BaseModel):
    """Schema for Kafka-consumed notification events"""
    event_type: str
    source_service: str
    title: str
    message: str
    channel: NotificationChannel = NotificationChannel.SLACK
    priority: NotificationPriority = NotificationPriority.NORMAL
    recipient: str | None = None
    metadata: dict[str, Any] | None = None
