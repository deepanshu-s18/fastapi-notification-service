import logging
from fastapi import APIRouter, Depends, Query, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.config import get_settings
from app.schemas.notification import (
    NotificationCreate, NotificationResponse, NotificationListResponse
)
from app.models.notification import NotificationStatus, NotificationChannel
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)
settings = get_settings()
router = APIRouter(prefix="/api/v1/notifications", tags=["Notifications"])

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.api_key:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")
    return api_key


@router.post(
    "/",
    response_model=NotificationResponse,
    status_code=201,
    summary="Send a notification",
    description="Create and immediately dispatch a notification via Slack, webhook, or email.",
)
async def send_notification(
    payload: NotificationCreate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    notification = await NotificationService.create_and_send(db, payload)
    return notification


@router.get(
    "/",
    response_model=NotificationListResponse,
    summary="List notifications",
    description="Paginated list of all notifications with optional status/channel filters.",
)
async def list_notifications(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status: NotificationStatus | None = Query(None, description="Filter by status"),
    channel: NotificationChannel | None = Query(None, description="Filter by channel"),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    return await NotificationService.get_notifications(db, page, page_size, status, channel)


@router.post(
    "/retry",
    summary="Retry failed notifications",
    description="Re-attempt delivery for all FAILED notifications under the retry limit.",
)
async def retry_failed(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    count = await NotificationService.retry_failed(db)
    return {"message": f"Retried {count} failed notifications"}
