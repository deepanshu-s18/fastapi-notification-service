import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.main import app
from app.database import get_db, Base
from app.config import get_settings

settings = get_settings()
TEST_DB_URL = "sqlite+aiosqlite:///./test.db"
TEST_API_KEY = settings.api_key


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(db_session):
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_health_endpoint(client):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "Notification" in data["service"]


@pytest.mark.asyncio
async def test_send_notification_unauthorized(client):
    """Missing API key should return 403."""
    response = await client.post("/api/v1/notifications/", json={
        "title": "Test", "message": "Hello", "channel": "SLACK"
    })
    assert response.status_code == 403


@pytest.mark.asyncio
@patch("app.services.webhook_service.WebhookService.send_slack", new_callable=AsyncMock, return_value=True)
async def test_send_slack_notification(mock_slack, client):
    """POST /notifications with valid key + SLACK channel creates and returns notification."""
    response = await client.post(
        "/api/v1/notifications/",
        headers={"X-API-Key": TEST_API_KEY},
        json={
            "title": "Deployment Complete",
            "message": "Service `auth-service` v2.3.1 deployed successfully",
            "channel": "SLACK",
            "priority": "HIGH",
            "source_service": "ci-cd-pipeline",
            "event_type": "deployment.completed",
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == "Deployment Complete"
    assert data["channel"] == "SLACK"
    assert data["status"] == "SENT"
    mock_slack.assert_called_once()


@pytest.mark.asyncio
@patch("app.services.webhook_service.WebhookService.send_slack", new_callable=AsyncMock, return_value=False)
async def test_failed_notification_marked_as_failed(mock_slack, client):
    """When Slack dispatch fails, notification status should be FAILED."""
    response = await client.post(
        "/api/v1/notifications/",
        headers={"X-API-Key": TEST_API_KEY},
        json={"title": "Test", "message": "Fail me", "channel": "SLACK"}
    )
    assert response.status_code == 201
    assert response.json()["status"] == "FAILED"


@pytest.mark.asyncio
@patch("app.services.webhook_service.WebhookService.send_slack", new_callable=AsyncMock, return_value=True)
async def test_list_notifications(mock_slack, client):
    """GET /notifications should return paginated list with correct metadata."""
    # Create 3 notifications
    for i in range(3):
        await client.post(
            "/api/v1/notifications/",
            headers={"X-API-Key": TEST_API_KEY},
            json={"title": f"Notif {i}", "message": "msg", "channel": "SLACK"}
        )

    response = await client.get(
        "/api/v1/notifications/?page=1&page_size=2",
        headers={"X-API-Key": TEST_API_KEY}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["pages"] == 2
    assert len(data["items"]) == 2


@pytest.mark.asyncio
async def test_list_notifications_filter_by_status(client):
    """GET /notifications?status=PENDING should only return PENDING notifications."""
    response = await client.get(
        "/api/v1/notifications/?status=PENDING",
        headers={"X-API-Key": TEST_API_KEY}
    )
    assert response.status_code == 200
    for item in response.json()["items"]:
        assert item["status"] == "PENDING"
