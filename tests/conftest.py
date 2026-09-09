"""
conftest.py — pytest configuration for fastapi-notification-service

Mocks the Kafka consumer startup so tests don't require a real Kafka broker.
The lifespan function in main.py calls start_kafka_consumer() on app startup;
without this mock it would hang trying to connect to localhost:9092.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture(autouse=True)
def mock_kafka_consumer():
    """
    Patch start_kafka_consumer to return a coroutine that immediately finishes.
    This prevents the ASGI lifespan from blocking on a Kafka connection.
    """
    async def noop():
        # Simulate a consumer task that runs until cancelled
        try:
            await asyncio.sleep(9999)
        except asyncio.CancelledError:
            pass

    with patch("app.main.start_kafka_consumer", return_value=noop()):
        yield
