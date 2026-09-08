from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    # App
    app_name: str = "Notification Microservice"
    app_version: str = "1.0.0"
    debug: bool = False

    # Database
    database_url: str = "postgresql+asyncpg://notifyuser:notifypass@localhost:5432/notifydb"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_notifications: str = "notifications"
    kafka_consumer_group: str = "notification-service-group"
    kafka_auto_offset_reset: str = "earliest"

    # Slack
    slack_webhook_url: str = ""
    slack_enabled: bool = False

    # API Auth (simple API-key for internal service auth)
    api_key: str = "internal-service-key-change-in-prod"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    return Settings()
