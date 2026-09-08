import time
import aiohttp
import logging
from app.config import get_settings
from app.circuit_breaker import get_breaker
from app.metrics import dispatch_duration_seconds, webhook_requests_total

logger = logging.getLogger(__name__)
settings = get_settings()


class WebhookService:
    """
    Handles outbound webhook/Slack notifications.
    Demonstrates system-to-system integration via HTTP webhooks.
    """

    @staticmethod
    async def send_slack(title: str, message: str, priority: str = "NORMAL",
                         recipient: str | None = None) -> bool:
        if not settings.slack_enabled or not settings.slack_webhook_url:
            logger.info("Slack disabled or no webhook URL configured — skipping")
            return True

        emoji = {"LOW": "📋", "NORMAL": "🔔", "HIGH": "⚠️", "CRITICAL": "🚨"}.get(priority, "🔔")
        color = {"LOW": "#36a64f", "NORMAL": "#439FE0", "HIGH": "#FFA500", "CRITICAL": "#FF0000"}.get(priority, "#439FE0")

        payload = {
            "attachments": [{
                "color": color,
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {title}"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": message}},
                    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Priority: *{priority}*"}]}
                ]
            }]
        }
        if recipient and recipient.startswith("#"):
            payload["channel"] = recipient

        breaker = get_breaker("SLACK")
        start = time.monotonic()
        try:
            async def _do_request() -> bool:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        settings.slack_webhook_url, json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        return resp.status == 200

            result = await breaker.call(_do_request)
            webhook_requests_total.labels(channel="SLACK", success=str(result)).inc()
            logger.info("Slack notification sent: '%s'", title)
            return result
        except RuntimeError as e:
            # Circuit is OPEN — fail fast
            logger.warning("Slack circuit breaker OPEN: %s", e)
            webhook_requests_total.labels(channel="SLACK", success="False").inc()
            return False
        except aiohttp.ClientError as e:
            logger.error("Slack webhook connection error: %s", str(e))
            webhook_requests_total.labels(channel="SLACK", success="False").inc()
            return False
        finally:
            dispatch_duration_seconds.labels(channel="SLACK").observe(time.monotonic() - start)

    @staticmethod
    async def send_generic_webhook(url: str, payload: dict) -> bool:
        """
        POST arbitrary JSON payload to a webhook URL.
        Used for generic system-to-system integration.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json", "User-Agent": "NotificationService/1.0"},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    success = response.status < 400
                    logger.info("Webhook %s → %d (success=%s)", url, response.status, success)
                    return success
        except aiohttp.ClientError as e:
            logger.error("Webhook error for %s: %s", url, str(e))
            return False
