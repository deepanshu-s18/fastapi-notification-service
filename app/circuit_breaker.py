"""
Circuit Breaker + retry logic for outbound webhook/Slack calls using tenacity.

States:
- CLOSED  → requests flow normally
- OPEN    → requests fail-fast (no network call) after consecutive_failures >= threshold
- HALF-OPEN → one trial request to test recovery

This prevents cascading failures when Slack/webhook targets are unavailable.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable, TypeVar

from app.metrics import circuit_breaker_state

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreaker:
    """
    Simple async circuit breaker.

    Args:
        name: Identifier (used in metrics labels)
        failure_threshold: Number of consecutive failures before opening
        recovery_timeout: Seconds before attempting HALF_OPEN trial
        success_threshold: Successes in HALF_OPEN before closing
    """
    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 2

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _consecutive_failures: int = field(default=0, init=False)
    _consecutive_successes: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)

    def _update_metric(self):
        is_open = 1 if self._state == CircuitState.OPEN else 0
        circuit_breaker_state.labels(channel=self.name).set(is_open)

    def _can_attempt(self) -> bool:
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                logger.info("Circuit '%s' entering HALF_OPEN for trial", self.name)
                self._state = CircuitState.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN allows one trial

    def _on_success(self):
        self._consecutive_failures = 0
        if self._state == CircuitState.HALF_OPEN:
            self._consecutive_successes += 1
            if self._consecutive_successes >= self.success_threshold:
                logger.info("Circuit '%s' CLOSED after successful recovery", self.name)
                self._state = CircuitState.CLOSED
                self._consecutive_successes = 0
        self._update_metric()

    def _on_failure(self):
        self._consecutive_successes = 0
        self._consecutive_failures += 1
        if self._state == CircuitState.HALF_OPEN or \
                self._consecutive_failures >= self.failure_threshold:
            logger.warning(
                "Circuit '%s' OPENED after %d failures",
                self.name, self._consecutive_failures
            )
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
        self._update_metric()

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """Execute func through the circuit breaker. Raises RuntimeError if OPEN."""
        if not self._can_attempt():
            raise RuntimeError(
                f"Circuit breaker '{self.name}' is OPEN — "
                f"retry after {int(self.recovery_timeout - (time.monotonic() - self._opened_at))}s"
            )
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise

    @property
    def state(self) -> CircuitState:
        return self._state


# ── Singleton breakers per channel ────────────────────────────────────────────
_breakers: dict[str, CircuitBreaker] = {}


def get_breaker(channel: str) -> CircuitBreaker:
    if channel not in _breakers:
        _breakers[channel] = CircuitBreaker(name=channel)
    return _breakers[channel]
