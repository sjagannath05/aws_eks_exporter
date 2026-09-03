"""Client-side rate limiting for the exporter's Kubernetes/kubectl calls.

A single TokenBucket is shared by the Python API client (via RateLimitedApi
proxies) and the kubectl describe subprocess so the API server sees one
bounded request stream. RateLimitedApi also retries HTTP 429 responses,
honouring Retry-After, since a throttled server tells us exactly how long
to back off.
"""

import logging
import random
import threading
import time
from typing import Callable, Optional, Type

logger = logging.getLogger(__name__)

DEFAULT_QPS = 10.0      # sustained requests per second
DEFAULT_BURST = 20      # requests allowed before throttling kicks in


class TokenBucket:
    """Blocking token bucket. rate <= 0 or burst <= 0 disables limiting."""

    def __init__(self, rate: float = DEFAULT_QPS, burst: int = DEFAULT_BURST,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep):
        self.rate = float(rate)
        self.burst = int(burst)
        self.enabled = self.rate > 0 and self.burst > 0
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(self.burst)
        self._last = clock()
        self._lock = threading.Lock()
        self.waited_total = 0.0

    def _refill(self):
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(float(self.burst), self._tokens + elapsed * self.rate)

    def acquire(self) -> float:
        """Block until a token is available. Returns seconds slept."""
        if not self.enabled:
            return 0.0
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait = (1.0 - self._tokens) / self.rate
            self._sleep(wait)
            self.waited_total += wait
            self._refill()
            self._tokens = max(0.0, self._tokens - 1.0)
            return wait


def _retry_after_seconds(exc) -> Optional[float]:
    headers = getattr(exc, "headers", None)
    if not headers:
        return None
    try:
        value = headers.get("Retry-After") or headers.get("retry-after")
    except AttributeError:
        return None
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None  # HTTP-date form; fall back to backoff


class RateLimitedApi:
    """Proxy that rate-limits every method call on `api` and retries HTTP 429.

    retry_exception is the exception type carrying a `.status` attribute
    (kubernetes.client.exceptions.ApiException in production).
    """

    RETRY_STATUSES = (429,)

    def __init__(self, api, bucket: TokenBucket, retry_exception: Type[BaseException],
                 max_retries: int = 5, base_backoff: float = 1.0, max_backoff: float = 30.0,
                 sleep: Callable[[float], None] = time.sleep):
        object.__setattr__(self, "_api", api)
        object.__setattr__(self, "_bucket", bucket)
        object.__setattr__(self, "_retry_exception", retry_exception)
        object.__setattr__(self, "_max_retries", max(0, int(max_retries)))
        object.__setattr__(self, "_base_backoff", float(base_backoff))
        object.__setattr__(self, "_max_backoff", float(max_backoff))
        object.__setattr__(self, "_sleep", sleep)

    def __getattr__(self, name):
        attr = getattr(self._api, name)
        if not callable(attr):
            return attr

        def wrapped(*args, **kwargs):
            attempt = 0
            while True:
                self._bucket.acquire()
                try:
                    return attr(*args, **kwargs)
                except self._retry_exception as e:
                    status = getattr(e, "status", None)
                    if status not in self.RETRY_STATUSES or attempt >= self._max_retries:
                        raise
                    attempt += 1
                    delay = _retry_after_seconds(e)
                    if delay is None:
                        delay = min(self._max_backoff,
                                    self._base_backoff * (2 ** (attempt - 1)) * random.uniform(1.0, 1.5))
                    logger.warning(f"API server returned {status} on {name}; retry {attempt}/{self._max_retries} "
                                   f"in {delay:.1f}s")
                    self._sleep(delay)

        wrapped.__name__ = name
        return wrapped

    def __setattr__(self, name, value):
        setattr(self._api, name, value)
