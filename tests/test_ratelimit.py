import pytest

import ratelimit


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def test_bucket_allows_burst_then_throttles():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=10, burst=3, clock=clk.now, sleep=clk.sleep)
    for _ in range(3):
        b.acquire()
    assert clk.sleeps == []
    b.acquire()  # 4th call must wait for one token at 10/s
    assert len(clk.sleeps) == 1 and abs(clk.sleeps[0] - 0.1) < 1e-9


def test_bucket_refills_over_time():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=2, burst=2, clock=clk.now, sleep=clk.sleep)
    b.acquire(); b.acquire()
    clk.t += 1.0  # 2 tokens refilled
    b.acquire(); b.acquire()
    assert clk.sleeps == []


def test_bucket_never_exceeds_burst():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=100, burst=2, clock=clk.now, sleep=clk.sleep)
    clk.t += 100
    b.acquire(); b.acquire(); b.acquire()
    assert len(clk.sleeps) == 1


@pytest.mark.parametrize("rate,burst", [(0, 5), (-1, 5), (5, 0)])
def test_bucket_rate_zero_means_unlimited(rate, burst):
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=rate, burst=burst, clock=clk.now, sleep=clk.sleep)
    for _ in range(50):
        b.acquire()
    assert clk.sleeps == []


class FakeApiException(Exception):
    def __init__(self, status, headers=None):
        super().__init__(f"status {status}")
        self.status = status
        self.headers = headers or {}


class FakeApi:
    def __init__(self, failures):
        self.failures = list(failures)
        self.calls = 0
        self.attr = "not-callable"

    def list_pods(self, *a, **k):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return ("ok", a, k)


def test_proxy_acquires_token_per_call_and_passes_args():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=1, burst=1, clock=clk.now, sleep=clk.sleep)
    api = ratelimit.RateLimitedApi(FakeApi([]), b, retry_exception=FakeApiException, sleep=clk.sleep)
    assert api.list_pods(1, x=2) == ("ok", (1,), {"x": 2})
    api.list_pods()
    assert len(clk.sleeps) == 1  # second call throttled
    assert api.attr == "not-callable"  # non-callables pass straight through


def test_proxy_retries_429_honouring_retry_after():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=0, burst=0, clock=clk.now, sleep=clk.sleep)
    inner = FakeApi([FakeApiException(429, {"Retry-After": "3"}), FakeApiException(429)])
    api = ratelimit.RateLimitedApi(inner, b, retry_exception=FakeApiException, sleep=clk.sleep, max_retries=5,
                                   base_backoff=0.5)
    assert api.list_pods()[0] == "ok"
    assert inner.calls == 3
    assert clk.sleeps[0] == 3.0            # from Retry-After
    assert 0.5 <= clk.sleeps[1] <= 2.0     # exponential backoff, second attempt


def test_proxy_gives_up_after_max_retries():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=0, burst=0, clock=clk.now, sleep=clk.sleep)
    inner = FakeApi([FakeApiException(429)] * 10)
    api = ratelimit.RateLimitedApi(inner, b, retry_exception=FakeApiException, sleep=clk.sleep, max_retries=2)
    with pytest.raises(FakeApiException):
        api.list_pods()
    assert inner.calls == 3  # 1 + 2 retries


def test_proxy_does_not_retry_other_statuses():
    clk = FakeClock()
    b = ratelimit.TokenBucket(rate=0, burst=0, clock=clk.now, sleep=clk.sleep)
    inner = FakeApi([FakeApiException(403)])
    api = ratelimit.RateLimitedApi(inner, b, retry_exception=FakeApiException, sleep=clk.sleep)
    with pytest.raises(FakeApiException):
        api.list_pods()
    assert inner.calls == 1
