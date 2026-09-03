"""Generic helpers for resolving concurrent.futures.Future values embedded in
a nested dict/list tree, plus a progress-reporting wait.

Used to make per-resource `kubectl describe` calls asynchronous: an export
method builds its resource dicts immediately with a Future in the
'describe_info' slot (non-blocking submit), and the caller resolves the
whole tree once, after every resource type has been fetched, so the
describe subprocesses for one resource type overlap with the API calls
and describes of every other resource type.
"""

import logging
import time
from concurrent.futures import Future, as_completed
from typing import Callable, List

logger = logging.getLogger(__name__)


def collect_futures(obj) -> List[Future]:
    """Depth-first list of every Future found anywhere in a dict/list tree."""
    found: List[Future] = []
    _walk(obj, found.append)
    return found


def resolve_futures(obj):
    """Return a copy of the tree with every Future replaced by its .result().

    Re-raises the first exception found in any future, same as calling
    .result() directly would. Pure: the input tree is not modified.
    """
    if isinstance(obj, Future):
        return obj.result()
    if isinstance(obj, dict):
        return {k: resolve_futures(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_futures(v) for v in obj]
    return obj


def _walk(obj, visit: Callable[[Future], None]):
    if isinstance(obj, Future):
        visit(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _walk(v, visit)
    elif isinstance(obj, list):
        for v in obj:
            _walk(v, visit)


def wait_with_progress(futures: List[Future], on_progress: Callable[[int, int], None] = None,
                        min_interval_s: float = 2.0, clock: Callable[[], float] = time.monotonic) -> None:
    """Block until every future is done, calling on_progress(done, total) as they finish.

    Callbacks are throttled to at most one per min_interval_s, always
    including the final (total, total) call. A future raising is not
    re-raised here (that happens later, in resolve_futures / .result());
    this only waits for completion so failures don't block progress.
    """
    total = len(futures)
    if total == 0:
        return
    done = 0
    last_report = clock() - min_interval_s
    for f in as_completed(futures):
        done += 1
        now = clock()
        if on_progress and (done == total or now - last_report >= min_interval_s):
            on_progress(done, total)
            last_report = now
