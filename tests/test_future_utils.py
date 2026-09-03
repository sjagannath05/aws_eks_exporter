from concurrent.futures import Future, ThreadPoolExecutor
import time

import pytest

import future_utils as fu


def _done(value):
    f = Future()
    f.set_result(value)
    return f


def test_collect_futures_finds_nested_futures():
    tree = {
        "a": [{"name": "x", "describe_info": _done("out-x")}, {"name": "y", "describe_info": "already-a-string"}],
        "b": {"nested": [_done("z")]},
        "c": "plain",
    }
    found = fu.collect_futures(tree)
    assert len(found) == 2
    assert {f.result() for f in found} == {"out-x", "z"}


def test_collect_futures_empty_tree():
    assert fu.collect_futures({}) == []
    assert fu.collect_futures([]) == []
    assert fu.collect_futures("x") == []


def test_resolve_futures_substitutes_results_and_leaves_other_values():
    tree = {
        "list": [{"describe_info": _done("resolved-1")}, {"describe_info": "static"}],
        "dict": {"describe_info": _done("resolved-2")},
        "num": 5,
        "none": None,
    }
    resolved = fu.resolve_futures(tree)
    assert resolved["list"][0]["describe_info"] == "resolved-1"
    assert resolved["list"][1]["describe_info"] == "static"
    assert resolved["dict"]["describe_info"] == "resolved-2"
    assert resolved["num"] == 5
    assert resolved["none"] is None
    # original tree is untouched (pure function)
    assert isinstance(tree["list"][0]["describe_info"], Future)


def test_resolve_futures_propagates_exceptions():
    f = Future()
    f.set_exception(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        fu.resolve_futures({"x": f})


def test_wait_with_progress_calls_back_for_each_completion_and_waits_for_all():
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(lambda i=i: (time.sleep(0.01), i)[1]) for i in range(9)]
        seen = []
        fu.wait_with_progress(futures, on_progress=lambda done, total: seen.append((done, total)))
    assert seen[-1] == (9, 9)
    assert [d for d, _ in seen] == sorted(d for d, _ in seen)  # done count is non-decreasing
    assert all(t == 9 for _, t in seen)


def test_wait_with_progress_empty_list_calls_back_nothing():
    calls = []
    fu.wait_with_progress([], on_progress=lambda d, t: calls.append((d, t)))
    assert calls == []


def test_wait_with_progress_swallows_task_exceptions_so_all_complete():
    with ThreadPoolExecutor(max_workers=2) as ex:
        def boom():
            raise ValueError("nope")
        futures = [ex.submit(boom), ex.submit(lambda: "ok")]
        seen = []
        fu.wait_with_progress(futures, on_progress=lambda d, t: seen.append(d))
    assert seen[-1] == 2
    # the failing future's exception is still retrievable later via resolve_futures/.result()
    with pytest.raises(ValueError):
        futures[0].result()
