"""Unit tests for pipeline.neo4j_driver.run_with_retry.

These use a fake driver to simulate transient deadlocks and make sure the
retry loop matches the previous _neo4j_run_with_retry behavior.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from neo4j.exceptions import TransientError

from pipeline.neo4j_driver import run_with_retry


class _FakeSession:
    def __init__(self, driver: _FakeDriver) -> None:
        self._driver = driver

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def run(self, cypher: str, **params: Any) -> None:
        self._driver.calls.append((cypher, params))
        if self._driver.calls_until_success is None:
            return
        if len(self._driver.calls) < self._driver.calls_until_success:
            raise TransientError("simulated deadlock")


class _FakeDriver:
    def __init__(self, calls_until_success: int | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.calls_until_success = calls_until_success

    def session(self) -> _FakeSession:
        return _FakeSession(self)


def test_run_with_retry_succeeds_on_first_try() -> None:
    driver = _FakeDriver()
    run_with_retry(driver, "RETURN 1", foo="bar")
    assert len(driver.calls) == 1
    assert driver.calls[0] == ("RETURN 1", {"foo": "bar"})


def test_run_with_retry_retries_until_success() -> None:
    driver = _FakeDriver(calls_until_success=3)
    with patch("pipeline.neo4j_driver.time.sleep"):
        run_with_retry(driver, "RETURN 1")
    assert len(driver.calls) == 3


def test_run_with_retry_raises_after_max_retries() -> None:
    driver = _FakeDriver(calls_until_success=10)
    with patch("pipeline.neo4j_driver.time.sleep"):
        with pytest.raises(TransientError):
            run_with_retry(driver, "RETURN 1", max_retries=3)
    assert len(driver.calls) == 3
