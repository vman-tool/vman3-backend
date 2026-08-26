import pytest

from tests.support.fakes import FakeDB


@pytest.fixture
def fake_db() -> FakeDB:
    """A FakeDB with no queued query responses - use FakeDB(responder=...)
    directly instead when a test needs specific query -> result mapping."""
    return FakeDB()
