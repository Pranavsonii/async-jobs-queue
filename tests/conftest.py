"""
Test setup.

- business logic is tested as plain functions (no broker/worker)
- eager mode is used to test the task wiring
- eager mode runs tasks in-process, so it doesn't prove anything about
  concurrency, redelivery or real retries. that needs a real broker.

Env vars are set before importing app.config since settings are read once.
"""

from __future__ import annotations

import os
from pathlib import Path

TEST_DB = Path(__file__).resolve().parent / ".payflow-test.db"

os.environ["DATABASE_URL"] = f"sqlite+pysqlite:///{TEST_DB.as_posix()}"
os.environ["GATEWAY_SEED"] = "1234"      # same "random" failures every run
os.environ["GATEWAY_LATENCY_MS"] = "0"   # no sleeping in tests
os.environ["GATEWAY_FAILURE_RATE"] = "0"  # tests turn failures on when needed

import pytest  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from app.db import engine  # noqa: E402
from app.models import Base  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _schema():
    """Create schema once, delete the db file at the end."""
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    engine.dispose()
    TEST_DB.unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def _clean_tables():
    # tasks open their own session via session_scope(), so tests share the
    # same engine and we just empty the tables after each test
    yield
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())


@pytest.fixture
def db():
    """Session on the same db the tasks and api use."""
    Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    session = Session()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def eager_celery():
    """
    Run tasks synchronously in this process, no broker.

    task_eager_propagates makes task exceptions show up in the test,
    otherwise a broken task just returns a failed result and the test passes.
    """
    from app.celery_app import celery_app

    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    yield celery_app
    celery_app.conf.task_always_eager = False
    celery_app.conf.task_eager_propagates = False


@pytest.fixture
def flaky_gateway(monkeypatch):
    """Change the gateway failure rate for one test."""

    def _set(rate: float) -> None:
        from app.config import settings

        monkeypatch.setattr(settings, "gateway_failure_rate", rate)

    return _set
