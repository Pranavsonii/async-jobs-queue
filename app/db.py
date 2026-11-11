"""
DB setup. One engine per process, one short session per unit of work.

Workers live for a long time and run thousands of tasks, so a leaked session
slowly kills the worker. Every task opens a session, commits/rolls back and
closes it - same as a web request.
"""

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import settings

# pool_pre_ping checks the connection before using it. without this a worker
# that sat idle overnight picks up dead connections and the first task fails
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


@contextmanager
def session_scope() -> Iterator[Session]:
    """
    Commit on success, rollback on error.

        with session_scope() as db:
            db.add(thing)

    The outbox depends on this - business row and outbox row go in the same
    `with` block so they commit together.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db() -> Iterator[Session]:
    """Same thing as a FastAPI dependency."""
    with session_scope() as db:
        yield db


def is_postgres() -> bool:
    # FOR UPDATE SKIP LOCKED only works on postgres, tests run on sqlite
    return engine.dialect.name == "postgresql"
