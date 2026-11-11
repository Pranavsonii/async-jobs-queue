"""
Four tables:

  payouts               actual business data
  processed_operations  idempotency ledger - "did we already do this?"
  outbox_events         transactional outbox (db write + enqueue together)
  dead_letters          jobs that failed for good, kept so we can replay

3 out of 4 tables are only there for reliability.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# sqlite only autoincrements `INTEGER PRIMARY KEY`, a BIGINT pk inserts NULL
# and blows up in tests. keep BIGINT on postgres, INTEGER on sqlite.
PrimaryKey = BigInteger().with_variant(Integer, "sqlite")


def utcnow() -> datetime:
    # always store utc in the db
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# --- payouts ---


class PayoutStatus(str, enum.Enum):
    PENDING = "pending"  # accepted, nobody picked it yet
    PROCESSING = "processing"  # a worker is on it
    PAID = "paid"  # done
    FAILED = "failed"  # failed for good (also in dead_letters)


class Payout(Base):
    """
    One outgoing payment. Amounts are stored in paise as integers,
    never floats (0.1 + 0.2 != 0.3).
    """

    __tablename__ = "payouts"

    id: Mapped[int] = mapped_column(PrimaryKey, primary_key=True, autoincrement=True)

    # client's own id for the payment. unique, so posting the same reference
    # twice can never create two payouts
    reference: Mapped[str] = mapped_column(String(64), unique=True, index=True)

    beneficiary: Mapped[str] = mapped_column(String(128))
    amount_paise: Mapped[int] = mapped_column(BigInteger)
    currency: Mapped[str] = mapped_column(String(3), default="INR")

    status: Mapped[PayoutStatus] = mapped_column(
        Enum(PayoutStatus, native_enum=False, length=16),
        default=PayoutStatus.PENDING,
        index=True,
    )

    attempts: Mapped[int] = mapped_column(Integer, default=0)
    gateway_reference: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    # set when a worker marks it PROCESSING. the beat sweeper uses this to find
    # payouts whose worker died (tasks/periodic.py)
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        return f"<Payout {self.reference} {self.status.value} {self.amount_paise}p>"


# --- idempotency ledger ---


class OperationStatus(str, enum.Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class ProcessedOperation(Base):
    """
    Tracks work that's already been done.

    Tasks can run more than once (at-least-once delivery), this table makes
    that harmless. The unique constraint on `key` is what really does the job -
    only the db sees all workers at the same time.

    We claim (in_progress) before the work and mark completed after it.
    Marking only before = crash loses the work. Marking only after = retry
    repeats it. Lease logic is in services/idempotency.py.
    """

    __tablename__ = "processed_operations"

    id: Mapped[int] = mapped_column(PrimaryKey, primary_key=True, autoincrement=True)

    key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    task_name: Mapped[str] = mapped_column(String(128))

    status: Mapped[OperationStatus] = mapped_column(
        Enum(OperationStatus, native_enum=False, length=16),
        default=OperationStatus.IN_PROGRESS,
    )

    # which celery task holds the claim, so a task can tell its own retry
    # apart from some other worker
    owner_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # result of the first successful run, returned again for duplicates
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    claimed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


# --- outbox ---


class OutboxStatus(str, enum.Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"


class OutboxEvent(Base):
    """
    Fixes "row committed but enqueue failed" (and the opposite, enqueued but
    commit rolled back so the worker finds nothing).

    Can't have one transaction across postgres and redis. So we write the
    "please enqueue this" row in the same transaction as the business row, and
    a poller does the real enqueue later.

    Downside: poller can crash after enqueue but before marking published, so
    an event can go out twice. That's ok because the consumer is idempotent.
    """

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(PrimaryKey, primary_key=True, autoincrement=True)

    event_type: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict] = mapped_column(JSON)

    status: Mapped[OutboxStatus] = mapped_column(
        Enum(OutboxStatus, native_enum=False, length=16),
        default=OutboxStatus.PENDING,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


# poller does `WHERE status='pending' ORDER BY id`, without this index it
# turns into a full scan once there are millions of published rows
Index("ix_outbox_pending", OutboxEvent.status, OutboxEvent.id)


# --- dead letters ---


class DeadLetter(Base):
    """
    Jobs that failed permanently end up here instead of disappearing.

    Some tasks will fail every time (bad data, closed account, a bug).
    Retrying forever wastes workers, dropping them loses money. So we store
    the args + traceback, someone fixes the issue and replays it.
    """

    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(PrimaryKey, primary_key=True, autoincrement=True)

    task_name: Mapped[str] = mapped_column(String(128), index=True)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # json so we can replay with the exact same args
    task_args: Mapped[dict] = mapped_column(JSON)

    error: Mapped[str] = mapped_column(Text)
    traceback: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    replayed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


def create_all(bind) -> None:
    # fine for the demo and tests. for real deploys use alembic,
    # create_all can't alter existing tables
    Base.metadata.create_all(bind=bind)
