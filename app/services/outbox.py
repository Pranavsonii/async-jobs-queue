"""
Transactional outbox - db write and enqueue that never go out of sync.

The problem:

    db.add(payout); db.commit()      # postgres
    process_payout.delay(payout.id)  # redis

Two separate commits. Things that can go wrong:
  1. commit ok, enqueue fails      -> payout never processed
  2. enqueue ok, commit fails      -> worker can't find the row
  3. enqueue ok, commit is slow    -> worker reads before commit, sees old data

try/except doesn't fix it, the cleanup can fail too.

Fix: don't enqueue from the request. Save the "enqueue this" row in the same
transaction:

    with session_scope() as db:
        db.add(payout)
        record_event(db, "payout.requested", {"payout_id": ...})

Beat runs a poller every few seconds that reads this table and does the actual
enqueue. Job only exists if the data is committed.

Tradeoffs: a bit of extra latency (up to one poll), and an event can be sent
twice if the poller crashes after sending. Consumer is idempotent so it's fine.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import is_postgres
from ..models import OutboxEvent, OutboxStatus, utcnow


def record_event(db: Session, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
    # doesn't commit - the caller's transaction decides. that's the whole point
    event = OutboxEvent(
        event_type=event_type,
        payload=payload,
        status=OutboxStatus.PENDING,
    )
    db.add(event)
    return event


def fetch_unpublished(db: Session, limit: int = 50) -> list[OutboxEvent]:
    """
    Get a batch of pending events and lock them.

    FOR UPDATE   -> row lock till the transaction ends
    SKIP LOCKED  -> skip rows already locked instead of waiting

    So multiple pollers can run in parallel, each getting different rows,
    instead of all waiting on each other.
    """
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.PENDING)
        .order_by(OutboxEvent.id)       # oldest first
        .limit(limit)
    )

    # sqlite has no row locks, skip it there (tests are single threaded anyway)
    if is_postgres():
        stmt = stmt.with_for_update(skip_locked=True)

    return list(db.execute(stmt).scalars().all())


def mark_published(db: Session, event: OutboxEvent) -> None:
    event.status = OutboxStatus.PUBLISHED
    event.published_at = utcnow()


def mark_failed(db: Session, event: OutboxEvent, error: str) -> None:
    """
    Record the error but keep it PENDING so the next tick tries again.
    A broker down for a couple of minutes shouldn't lose events.
    """
    event.attempts += 1
    event.last_error = error[:1000]
    if event.attempts >= 10:
        # 10 failures means the message itself is broken. stop retrying
        # but keep the row around to look at
        event.status = OutboxStatus.FAILED
