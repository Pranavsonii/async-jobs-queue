"""
Idempotency - running a task twice should be the same as running it once.

Most important file here, this is what makes at-least-once delivery safe
with money.

Why not just check-then-insert?

    if already_processed(key):      # SELECT
        return "duplicate"
    do_the_work()                   # <-- gap
    mark_processed(key)             # INSERT

Two workers can both pass the SELECT at the same time and both do the work.
Race condition. So we INSERT first and let the unique constraint decide - if
the insert works we own it, if it raises IntegrityError someone else got there.

Why a lease?

If a task claims the key and then the worker dies, the row stays in_progress
forever and every retry skips it. So the claim is only trusted for some time:

  completed                    -> duplicate, return the stored result
  in_progress, same task_id    -> our own retry, go ahead
  in_progress, expired         -> old owner is dead, take over
  in_progress, fresh, other    -> someone is working on it, skip

Keep the lease longer than the slowest task (same idea as visibility timeout).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..config import settings
from ..models import OperationStatus, ProcessedOperation, utcnow


class ClaimOutcome(str, Enum):
    PROCEED = "proceed"                # you own it, do the work
    DUPLICATE = "duplicate"            # already done
    IN_FLIGHT = "in_flight"            # another worker has it right now


@dataclass
class Claim:
    outcome: ClaimOutcome
    key: str
    # for DUPLICATE: result from the first run
    previous_result: dict[str, Any] | None = None

    @property
    def should_process(self) -> bool:
        return self.outcome is ClaimOutcome.PROCEED


def claim_operation(
    db: Session,
    key: str,
    task_name: str,
    task_id: str | None = None,
    lease_seconds: int | None = None,
) -> Claim:
    """
    Try to own the work for `key`. Call this at the top of any task with
    side effects.

    The key must come from the business data (payout reference, webhook event
    id) - not from anything celery generates, otherwise every redelivery gets
    a new key and this does nothing.

    Commits by itself on purpose so other workers can see the claim right away,
    before the slow work starts.
    """
    lease = timedelta(seconds=lease_seconds or settings.idempotency_lease_seconds)

    # let the unique constraint pick the winner
    try:
        with db.begin_nested():  # savepoint so a failure doesn't break the outer tx
            db.add(
                ProcessedOperation(
                    key=key,
                    task_name=task_name,
                    status=OperationStatus.IN_PROGRESS,
                    owner_task_id=task_id,
                    claimed_at=utcnow(),
                )
            )
            db.flush()  # run the INSERT now so IntegrityError comes here
    except IntegrityError:
        pass  # row already exists, check it below
    else:
        db.commit()
        return Claim(ClaimOutcome.PROCEED, key)

    # someone else inserted first, see what state it's in
    existing = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == key)
    ).scalar_one()

    if existing.status is OperationStatus.COMPLETED:
        return Claim(ClaimOutcome.DUPLICATE, key, previous_result=existing.result)

    if task_id and existing.owner_task_id == task_id:
        # our own retry - celery keeps the same task id across retries
        return Claim(ClaimOutcome.PROCEED, key)

    claimed_at = existing.claimed_at
    if claimed_at is not None and claimed_at.tzinfo is None:
        # sqlite loses tz info, add it back so the compare works
        claimed_at = claimed_at.replace(tzinfo=utcnow().tzinfo)

    if claimed_at is None or (utcnow() - claimed_at) > lease:
        # lease expired, previous worker is probably dead - take it over
        existing.owner_task_id = task_id
        existing.claimed_at = utcnow()
        db.commit()
        return Claim(ClaimOutcome.PROCEED, key)

    # someone alive is working on it, leave it
    return Claim(ClaimOutcome.IN_FLIGHT, key)


def complete_operation(
    db: Session, key: str, result: dict[str, Any] | None = None
) -> None:
    """
    Mark as done and save the result.

    Call this inside the SAME transaction as the side effect. If they commit
    separately and we crash in between, the retry pays again.
    """
    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == key)
    ).scalar_one_or_none()
    if op is None:  # shouldn't happen unless it got purged mid-flight
        op = ProcessedOperation(key=key, task_name="unknown")
        db.add(op)

    op.status = OperationStatus.COMPLETED
    op.result = result
    op.completed_at = utcnow()


def release_operation(db: Session, key: str) -> None:
    """
    Drop the claim after a failure we want to retry, so the retry doesn't have
    to wait for the lease to run out.

    Only for retryable failures. Permanent failures keep the claim so the
    bad message can't loop.
    """
    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == key)
    ).scalar_one_or_none()
    if op is not None and op.status is OperationStatus.IN_PROGRESS:
        db.delete(op)
