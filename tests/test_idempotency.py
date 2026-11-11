"""
Idempotency ledger tests. If this breaks we pay someone twice, so these
matter the most.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from app.models import OperationStatus, ProcessedOperation, utcnow
from app.services.idempotency import (
    ClaimOutcome,
    claim_operation,
    complete_operation,
    release_operation,
)


def test_first_claim_proceeds(db):
    claim = claim_operation(db, key="payout:PO-1", task_name="t", task_id="task-a")
    assert claim.outcome is ClaimOutcome.PROCEED
    assert claim.should_process


def test_completed_work_is_reported_as_duplicate(db):
    """Message redelivered after the work was already done."""
    claim_operation(db, key="payout:PO-1", task_name="t", task_id="task-a")
    complete_operation(db, "payout:PO-1", {"status": "paid", "amount": 500})
    db.commit()

    second = claim_operation(db, key="payout:PO-1", task_name="t", task_id="task-b")

    assert second.outcome is ClaimOutcome.DUPLICATE
    # duplicate gets the same result back
    assert second.previous_result == {"status": "paid", "amount": 500}


def test_live_worker_holding_claim_blocks_others(db):
    """Two workers get the same message at once, only one goes ahead."""
    first = claim_operation(db, key="payout:PO-2", task_name="t", task_id="task-a")
    second = claim_operation(db, key="payout:PO-2", task_name="t", task_id="task-b")

    assert first.outcome is ClaimOutcome.PROCEED
    assert second.outcome is ClaimOutcome.IN_FLIGHT


def test_a_task_can_reclaim_its_own_retry(db):
    """
    Celery keeps the task id on retries. Without this check the retry would
    see its own claim and skip, and the payout would never finish.
    """
    claim_operation(db, key="payout:PO-3", task_name="t", task_id="task-a")
    again = claim_operation(db, key="payout:PO-3", task_name="t", task_id="task-a")

    assert again.outcome is ClaimOutcome.PROCEED


def test_expired_lease_is_stolen(db):
    """Worker holding the claim got killed - its claim shouldn't block forever."""
    claim_operation(db, key="payout:PO-4", task_name="t", task_id="dead-worker")

    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "payout:PO-4")
    ).scalar_one()
    op.claimed_at = utcnow() - timedelta(hours=2)  # make the lease old
    db.commit()

    taken_over = claim_operation(
        db, key="payout:PO-4", task_name="t", task_id="live-worker", lease_seconds=60
    )

    assert taken_over.outcome is ClaimOutcome.PROCEED
    db.refresh(op)
    assert op.owner_task_id == "live-worker"


def test_fresh_lease_is_not_stolen(db):
    """Opposite of above, a lease that hasn't expired is respected."""
    claim_operation(db, key="payout:PO-5", task_name="t", task_id="busy-worker")

    other = claim_operation(
        db, key="payout:PO-5", task_name="t", task_id="impatient", lease_seconds=3600
    )

    assert other.outcome is ClaimOutcome.IN_FLIGHT


def test_release_frees_the_claim_for_retry(db):
    # after a transient failure we release so any worker can retry right away
    claim_operation(db, key="payout:PO-6", task_name="t", task_id="task-a")
    release_operation(db, "payout:PO-6")
    db.commit()

    retry = claim_operation(db, key="payout:PO-6", task_name="t", task_id="task-b")
    assert retry.outcome is ClaimOutcome.PROCEED


def test_release_does_not_free_completed_work(db):
    # only in-progress claims can be released, completed stays completed
    claim_operation(db, key="payout:PO-7", task_name="t", task_id="task-a")
    complete_operation(db, "payout:PO-7", {"status": "paid"})
    db.commit()

    release_operation(db, "payout:PO-7")
    db.commit()

    still_done = claim_operation(db, key="payout:PO-7", task_name="t", task_id="task-b")
    assert still_done.outcome is ClaimOutcome.DUPLICATE


def test_the_unique_constraint_is_what_actually_enforces_this(db):
    """
    The guarantee comes from the unique index in the db. If a migration
    ever drops it, this test catches it.
    """
    db.add(ProcessedOperation(key="dup", task_name="t"))
    db.commit()

    from sqlalchemy.exc import IntegrityError

    db.add(ProcessedOperation(key="dup", task_name="t"))
    with pytest.raises(IntegrityError):
        db.commit()
    db.rollback()


def test_claim_defaults_to_in_progress(db):
    claim_operation(db, key="payout:PO-8", task_name="payouts.process", task_id="x")
    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "payout:PO-8")
    ).scalar_one()

    assert op.status is OperationStatus.IN_PROGRESS
    assert op.completed_at is None
