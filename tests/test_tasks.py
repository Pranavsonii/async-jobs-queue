"""
Task tests in eager mode. Checks wiring and final state - right status,
duplicates are no-ops, bad messages land in the DLQ. Runs in-process so it
says nothing about concurrency.
"""

from __future__ import annotations

from sqlalchemy import select

from app.models import (
    DeadLetter,
    OperationStatus,
    OutboxEvent,
    Payout,
    PayoutStatus,
    ProcessedOperation,
)
from app.services import payouts as payout_service


def _make_payout(db, reference="PO-1", amount=25_000, beneficiary="Asha Kumar"):
    payout, _ = payout_service.create_payout(
        db, reference=reference, beneficiary=beneficiary, amount_paise=amount
    )
    db.commit()
    return payout


# happy path


def test_successful_payout(db, eager_celery):
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "PO-OK")
    result = process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "PO-OK"}
    ).get()

    db.expire_all()
    payout = db.get(Payout, payout.id)

    assert result["status"] == "paid"
    assert payout.status is PayoutStatus.PAID
    assert payout.gateway_reference is not None
    assert payout.locked_at is None  # lock released


def test_success_records_a_completed_idempotency_key(db, eager_celery):
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "PO-KEY")
    process_payout.apply(kwargs={"payout_id": payout.id, "reference": "PO-KEY"}).get()

    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "payout:PO-KEY")
    ).scalar_one()

    assert op.status is OperationStatus.COMPLETED
    assert op.result["status"] == "paid"
    assert op.completed_at is not None


def test_success_stages_a_receipt_through_the_outbox(db, eager_celery):
    # receipt goes via outbox, not send_receipt.delay()
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "PO-RECEIPT")
    process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "PO-RECEIPT"}
    ).get()

    events = db.execute(
        select(OutboxEvent).where(OutboxEvent.event_type == "payout.succeeded")
    ).scalars().all()

    assert len(events) == 1
    assert events[0].payload["reference"] == "PO-RECEIPT"


# duplicates


def test_duplicate_delivery_does_not_pay_twice(db, eager_celery):
    """The main one - same message delivered twice, only paid once."""
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "PO-DUP")

    first = process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "PO-DUP"}
    ).get()
    second = process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "PO-DUP"}
    ).get()

    # same answer both times
    assert first["gateway_reference"] == second["gateway_reference"]

    db.expire_all()
    payout = db.get(Payout, payout.id)
    # second delivery never hit the gateway
    assert payout.attempts == 1

    # only one receipt
    receipts = db.execute(
        select(OutboxEvent).where(OutboxEvent.event_type == "payout.succeeded")
    ).scalars().all()
    assert len(receipts) == 1


# permanent failure


def test_permanent_failure_goes_to_the_dlq_without_retrying(db, eager_celery):
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "FAIL-CLOSED-ACCOUNT")
    process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "FAIL-CLOSED-ACCOUNT"}
    )

    db.expire_all()
    payout = db.get(Payout, payout.id)
    entries = db.execute(select(DeadLetter)).scalars().all()

    assert payout.status is PayoutStatus.FAILED
    assert payout.failure_reason
    assert len(entries) == 1
    assert entries[0].task_args["reference"] == "FAIL-CLOSED-ACCOUNT"
    # only one attempt, no retries for a closed account
    assert payout.attempts == 1


def test_a_dead_lettered_payout_is_not_reprocessed_on_redelivery(db, eager_celery):
    # failed job keeps its claim so a redelivery can't send it again
    from app.tasks.payouts import process_payout

    payout = _make_payout(db, "FAIL-POISON")
    process_payout.apply(kwargs={"payout_id": payout.id, "reference": "FAIL-POISON"})
    process_payout.apply(kwargs={"payout_id": payout.id, "reference": "FAIL-POISON"})

    entries = db.execute(select(DeadLetter)).scalars().all()
    assert len(entries) == 1  # not two


def test_replaying_a_dead_letter_requeues_it(db, eager_celery):
    from app.tasks.payouts import process_payout, replay_dead_letter

    payout = _make_payout(db, "FAIL-REPLAY")
    process_payout.apply(kwargs={"payout_id": payout.id, "reference": "FAIL-REPLAY"})

    entry = db.execute(select(DeadLetter)).scalars().one()
    replay_dead_letter.apply(kwargs={"dead_letter_id": entry.id}).get()

    db.expire_all()
    entry = db.get(DeadLetter, entry.id)
    payout = db.get(Payout, payout.id)

    assert entry.replayed_at is not None
    assert payout.status is PayoutStatus.PENDING  # back to pending
    # old claim gone, otherwise replay does nothing
    stale = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "payout:FAIL-REPLAY")
    ).scalar_one_or_none()
    assert stale is None
    # goes through the outbox again
    events = db.execute(
        select(OutboxEvent).where(OutboxEvent.event_type == "payout.requested")
    ).scalars().all()
    assert len(events) == 1


# transient failure + retry


def test_transient_failure_retries_then_succeeds(db, eager_celery, monkeypatch):
    """
    Fails twice, works on the third try.

    Eager mode quirk: retries only chain when the Retry object is returned
    to apply(). With task_eager_propagates=True it gets raised instead and
    stops after the first retry, so propagation is turned off here. Another
    reason eager mode isn't enough to really test retries.
    """
    from app.services import gateway as gateway_module
    from app.tasks.payouts import process_payout

    calls = {"n": 0}
    real_send = gateway_module.send_payout

    def flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise gateway_module.TransientGatewayError("simulated 503")
        return real_send(**kwargs)

    # payouts.py calls gateway.send_payout(...) so patch it on the module
    monkeypatch.setattr(gateway_module, "send_payout", flaky)
    eager_celery.conf.task_eager_propagates = False

    payout = _make_payout(db, "PO-FLAKY")
    process_payout.apply(
        kwargs={"payout_id": payout.id, "reference": "PO-FLAKY"}, throw=False
    )

    db.expire_all()
    payout = db.get(Payout, payout.id)

    assert calls["n"] == 3
    assert payout.status is PayoutStatus.PAID
    assert payout.attempts == 3  # one per try


def test_missing_payout_is_dead_lettered_not_retried(db, eager_celery):
    # row doesn't exist now, won't exist later either
    from app.tasks.payouts import process_payout

    process_payout.apply(kwargs={"payout_id": 999_999, "reference": "PO-GHOST"})

    entries = db.execute(select(DeadLetter)).scalars().all()
    assert len(entries) == 1
    assert entries[0].error == "payout row not found"


# webhooks


def test_webhook_applies_state_then_ignores_redelivery(db, eager_celery):
    # second delivery of the same event_id should do nothing
    from app.tasks.webhooks import handle_gateway_event

    payout = _make_payout(db, "PO-HOOK")

    kwargs = {
        "event_id": "evt_abc123",
        "event_type": "payout.processed",
        "payload": {"reference": "PO-HOOK", "gateway_reference": "gw_xyz"},
    }
    first = handle_gateway_event.apply(kwargs=kwargs).get()
    second = handle_gateway_event.apply(kwargs=kwargs).get()

    db.expire_all()
    payout = db.get(Payout, payout.id)

    assert first["status"] == "applied"
    assert second == first  # same stored result
    assert payout.status is PayoutStatus.PAID
    assert payout.gateway_reference == "gw_xyz"


def test_webhook_for_unknown_reference_is_absorbed(db, eager_celery):
    from app.tasks.webhooks import handle_gateway_event

    result = handle_gateway_event.apply(
        kwargs={
            "event_id": "evt_unknown",
            "event_type": "payout.processed",
            "payload": {"reference": "PO-NOT-OURS"},
        }
    ).get()

    assert result["status"] == "unknown_reference"


# periodic tasks


def test_outbox_poller_publishes_and_marks_rows(db, eager_celery, monkeypatch):
    from app.tasks import periodic

    sent: list[tuple] = []
    monkeypatch.setattr(
        periodic.celery_app,
        "send_task",
        lambda name, **kw: sent.append((name, kw)) or type("R", (), {"id": "x"})(),
    )

    payout = _make_payout(db, "PO-OUTBOX")
    from app.services.outbox import record_event

    record_event(
        db, "payout.requested", {"payout_id": payout.id, "reference": "PO-OUTBOX"}
    )
    db.commit()

    result = periodic.publish_outbox()

    assert result == {"published": 1, "failed": 0}
    assert sent[0][0] == "payouts.process"
    assert sent[0][1]["kwargs"]["reference"] == "PO-OUTBOX"
    # second run sends nothing
    assert periodic.publish_outbox() == {"published": 0, "failed": 0}


def test_outbox_poller_leaves_rows_pending_when_the_broker_is_down(
    db, eager_celery, monkeypatch
):
    from app.tasks import periodic

    def boom(*a, **kw):
        raise ConnectionError("redis is down")

    monkeypatch.setattr(periodic.celery_app, "send_task", boom)

    from app.services.outbox import record_event

    record_event(db, "payout.requested", {"payout_id": 1, "reference": "PO-DOWN"})
    db.commit()

    assert periodic.publish_outbox() == {"published": 0, "failed": 1}

    event = db.execute(select(OutboxEvent)).scalars().one()
    db.refresh(event)
    assert event.attempts == 1
    assert event.last_error  # still pending, retried next tick


def test_sweeper_requeues_a_payout_whose_worker_died(db, eager_celery):
    # simulate a worker killed after mark_processing: old lock + orphan claim
    from datetime import timedelta

    from app.models import utcnow
    from app.services.idempotency import claim_operation
    from app.tasks import periodic

    payout = _make_payout(db, "PO-STUCK")
    payout.status = PayoutStatus.PROCESSING
    payout.locked_at = utcnow() - timedelta(hours=2)
    claim_operation(db, key="payout:PO-STUCK", task_name="t", task_id="dead")
    db.commit()

    assert periodic.sweep_stuck_payouts() == {"requeued": 1}

    db.expire_all()
    payout = db.get(Payout, payout.id)
    assert payout.status is PayoutStatus.PENDING
    assert payout.locked_at is None
    # orphan claim removed
    assert db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "payout:PO-STUCK")
    ).scalar_one_or_none() is None
    # requeued via outbox
    assert db.execute(
        select(OutboxEvent).where(OutboxEvent.event_type == "payout.requested")
    ).scalars().all()


def test_sweeper_ignores_payouts_that_are_still_within_their_lock_window(
    db, eager_celery
):
    from app.tasks import periodic

    payout = _make_payout(db, "PO-BUSY")
    payout_service.mark_processing(db, payout)  # locked_at = now
    db.commit()

    assert periodic.sweep_stuck_payouts() == {"requeued": 0}


def test_reconciliation_flags_a_payout_the_gateway_never_settled(db, eager_celery):
    from app.tasks import periodic

    paid = _make_payout(db, "PO-SETTLED")
    paid.status = PayoutStatus.PAID
    missing = _make_payout(db, "UNSETTLED-PO-1")
    missing.status = PayoutStatus.PAID
    db.commit()

    result = periodic.reconcile_payouts(window_days=1)

    assert result["checked"] == 2
    assert len(result["mismatches"]) == 1
    assert result["mismatches"][0]["reference"] == "UNSETTLED-PO-1"


def test_purge_removes_only_old_completed_keys(db, eager_celery):
    from datetime import timedelta

    from app.models import utcnow
    from app.services.idempotency import claim_operation, complete_operation
    from app.tasks import periodic

    # old + completed -> deleted
    claim_operation(db, key="old", task_name="t", task_id="a")
    complete_operation(db, "old", {})
    db.execute(select(ProcessedOperation))  # flush the update
    db.commit()
    op = db.execute(
        select(ProcessedOperation).where(ProcessedOperation.key == "old")
    ).scalar_one()
    op.completed_at = utcnow() - timedelta(days=90)

    # recent + completed -> kept
    claim_operation(db, key="recent", task_name="t", task_id="b")
    complete_operation(db, "recent", {})

    # still in progress -> kept, deleting it would free a live worker's claim
    claim_operation(db, key="inflight", task_name="t", task_id="c")
    db.commit()

    assert periodic.purge_idempotency_keys(retain_days=30) == {"purged": 1}

    remaining = {
        row.key for row in db.execute(select(ProcessedOperation)).scalars().all()
    }
    assert remaining == {"recent", "inflight"}
