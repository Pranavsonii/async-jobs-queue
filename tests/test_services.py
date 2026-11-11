"""
Service + outbox tests. Plain function calls, no celery involved, so these
run super fast.
"""

from __future__ import annotations

import pytest

from app.models import OutboxStatus, PayoutStatus
from app.services import gateway, outbox as outbox_service, payouts as payout_service


# payout logic


def test_create_payout_is_idempotent_on_reference(db):
    first, created_1 = payout_service.create_payout(
        db, reference="PO-1", beneficiary="Asha", amount_paise=10_000
    )
    db.commit()

    second, created_2 = payout_service.create_payout(
        db, reference="PO-1", beneficiary="Someone Else", amount_paise=99_999
    )
    db.commit()

    assert created_1 is True
    assert created_2 is False
    assert first.id == second.id
    # second call shouldn't overwrite anything
    assert second.beneficiary == "Asha"
    assert second.amount_paise == 10_000


def test_execute_payout_marks_paid(db):
    payout, _ = payout_service.create_payout(
        db, reference="PO-2", beneficiary="Asha", amount_paise=10_000
    )
    result = payout_service.execute_payout(db, payout)

    assert payout.status is PayoutStatus.PAID
    assert payout.gateway_reference is not None
    assert payout.locked_at is None
    assert result.as_dict()["status"] == "paid"


def test_mark_processing_records_a_lock_timestamp(db):
    payout, _ = payout_service.create_payout(
        db, reference="PO-3", beneficiary="Asha", amount_paise=10_000
    )
    payout_service.mark_processing(db, payout)

    assert payout.status is PayoutStatus.PROCESSING
    # sweeper needs locked_at to find abandoned payouts
    assert payout.locked_at is not None
    assert payout.attempts == 1


def test_idempotency_key_derives_from_the_business_reference(db):
    # if this ever used a task id / uuid, every redelivery would look new
    assert payout_service.idempotency_key("PO-9") == "payout:PO-9"
    assert payout_service.idempotency_key("PO-9") == payout_service.idempotency_key("PO-9")


# gateway errors


def test_permanent_failures_are_not_retryable():
    with pytest.raises(gateway.PermanentGatewayError):
        gateway.send_payout(reference="FAIL-1", beneficiary="Closed", amount_paise=100)

    with pytest.raises(gateway.PermanentGatewayError):
        gateway.send_payout(reference="PO-1", beneficiary="Asha", amount_paise=-5)

    with pytest.raises(gateway.PermanentGatewayError):
        gateway.send_payout(reference="PO-1", beneficiary="   ", amount_paise=100)


def test_transient_failures_are_retryable():
    with pytest.raises(gateway.TransientGatewayError):
        gateway.send_payout(reference="FLAKY-1", beneficiary="Asha", amount_paise=100)


def test_the_two_failure_types_are_distinguishable():
    # if Permanent subclassed Transient, autoretry_for would retry permanent errors too
    assert not issubclass(gateway.PermanentGatewayError, gateway.TransientGatewayError)
    assert not issubclass(gateway.TransientGatewayError, gateway.PermanentGatewayError)


# outbox


def test_record_event_does_not_commit(db):
    # if record_event committed by itself, rolling back the payout would
    # leave an orphan event
    outbox_service.record_event(db, "payout.requested", {"payout_id": 1})
    db.rollback()

    assert outbox_service.fetch_unpublished(db) == []


def test_business_row_and_event_commit_together(db):
    payout, _ = payout_service.create_payout(
        db, reference="PO-4", beneficiary="Asha", amount_paise=10_000
    )
    outbox_service.record_event(
        db, "payout.requested", {"payout_id": payout.id, "reference": "PO-4"}
    )
    db.commit()

    events = outbox_service.fetch_unpublished(db)
    assert len(events) == 1
    assert events[0].payload["reference"] == "PO-4"
    assert payout_service.get_by_reference(db, "PO-4") is not None


def test_rollback_loses_both_or_neither(db):
    payout_service.create_payout(
        db, reference="PO-5", beneficiary="Asha", amount_paise=10_000
    )
    outbox_service.record_event(db, "payout.requested", {"reference": "PO-5"})
    db.rollback()

    assert payout_service.get_by_reference(db, "PO-5") is None
    assert outbox_service.fetch_unpublished(db) == []


def test_fetch_unpublished_is_fifo(db):
    for i in range(3):
        outbox_service.record_event(db, "payout.requested", {"n": i})
    db.commit()

    events = outbox_service.fetch_unpublished(db)
    assert [e.payload["n"] for e in events] == [0, 1, 2]


def test_published_events_are_not_refetched(db):
    outbox_service.record_event(db, "payout.requested", {"n": 1})
    db.commit()

    event = outbox_service.fetch_unpublished(db)[0]
    outbox_service.mark_published(db, event)
    db.commit()

    assert outbox_service.fetch_unpublished(db) == []
    assert event.status is OutboxStatus.PUBLISHED
    assert event.published_at is not None


def test_publish_failures_stay_pending_then_eventually_give_up(db):
    # failed publish stays pending for retry, but after 10 failures we give up
    outbox_service.record_event(db, "payout.requested", {"n": 1})
    db.commit()
    event = outbox_service.fetch_unpublished(db)[0]

    for _ in range(9):
        outbox_service.mark_failed(db, event, "broker down")
    db.commit()
    assert event.status is OutboxStatus.PENDING   # still retrying
    assert len(outbox_service.fetch_unpublished(db)) == 1

    outbox_service.mark_failed(db, event, "broker down")
    db.commit()
    assert event.status is OutboxStatus.FAILED    # gave up, row kept
    assert outbox_service.fetch_unpublished(db) == []
