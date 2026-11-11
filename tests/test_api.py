"""
API tests. Mostly checking that endpoints only accept work and return,
they never do the work inline.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.main import app
from app.models import OutboxEvent, Payout, PayoutStatus

client = TestClient(app)


def test_create_returns_202_and_a_poll_url(db):
    r = client.post(
        "/payouts",
        json={"reference": "PO-A1", "beneficiary": "Asha", "amount_paise": 50_000},
    )

    assert r.status_code == 202  # accepted, not done yet
    body = r.json()
    assert body["created"] is True
    assert body["status"] == "pending"
    assert body["poll_url"] == f"/payouts/{body['payout_id']}"


def test_create_writes_the_payout_and_the_outbox_row_together(db):
    client.post(
        "/payouts",
        json={"reference": "PO-A2", "beneficiary": "Asha", "amount_paise": 50_000},
    )

    payout = db.execute(
        select(Payout).where(Payout.reference == "PO-A2")
    ).scalar_one()
    event = db.execute(
        select(OutboxEvent).where(OutboxEvent.event_type == "payout.requested")
    ).scalar_one()

    assert payout.status is PayoutStatus.PENDING
    assert event.payload["payout_id"] == payout.id


def test_posting_the_same_reference_twice_does_not_duplicate(db):
    body = {"reference": "PO-A3", "beneficiary": "Asha", "amount_paise": 50_000}

    first = client.post("/payouts", json=body)
    second = client.post("/payouts", json=body)

    assert first.status_code == 202
    assert second.status_code == 200          # already existed
    assert second.json()["created"] is False
    assert first.json()["payout_id"] == second.json()["payout_id"]

    # only one outbox event, so the worker only gets it once
    events = db.execute(select(OutboxEvent)).scalars().all()
    assert len(events) == 1


def test_validation_rejects_bad_input_before_it_becomes_a_message(db):
    bad_amount = client.post(
        "/payouts",
        json={"reference": "PO-A4", "beneficiary": "Asha", "amount_paise": 0},
    )
    bad_reference = client.post(
        "/payouts",
        json={"reference": "has spaces", "beneficiary": "Asha", "amount_paise": 100},
    )
    missing = client.post("/payouts", json={"reference": "PO-A5"})

    assert bad_amount.status_code == 422
    assert bad_reference.status_code == 422
    assert missing.status_code == 422
    assert db.execute(select(Payout)).scalars().all() == []


def test_read_payout(db):
    created = client.post(
        "/payouts",
        json={"reference": "PO-A6", "beneficiary": "Asha", "amount_paise": 50_000},
    ).json()

    r = client.get(f"/payouts/{created['payout_id']}")

    assert r.status_code == 200
    assert r.json()["reference"] == "PO-A6"
    assert r.json()["status"] == "pending"


def test_read_missing_payout_404s(db):
    assert client.get("/payouts/999999").status_code == 404


def test_webhook_endpoint_only_enqueues(db, monkeypatch):
    from app import main

    sent = {}
    monkeypatch.setattr(
        main.celery_app,
        "send_task",
        lambda name, **kw: sent.update(name=name, **kw) or type("R", (), {"id": "t1"})(),
    )

    r = client.post(
        "/webhooks/gateway",
        json={
            "event_id": "evt_1",
            "event_type": "payout.processed",
            "payload": {"reference": "PO-A7"},
        },
    )

    assert r.status_code == 202
    assert sent["name"] == "webhooks.gateway_event"
    assert sent["kwargs"]["event_id"] == "evt_1"


def test_stats_reports_queue_depth_and_counts(db):
    client.post(
        "/payouts",
        json={"reference": "PO-A8", "beneficiary": "Asha", "amount_paise": 50_000},
    )

    body = client.get("/stats").json()

    assert body["payouts_by_status"]["pending"] == 1
    assert body["outbox_pending"] == 1
    assert body["dead_letters"] == 0
    # no redis in tests so depth is -1, but /stats should still work
    assert {q["queue"] for q in body["queues"]} == {
        "default",
        "payouts",
        "fast",
        "heavy",
    }


def test_health_reports_each_dependency_separately(db):
    body = client.get("/health").json()

    assert body["api"] == "ok"
    assert body["database"] == "ok"
    assert "redis" in body  # can be an error string in CI, that's fine
