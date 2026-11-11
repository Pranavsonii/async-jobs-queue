"""
Scheduled tasks (enqueued by celery beat).

Beat only enqueues, normal workers run these. Only run ONE beat, otherwise
every job here gets enqueued twice.

  publish_outbox           move committed outbox rows into the queue (every 5s)
  sweep_stuck_payouts      requeue payouts whose worker died (every minute)
  reconcile_payouts        compare our records with the gateway (nightly)
  purge_idempotency_keys   clean up old ledger rows (nightly)
"""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import delete, select

from ..celery_app import celery_app
from ..config import settings
from ..db import session_scope
from ..models import (
    OperationStatus,
    Payout,
    PayoutStatus,
    ProcessedOperation,
    utcnow,
)
from ..services import gateway
from ..services import outbox as outbox_service
from ..services import payouts as payout_service

log = logging.getLogger(__name__)


# --- outbox poller ---
# only place that actually enqueues jobs. everything else just writes an
# outbox row and this picks it up.

# event type -> task name. add a line here for new event types
EVENT_ROUTES: dict[str, str] = {
    "payout.requested": "payouts.process",
    "payout.succeeded": "notifications.send_receipt",
}


@celery_app.task(name="periodic.publish_outbox")
def publish_outbox(batch_size: int = 50) -> dict:
    published = 0
    failed = 0

    with session_scope() as db:
        # rows stay locked for this whole transaction, so keep batches small
        events = outbox_service.fetch_unpublished(db, limit=batch_size)

        for event in events:
            task_name = EVENT_ROUTES.get(event.event_type)
            if task_name is None:
                outbox_service.mark_failed(db, event, f"no route for {event.event_type}")
                failed += 1
                continue

            try:
                payload = event.payload
                if event.event_type == "payout.requested":
                    celery_app.send_task(
                        task_name,
                        kwargs={
                            "payout_id": payload["payout_id"],
                            "reference": payload["reference"],
                        },
                    )
                elif event.event_type == "payout.succeeded":
                    celery_app.send_task(
                        task_name, kwargs={"payout_id": payload["payout_id"]}
                    )

                outbox_service.mark_published(db, event)
                published += 1

            except Exception as exc:  # broker down etc
                # stays PENDING, next tick will retry. broker outage just
                # delays jobs, doesn't lose them
                outbox_service.mark_failed(db, event, str(exc))
                failed += 1
                log.warning("outbox %s publish failed: %s", event.id, exc)

    if published or failed:
        log.info("outbox: published=%s failed=%s", published, failed)
    return {"published": published, "failed": failed}


# --- crash recovery ---


@celery_app.task(name="periodic.sweep_stuck_payouts")
def sweep_stuck_payouts() -> dict:
    """
    Requeue payouts stuck in PROCESSING for too long.

    Happens when the worker gets killed (OOM, docker kill) after
    mark_processing but before the commit. The broker would redeliver
    eventually, but only after the visibility timeout (1h), so this is the
    quicker fallback. If both fire, idempotency takes care of it.
    """
    cutoff = utcnow() - timedelta(seconds=settings.stuck_payout_seconds)
    requeued = 0

    with session_scope() as db:
        stuck = db.execute(
            select(Payout).where(
                Payout.status == PayoutStatus.PROCESSING,
                Payout.locked_at.is_not(None),
                Payout.locked_at < cutoff,
            )
        ).scalars().all()

        for payout in stuck:
            payout_service.reset_for_retry(db, payout)

            # remove the dead worker's claim, otherwise the requeued job
            # sees IN_FLIGHT and skips
            key = payout_service.idempotency_key(payout.reference)
            claim = db.execute(
                select(ProcessedOperation).where(ProcessedOperation.key == key)
            ).scalar_one_or_none()
            if claim is not None and claim.status is OperationStatus.IN_PROGRESS:
                db.delete(claim)

            outbox_service.record_event(
                db,
                "payout.requested",
                {"payout_id": payout.id, "reference": payout.reference},
            )
            requeued += 1
            log.warning("swept stuck payout %s back into the queue", payout.reference)

    return {"requeued": requeued}


# --- reconciliation ---


@celery_app.task(name="periodic.reconcile_payouts")
def reconcile_payouts(window_days: int = 1) -> dict:
    """
    Compare our PAID payouts with the gateway's settlement data.

    Retries and sweepers can still miss stuff, gateways can reverse payments.
    This is the daily check that catches whatever slipped through.
    """
    since = utcnow() - timedelta(days=window_days)
    mismatches: list[dict] = []

    with session_scope() as db:
        ours = db.execute(
            select(Payout).where(
                Payout.status == PayoutStatus.PAID,
                Payout.created_at >= since,
            )
        ).scalars().all()

        settled = gateway.fetch_settled_references([p.reference for p in ours])

        for payout in ours:
            if payout.reference not in settled:
                # we think it's paid, gateway doesn't. don't auto fix,
                # flag it for someone to check
                mismatches.append(
                    {
                        "reference": payout.reference,
                        "amount_paise": payout.amount_paise,
                        "issue": "marked paid locally but not settled by gateway",
                    }
                )

        checked = len(ours)

    if mismatches:
        from .notifications import alert_oncall

        alert_oncall.delay(
            "reconciliation mismatch",
            f"{len(mismatches)} payout(s) do not match the gateway",
        )

    log.info("reconciliation: checked=%s mismatches=%s", checked, len(mismatches))
    return {"checked": checked, "mismatches": mismatches}


# --- housekeeping ---


@celery_app.task(name="periodic.purge_idempotency_keys")
def purge_idempotency_keys(retain_days: int = 30) -> dict:
    """
    Delete completed ledger rows older than retain_days.

    30 days is plenty for any duplicate to show up (redelivery, webhook
    retries, manual retries). Only COMPLETED rows - deleting an in-progress
    one would free a live worker's claim.
    """
    cutoff = utcnow() - timedelta(days=retain_days)

    with session_scope() as db:
        result = db.execute(
            delete(ProcessedOperation).where(
                ProcessedOperation.status == OperationStatus.COMPLETED,
                ProcessedOperation.completed_at < cutoff,
            )
        )
        purged = result.rowcount or 0

    log.info("purged %s idempotency keys older than %s days", purged, retain_days)
    return {"purged": purged}
