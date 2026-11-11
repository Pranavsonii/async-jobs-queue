"""
Incoming webhooks from the gateway.

Providers send webhooks at-least-once - if their POST times out they just send
it again. So we WILL get the same event more than once.

Same approach as any other job: accept fast, process in background, dedupe on
the provider's event id. The endpoint has to reply quickly, otherwise the
provider thinks it failed and retries while we're still processing.
"""

from __future__ import annotations

import logging

from celery.exceptions import Ignore

from ..celery_app import celery_app
from ..db import session_scope
from ..models import PayoutStatus
from ..services import payouts as payout_service
from ..services.idempotency import ClaimOutcome, claim_operation, complete_operation

log = logging.getLogger(__name__)


@celery_app.task(bind=True, name="webhooks.gateway_event")
def handle_gateway_event(self, event_id: str, event_type: str, payload: dict) -> dict:
    """
    Process one webhook event.

    Key is the provider's event id - that's the only thing that stays the
    same across their redeliveries.
    """
    key = f"webhook:{event_id}"

    with session_scope() as db:
        claim = claim_operation(
            db, key=key, task_name=self.name, task_id=self.request.id
        )

        if claim.outcome is ClaimOutcome.DUPLICATE:
            log.info("webhook %s: duplicate, ignored", event_id)
            return claim.previous_result or {"status": "duplicate_ignored"}

        if claim.outcome is ClaimOutcome.IN_FLIGHT:
            raise Ignore()

        reference = payload.get("reference")
        payout = (
            payout_service.get_by_reference(db, reference) if reference else None
        )

        if payout is None:
            # event for a payout we don't have. no retry, just mark it
            # handled so redeliveries are cheap
            result = {"status": "unknown_reference", "reference": reference}
            complete_operation(db, key, result)
            log.warning("webhook %s: unknown reference %s", event_id, reference)
            return result

        # map provider event -> our status
        if event_type in ("payout.processed", "payment.captured"):
            payout.status = PayoutStatus.PAID
            payout.gateway_reference = payload.get("gateway_reference")
            payout.failure_reason = None
        elif event_type in ("payout.reversed", "payout.failed"):
            payout.status = PayoutStatus.FAILED
            payout.failure_reason = payload.get("reason", "reversed by gateway")
        else:
            result = {"status": "ignored_event_type", "event_type": event_type}
            complete_operation(db, key, result)
            return result

        result = {
            "status": "applied",
            "reference": reference,
            "new_status": payout.status.value,
        }
        # same transaction as the status change
        complete_operation(db, key, result)
        log.info("webhook %s: %s -> %s", event_id, reference, payout.status.value)
        return result
