"""
Fire and forget tasks.

Compare with payouts.py - an email doesn't need an idempotency ledger, DLQ or
stored result. Duplicate email = mildly annoying, duplicate payout = real
problem. So keep this simple.
"""

from __future__ import annotations

import logging
import time

from ..celery_app import celery_app
from ..db import session_scope
from ..services import payouts as payout_service

log = logging.getLogger(__name__)


@celery_app.task(
    name="notifications.send_receipt",
    # nobody will check if the receipt was sent, so don't store a result
    # in redis for every single email
    ignore_result=True,
    # emails are cheap to resend, a few quick retries is enough
    autoretry_for=(Exception,),
    retry_backoff=True,
    max_retries=3,
    # smtp providers throttle. note: this limit is per worker process,
    # not global
    rate_limit="60/m",
)
def send_receipt(payout_id: int) -> None:
    """'Send' a receipt email."""
    with session_scope() as db:
        payout = payout_service.get_payout(db, payout_id)
        if payout is None:
            log.warning("receipt for missing payout %s -- dropping", payout_id)
            return

        recipient = payout.beneficiary
        amount = payout.amount_paise / 100
        reference = payout.reference

    # session is closed before the slow part, don't hold a db connection
    # while waiting on smtp
    time.sleep(0.2)  # pretend smtp
    log.info("EMAIL -> %s: payout %s of INR %.2f completed", recipient, reference, amount)


@celery_app.task(name="notifications.alert_oncall", ignore_result=True)
def alert_oncall(subject: str, detail: str) -> None:
    # placeholder for pagerduty/slack. it's a task so a slow alerting
    # service never blocks the payment flow
    log.error("ALERT: %s -- %s", subject, detail)
