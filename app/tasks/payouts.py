"""
Payout task - this is where everything comes together.

    delivered twice?          -> idempotency claim
    gateway down for a bit?   -> autoretry + backoff + jitter
    account closed?           -> dead letter, no retry
    worker killed midway?     -> acks_late + stuck payout sweeper
    task hangs?               -> soft/hard time limits
"""

from __future__ import annotations

import logging
import traceback as tb_module

from celery.exceptions import Ignore, SoftTimeLimitExceeded

from ..celery_app import celery_app
from ..config import settings
from ..db import session_scope
from ..models import PayoutStatus
from ..services import dlq, payouts as payout_service
from ..services.gateway import PermanentGatewayError, TransientGatewayError
from ..services.idempotency import (
    ClaimOutcome,
    claim_operation,
    complete_operation,
    release_operation,
)

log = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="payouts.process",          # fixed name so renaming the function
                                     # doesn't orphan queued messages
    # retries
    autoretry_for=(TransientGatewayError,),
    retry_backoff=True,              # 1s, 2s, 4s, 8s...
    retry_backoff_max=settings.retry_backoff_max,
    retry_jitter=True,               # random delay so failed tasks don't all
                                     # retry at the same moment and knock the
                                     # gateway over again
    max_retries=settings.max_retries,
    # time limits
    soft_time_limit=settings.payout_soft_time_limit,  # raises, we can clean up
    time_limit=settings.payout_hard_time_limit,       # hard kill
)
def process_payout(self, payout_id: int, reference: str) -> dict:
    """
    Pay one beneficiary. Delivered at least once, but only takes effect once.

    Takes ids not objects - keeps messages small in redis and we always read
    the latest state from the db instead of a stale snapshot.
    """
    key = payout_service.idempotency_key(reference)

    with session_scope() as db:
        # 1. claim it before touching money
        claim = claim_operation(
            db, key=key, task_name=self.name, task_id=self.request.id
        )

        if claim.outcome is ClaimOutcome.DUPLICATE:
            # already done, return the original result
            log.info("payout %s: duplicate delivery ignored", reference)
            return claim.previous_result or {"status": "duplicate_skipped"}

        if claim.outcome is ClaimOutcome.IN_FLIGHT:
            # another worker has it. Ignore = no retry, no failure
            log.info("payout %s: already in flight elsewhere", reference)
            raise Ignore()

        # 2. load
        payout = payout_service.get_payout(db, payout_id)
        if payout is None:
            # row won't show up later, no point retrying
            dlq.dead_letter(
                db,
                task_name=self.name,
                task_id=self.request.id,
                task_args={"payout_id": payout_id, "reference": reference},
                error="payout row not found",
            )
            # commit before raising, session_scope rolls back on exceptions
            # and we'd lose the dead letter
            db.commit()
            raise Ignore()

        if payout.status is PayoutStatus.PAID:
            # ledger says not done but payout is paid - trust the payout, don't pay again
            complete_operation(db, key, {"status": "already_paid"})
            return {"status": "already_paid", "reference": reference}

        payout_service.mark_processing(db, payout)
        db.flush()

        # 3. do it
        try:
            result = payout_service.execute_payout(db, payout)

        except TransientGatewayError as exc:
            # release the claim so the retry isn't blocked by us, then
            # re-raise so autoretry kicks in
            payout_service.reset_for_retry(db, payout)
            release_operation(db, key)
            db.commit()
            log.warning(
                "payout %s: transient failure (attempt %s/%s): %s",
                reference,
                self.request.retries + 1,
                self.max_retries,
                exc,
            )
            raise

        except PermanentGatewayError as exc:
            # no point retrying. mark failed + DLQ in one transaction,
            # keep the claim so a redelivery doesn't try again
            payout_service.mark_failed(db, payout, str(exc))
            dlq.dead_letter(
                db,
                task_name=self.name,
                task_id=self.request.id,
                task_args={"payout_id": payout_id, "reference": reference},
                error=str(exc),
                traceback=tb_module.format_exc(),
            )
            complete_operation(db, key, {"status": "failed", "reason": str(exc)})
            db.commit()
            log.error("payout %s: permanent failure: %s", reference, exc)
            # already handled, don't let celery retry it
            raise Ignore()

        except SoftTimeLimitExceeded:
            # few seconds left before the hard kill, leave things clean
            # instead of stuck in PROCESSING
            payout_service.reset_for_retry(db, payout)
            release_operation(db, key)
            db.commit()
            log.error("payout %s: soft time limit exceeded", reference)
            raise

        # 4. PAID + ledger COMPLETED in the same commit, so either both
        # happen or neither does
        complete_operation(db, key, result.as_dict())

        # receipt goes through the outbox, not .delay(), since this
        # transaction isn't committed yet
        from ..services.outbox import record_event

        record_event(
            db,
            "payout.succeeded",
            {"payout_id": payout.id, "reference": payout.reference},
        )

        log.info("payout %s: paid (%s)", reference, result.gateway_reference)
        return result.as_dict()


@celery_app.task(bind=True, name="payouts.replay")
def replay_dead_letter(self, dead_letter_id: int) -> dict:
    """
    Re-queue a job from the DLQ after it's been fixed.

    Clears the old idempotency claim first - it was marked completed (as
    failed) and would turn the replay into a no-op otherwise.
    """
    from sqlalchemy import select

    from ..models import DeadLetter, ProcessedOperation

    with session_scope() as db:
        entry = db.get(DeadLetter, dead_letter_id)
        if entry is None:
            raise Ignore()

        args = entry.task_args
        reference = args.get("reference")

        # remove old claim so the replay can actually run
        if reference:
            key = payout_service.idempotency_key(reference)
            stale = db.execute(
                select(ProcessedOperation).where(ProcessedOperation.key == key)
            ).scalar_one_or_none()
            if stale is not None:
                db.delete(stale)

            payout = payout_service.get_by_reference(db, reference)
            if payout is not None:
                payout_service.reset_for_retry(db, payout)
                payout.failure_reason = None

        dlq.mark_replayed(db, entry)

        # through the outbox again, don't enqueue inside an open transaction
        from ..services.outbox import record_event

        record_event(db, "payout.requested", {"payout_id": args["payout_id"],
                                              "reference": reference})

    return {"replayed": dead_letter_id, "reference": reference}
