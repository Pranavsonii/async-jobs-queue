"""
Payout business logic.

No celery stuff in here on purpose. Tasks handle retries/acks/idempotency,
the real logic stays in plain functions. Easier to test (no broker needed),
can be called from scripts/admin too, and if we ever move off celery this
file stays the same.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Payout, PayoutStatus, utcnow
from . import gateway


@dataclass
class PayoutResult:
    payout_id: int
    reference: str
    status: str
    gateway_reference: str | None = None

    def as_dict(self) -> dict:
        # task results have to be json (pickle is off)
        return {
            "payout_id": self.payout_id,
            "reference": self.reference,
            "status": self.status,
            "gateway_reference": self.gateway_reference,
        }


def get_payout(db: Session, payout_id: int) -> Payout | None:
    return db.get(Payout, payout_id)


def get_by_reference(db: Session, reference: str) -> Payout | None:
    return db.execute(
        select(Payout).where(Payout.reference == reference)
    ).scalar_one_or_none()


def create_payout(
    db: Session, *, reference: str, beneficiary: str, amount_paise: int
) -> tuple[Payout, bool]:
    """
    Create a payout or return the existing one with the same reference.
    Returns (payout, created) so the api can send 202 vs 200.

    Clients retry, users double tap, so the http endpoint needs to be
    idempotent too, not just the worker.
    """
    existing = get_by_reference(db, reference)
    if existing is not None:
        return existing, False

    payout = Payout(
        reference=reference,
        beneficiary=beneficiary,
        amount_paise=amount_paise,
        status=PayoutStatus.PENDING,
    )
    db.add(payout)
    db.flush()  # get the id without committing
    return payout, True


def mark_processing(db: Session, payout: Payout) -> None:
    # locked_at lets the sweeper find payouts whose worker died
    payout.status = PayoutStatus.PROCESSING
    payout.locked_at = utcnow()
    payout.attempts += 1


def execute_payout(db: Session, payout: Payout) -> PayoutResult:
    """
    Actually send the money.

    Can raise TransientGatewayError (retry) or PermanentGatewayError (DLQ).
    Not caught here - the task decides what to do with them.
    """
    response = gateway.send_payout(
        reference=payout.reference,
        beneficiary=payout.beneficiary,
        amount_paise=payout.amount_paise,
    )

    payout.status = PayoutStatus.PAID
    payout.gateway_reference = response.gateway_reference
    payout.failure_reason = None
    payout.locked_at = None

    return PayoutResult(
        payout_id=payout.id,
        reference=payout.reference,
        status=payout.status.value,
        gateway_reference=payout.gateway_reference,
    )


def mark_failed(db: Session, payout: Payout, reason: str) -> None:
    payout.status = PayoutStatus.FAILED
    payout.failure_reason = reason[:1000]
    payout.locked_at = None


def reset_for_retry(db: Session, payout: Payout) -> None:
    # back to pending after a transient failure
    payout.status = PayoutStatus.PENDING
    payout.locked_at = None


def idempotency_key(reference: str) -> str:
    # built from the business reference, NOT the task id. otherwise a
    # redelivered message gets a new key and the payout goes out twice
    return f"payout:{reference}"
