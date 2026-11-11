"""
Fake payment gateway.

If the demo always succeeds you never see retries or the DLQ, so this one
behaves like a real gateway:
  - it's slow (so a queue builds up)
  - it fails sometimes temporarily (-> retry with backoff)
  - it fails permanently for some inputs (-> DLQ)

Transient = timeout, 503, rate limit. Try later and it'll probably work.
Permanent = invalid account, bad amount. Will fail the same way forever.

Getting this split right matters: permanent treated as transient = useless
retries, transient treated as permanent = dropping a payment that would've
gone through.
"""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass

from ..config import settings


class GatewayError(Exception):
    """Base error."""


class TransientGatewayError(GatewayError):
    """Safe to retry."""


class PermanentGatewayError(GatewayError):
    """Don't retry, goes to DLQ."""


@dataclass
class GatewayResponse:
    gateway_reference: str
    status: str


# own Random instance so we don't mess with the global random module.
# with a seed tests get the same failures every time
_rng = random.Random(settings.gateway_seed)


def send_payout(
    *,
    reference: str,
    beneficiary: str,
    amount_paise: int,
) -> GatewayResponse:
    """
    Pretend to send money.

    For demo/testing from curl:
        reference starts with "FAIL-"  -> permanent failure (DLQ)
        reference starts with "FLAKY-" -> transient failure (retry)
    """
    # fake network delay, otherwise everything is instant and there's no queue to see
    time.sleep(settings.gateway_latency_ms / 1000)

    # basic validation, these will never pass on retry
    if amount_paise <= 0:
        raise PermanentGatewayError("amount must be greater than zero")
    if amount_paise > 10_000_000_00:  # 10 lakh limit, just picked a number
        raise PermanentGatewayError("amount exceeds per-transaction limit")
    if not beneficiary.strip():
        raise PermanentGatewayError("beneficiary is required")

    if reference.startswith("FAIL-"):
        raise PermanentGatewayError("beneficiary account is closed")
    if reference.startswith("FLAKY-"):
        raise TransientGatewayError("gateway timeout (simulated)")

    # random failures so retries actually happen
    if _rng.random() < settings.gateway_failure_rate:
        raise TransientGatewayError("gateway returned 503 (simulated)")

    return GatewayResponse(
        gateway_reference=f"gw_{uuid.uuid4().hex[:16]}",
        status="processed",
    )


def fetch_settled_references(references: list[str]) -> set[str]:
    """
    Fake settlement file for the nightly reconciliation.
    Everything is settled except refs starting with "UNSETTLED-".
    """
    return {ref for ref in references if not ref.startswith("UNSETTLED-")}
