"""
Request / response schemas.

Validate at the api so bad data never reaches the queue. A bad request fails
right away in front of the user; a bad message fails inside a worker later,
retries 5 times and ends up in the DLQ.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class PayoutCreate(BaseModel):
    # client side idempotency key, required on purpose - if it's optional
    # people skip it and retries create duplicate payouts
    reference: str = Field(min_length=3, max_length=64)
    beneficiary: str = Field(min_length=1, max_length=128)
    # integer paise, no floats (199.99 -> 199.98999999999998)
    amount_paise: int = Field(gt=0, le=10_000_000_00)

    @field_validator("reference")
    @classmethod
    def no_whitespace(cls, v: str) -> str:
        v = v.strip()
        if not v or " " in v:
            raise ValueError("reference must be non-empty and contain no spaces")
        return v


class PayoutRead(BaseModel):
    id: int
    reference: str
    beneficiary: str
    amount_paise: int
    currency: str
    status: str
    attempts: int
    gateway_reference: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class PayoutAccepted(BaseModel):
    """
    Response for POST /payouts. Not the finished payout because it isn't
    finished yet - just "accepted, poll here".
    """

    payout_id: int
    reference: str
    status: str
    created: bool
    poll_url: str


class TaskStatus(BaseModel):
    task_id: str
    state: str          # PENDING / STARTED / SUCCESS / FAILURE / RETRY / REVOKED
    ready: bool
    successful: bool | None = None
    result: dict | list | str | None = None
    error: str | None = None


class WebhookEvent(BaseModel):
    """Roughly the shape razorpay etc. would send."""

    event_id: str = Field(min_length=1, max_length=128)
    event_type: str
    payload: dict


class QueueDepth(BaseModel):
    queue: str
    depth: int


class SystemStats(BaseModel):
    # queue depth is the main health signal, rest is extra info
    queues: list[QueueDepth]
    payouts_by_status: dict[str, int]
    outbox_pending: int
    dead_letters: int
    idempotency_keys: int
