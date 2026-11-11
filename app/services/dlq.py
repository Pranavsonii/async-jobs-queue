"""
Dead letter queue.

Some jobs will never succeed. Retrying forever blocks workers, swallowing the
error loses the payment with no trace. So we park the job with its args, error
and traceback, and it can be replayed once the problem is fixed.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import DeadLetter, utcnow

log = logging.getLogger(__name__)


def dead_letter(
    db: Session,
    *,
    task_name: str,
    task_args: dict[str, Any],
    error: str,
    task_id: str | None = None,
    traceback: str | None = None,
) -> DeadLetter:
    """
    Store a failed job. Doesn't commit - in the payout task this goes in the
    same transaction that marks the payout FAILED, so both always match.
    """
    entry = DeadLetter(
        task_name=task_name,
        task_id=task_id,
        task_args=task_args,
        error=error[:2000],
        traceback=(traceback or "")[:8000] or None,
    )
    db.add(entry)

    # loud log line on purpose, alerting can match on DEAD_LETTER
    log.error(
        "DEAD_LETTER task=%s task_id=%s args=%s error=%s",
        task_name,
        task_id,
        task_args,
        error,
    )
    return entry


def pending_count(db: Session) -> int:
    # entries not replayed yet. should be 0, anything else needs a look
    return (
        db.execute(
            select(func.count())
            .select_from(DeadLetter)
            .where(DeadLetter.replayed_at.is_(None))
        ).scalar_one()
    )


def mark_replayed(db: Session, entry: DeadLetter) -> None:
    entry.replayed_at = utcnow()
