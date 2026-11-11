"""
Statement report using a celery chord (fan-out / fan-in).

This is the CPU heavy part, so it runs on its own `heavy` queue and worker.
If it shared a queue with receipts, every email would wait behind the report.

    chord(group of N tasks)(callback)

N summarise_chunk tasks run in parallel, when all are done celery calls
build_statement with the list of their results.

Note: chords need a result backend (the callback reads the stored results),
so these tasks can't use ignore_result.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from celery import chord

from ..celery_app import celery_app
from ..db import session_scope
from ..models import Payout, PayoutStatus, utcnow

log = logging.getLogger(__name__)

CHUNK_SIZE = 50


@celery_app.task(name="reports.summarise_chunk")
def summarise_chunk(payout_ids: list[int]) -> dict:
    # return a small summary, not the rows - every result goes through redis
    with session_scope() as db:
        payouts = db.query(Payout).filter(Payout.id.in_(payout_ids)).all()

        total = sum(p.amount_paise for p in payouts if p.status is PayoutStatus.PAID)
        return {
            "count": len(payouts),
            "paid": sum(1 for p in payouts if p.status is PayoutStatus.PAID),
            "failed": sum(1 for p in payouts if p.status is PayoutStatus.FAILED),
            "pending": sum(
                1
                for p in payouts
                if p.status in (PayoutStatus.PENDING, PayoutStatus.PROCESSING)
            ),
            "total_paise": total,
        }


@celery_app.task(name="reports.build_statement")
def build_statement(chunk_results: list[dict], window_days: int = 7) -> dict:
    # chord callback, first arg is the list of all chunk results
    summary = {
        "window_days": window_days,
        "chunks": len(chunk_results),
        "count": sum(c["count"] for c in chunk_results),
        "paid": sum(c["paid"] for c in chunk_results),
        "failed": sum(c["failed"] for c in chunk_results),
        "pending": sum(c["pending"] for c in chunk_results),
        "total_paise": sum(c["total_paise"] for c in chunk_results),
        "generated_at": utcnow().isoformat(),
    }
    summary["total_inr"] = round(summary["total_paise"] / 100, 2)
    log.info("statement built: %s", summary)
    return summary


@celery_app.task(bind=True, name="reports.generate_statement")
def generate_statement(self, window_days: int = 7) -> dict:
    """
    Split payouts into chunks and start the chord. Returns right away -
    poll the chord_id for the final statement, not this task's id.
    """
    with session_scope() as db:
        since = utcnow() - timedelta(days=window_days)
        ids = [
            row[0]
            for row in db.query(Payout.id).filter(Payout.created_at >= since).all()
        ]

    if not ids:
        return {"count": 0, "chunks": 0, "note": "nothing in window"}

    chunks = [ids[i : i + CHUNK_SIZE] for i in range(0, len(ids), CHUNK_SIZE)]

    # .s() = signature (task + args, not sent yet), kind of like functools.partial
    header = [summarise_chunk.s(chunk) for chunk in chunks]
    callback = build_statement.s(window_days=window_days)

    async_result = chord(header)(callback)

    return {
        "chord_id": async_result.id,   # poll this one
        "chunks": len(chunks),
        "payouts": len(ids),
    }
