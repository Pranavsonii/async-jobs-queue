"""
FastAPI app - accepts work and returns quickly.

Two rules in this file:
  - no endpoint does the actual work, the user is waiting on the request
  - no endpoint calls .delay(), we write an outbox row instead and beat
    enqueues after the commit. (/webhooks, /reports and /dlq/replay send
    directly since they don't write any business data.)
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import redis
from celery.result import AsyncResult
from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .celery_app import celery_app
from .config import settings
from .db import engine, get_db
from .models import (
    DeadLetter,
    OutboxEvent,
    OutboxStatus,
    Payout,
    ProcessedOperation,
    create_all,
)
from .schemas import (
    PayoutAccepted,
    PayoutCreate,
    PayoutRead,
    QueueDepth,
    SystemStats,
    TaskStatus,
    WebhookEvent,
)
from .services import payouts as payout_service
from .services.outbox import record_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # create_all is ok for the demo. in prod run alembic migrations
    # before starting the app
    create_all(engine)
    log.info("schema ready")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="PayFlow Jobs",
    description=(
        "A production-shaped background job system: FastAPI producer, Redis "
        "broker, Celery workers, transactional outbox, idempotency ledger, "
        "dead letter queue."
    ),
    version="1.0.0",
)

# same queues as celery_app.task_routes, used by /stats
QUEUES = ["default", "payouts", "fast", "heavy"]

_redis = redis.Redis.from_url(settings.redis_broker_url)


# --- payouts ---


@app.post(
    "/payouts",
    response_model=PayoutAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["payouts"],
)
def create_payout(
    body: PayoutCreate, response: Response, db: Session = Depends(get_db)
) -> PayoutAccepted:
    """
    Accept a payout. Returns 202 (accepted, not done yet) - client polls
    poll_url for the result.

    Payout row and outbox row are written in one transaction, so either
    both exist or neither does.
    """
    payout, created = payout_service.create_payout(
        db,
        reference=body.reference,
        beneficiary=body.beneficiary,
        amount_paise=body.amount_paise,
    )

    if created:
        record_event(
            db,
            "payout.requested",
            {"payout_id": payout.id, "reference": payout.reference},
        )
    else:
        # already seen this reference, don't enqueue again
        response.status_code = status.HTTP_200_OK

    return PayoutAccepted(
        payout_id=payout.id,
        reference=payout.reference,
        status=payout.status.value,
        created=created,
        poll_url=f"/payouts/{payout.id}",
    )


@app.get("/payouts/{payout_id}", response_model=PayoutRead, tags=["payouts"])
def read_payout(payout_id: int, db: Session = Depends(get_db)) -> Payout:
    payout = payout_service.get_payout(db, payout_id)
    if payout is None:
        raise HTTPException(404, "payout not found")
    return payout


@app.get("/payouts", response_model=list[PayoutRead], tags=["payouts"])
def list_payouts(limit: int = 50, db: Session = Depends(get_db)) -> list[Payout]:
    return list(
        db.execute(select(Payout).order_by(Payout.id.desc()).limit(min(limit, 200)))
        .scalars()
        .all()
    )


# --- task status ---


@app.get("/tasks/{task_id}", response_model=TaskStatus, tags=["tasks"])
def read_task(task_id: str) -> TaskStatus:
    """
    Poll a celery task.

    Careful: PENDING doesn't mean queued. Celery has no record of a task until
    a worker touches it, so a wrong id also shows PENDING forever. That's why
    the real state lives in payouts.status.
    """
    result = AsyncResult(task_id, app=celery_app)

    payload = TaskStatus(
        task_id=task_id,
        state=result.state,
        ready=result.ready(),
    )

    if result.ready():
        payload.successful = result.successful()
        if result.successful():
            payload.result = result.result
        else:
            # report the error, don't raise it here
            payload.error = str(result.result)

    return payload


# --- webhooks ---


@app.post("/webhooks/gateway", status_code=status.HTTP_202_ACCEPTED, tags=["webhooks"])
def gateway_webhook(event: WebhookEvent) -> dict:
    """
    Gateway webhook. Just validate, enqueue and return - providers time out
    fast and retry. The task dedupes on event_id.

    Enqueues directly (no outbox) since there's no db write here. If the
    enqueue fails we return 5xx and the provider retries.
    """
    async_result = celery_app.send_task(
        "webhooks.gateway_event",
        kwargs={
            "event_id": event.event_id,
            "event_type": event.event_type,
            "payload": event.payload,
        },
    )
    return {"accepted": True, "task_id": async_result.id}


# --- reports ---


@app.post("/reports/statement", status_code=status.HTTP_202_ACCEPTED, tags=["reports"])
def request_statement(window_days: int = 7) -> dict:
    # starts the chord on the heavy queue. the task returns a chord_id,
    # poll that at /tasks/{id} for the actual statement
    async_result = celery_app.send_task(
        "reports.generate_statement", kwargs={"window_days": window_days}
    )
    return {"task_id": async_result.id, "poll_url": f"/tasks/{async_result.id}"}


# --- dead letters ---


@app.get("/dlq", tags=["ops"])
def list_dead_letters(db: Session = Depends(get_db)) -> list[dict]:
    entries = (
        db.execute(select(DeadLetter).order_by(DeadLetter.id.desc()).limit(100))
        .scalars()
        .all()
    )
    return [
        {
            "id": e.id,
            "task_name": e.task_name,
            "task_args": e.task_args,
            "error": e.error,
            "created_at": e.created_at,
            "replayed_at": e.replayed_at,
        }
        for e in entries
    ]


@app.post("/dlq/{dead_letter_id}/replay", status_code=202, tags=["ops"])
def replay_dead_letter(dead_letter_id: int, db: Session = Depends(get_db)) -> dict:
    """Replay a dead lettered job once the issue is fixed."""
    entry = db.get(DeadLetter, dead_letter_id)
    if entry is None:
        raise HTTPException(404, "dead letter not found")

    async_result = celery_app.send_task(
        "payouts.replay", kwargs={"dead_letter_id": dead_letter_id}
    )
    return {"task_id": async_result.id}


# --- stats / health ---


@app.get("/stats", response_model=SystemStats, tags=["ops"])
def stats(db: Session = Depends(get_db)) -> SystemStats:
    """
    Dashboard numbers.

    Queue depth is the main one - flat means workers keep up, going up means
    we're falling behind. Celery stores each queue as a redis list so depth
    is just LLEN (only counts waiting messages, not reserved ones).
    """
    queues = []
    for name in QUEUES:
        try:
            queues.append(QueueDepth(queue=name, depth=_redis.llen(name)))
        except redis.RedisError:
            queues.append(QueueDepth(queue=name, depth=-1))  # -1 = unknown

    by_status = dict(
        db.execute(select(Payout.status, func.count()).group_by(Payout.status)).all()
    )

    outbox_pending = db.execute(
        select(func.count())
        .select_from(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.PENDING)
    ).scalar_one()

    dead = db.execute(
        select(func.count())
        .select_from(DeadLetter)
        .where(DeadLetter.replayed_at.is_(None))
    ).scalar_one()

    keys = db.execute(select(func.count()).select_from(ProcessedOperation)).scalar_one()

    return SystemStats(
        queues=queues,
        payouts_by_status={
            (k.value if hasattr(k, "value") else str(k)): v
            for k, v in by_status.items()
        },
        outbox_pending=outbox_pending,
        dead_letters=dead,
        idempotency_keys=keys,
    )


@app.get("/health", tags=["ops"])
def health() -> dict:
    # check each dependency separately, "redis down" and "db down" are
    # different problems
    checks = {"api": "ok"}

    try:
        _redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {exc}"

    checks["healthy"] = all(v == "ok" for v in checks.values())
    return checks


# --- small dashboard ---

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
