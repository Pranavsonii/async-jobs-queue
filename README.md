# PayFlow Jobs

A production-shaped background job system in Python: **FastAPI** producer →
**Redis** broker → **Celery** workers, with the reliability layer that separates
a tutorial from something you would put money through.

Built around a payouts domain, because payments are where "the job ran twice"
stops being an inconvenience and starts being a refund.

```
                    ┌──────────────┐
  POST /payouts ───▶│   FastAPI    │  writes payout + outbox row
     202 Accepted   │  (producer)  │  in ONE transaction
                    └──────┬───────┘
                           │ (never enqueues directly)
                    ┌──────▼───────┐
                    │  PostgreSQL  │  payouts · outbox · idempotency · DLQ
                    └──────┬───────┘
                           │ polled every 5s
                    ┌──────▼───────┐
                    │ Celery Beat  │  the only process that enqueues
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │    Redis     │  queues: payouts · fast · heavy · default
                    └──────┬───────┘
              ┌────────────┼────────────┐
        ┌─────▼─────┐┌─────▼─────┐┌─────▼─────┐
        │  payouts  ││   fast    ││   heavy   │   three worker pools,
        │  worker   ││  worker   ││  worker   │   so slow work never
        └───────────┘└───────────┘└───────────┘   blocks fast work
```

## Run it

```bash
docker compose up --build
```

| What | Where |
|---|---|
| Dashboard | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| Flower (Celery monitor) | http://localhost:5555 |

Then open the dashboard and press **Enqueue 25**. Queue depth spikes, workers
drain it, payouts move `pending → processing → paid`. Press **Enqueue a poison
job** and watch it land in the dead letter queue instead.

### Tests

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest
```

49 tests, no broker and no database required — they run on SQLite in a few
seconds.

<details>
<summary><b>Running without Docker (and a Windows caveat)</b></summary>

Celery's default `prefork` pool **does not work on Windows** — it needs
`fork(2)`. Locally on Windows use `--pool=solo` (one task at a time) or
`--pool=threads`. In Docker it is Linux, so prefork works normally.

```bash
docker run -d -p 6379:6379 redis:7-alpine
docker run -d -p 5432:5432 -e POSTGRES_USER=payflow -e POSTGRES_PASSWORD=payflow \
  -e POSTGRES_DB=payflow postgres:16-alpine

cp .env.example .env

uvicorn app.main:app --reload
celery -A app.celery_app worker --loglevel=info --pool=solo -Q payouts,fast,heavy,default
celery -A app.celery_app beat --loglevel=info
```
</details>

## What this project actually demonstrates

Anyone can wire up `task.delay()`. The interesting part is everything that
happens when the machine you were relying on disappears mid-payment.

### 1. At-least-once delivery, made safe with an idempotency ledger

`acks_late=True` means a message is acknowledged **after** the task finishes.
Kill a worker mid-task and the broker redelivers to someone else, so work is
never lost — at the cost of work sometimes running twice.

The cost is paid in [`app/services/idempotency.py`](app/services/idempotency.py),
and not the way most tutorials pay it. The naive `if already_done(): return`
has a race between the check and the write. This uses **insert-first**: the
`UNIQUE` constraint decides the winner atomically, inside the database, which
is the only component that can see every worker at once.

It is also a **lease**, not a lock. A claim held by a worker that has since died
expires and is taken over, so a crash cannot leave a payout permanently marked
as being-worked-on.

```
claim COMPLETED                 → duplicate, replay the stored result
claim IN_PROGRESS, same task id → our own retry, carry on
claim IN_PROGRESS, lease stale  → previous owner is dead, take over
claim IN_PROGRESS, lease fresh  → someone is genuinely on it, skip
```

### 2. Transactional outbox

`POST /payouts` writes the payout row and an `outbox_events` row in **one
transaction**, and never calls `.delay()`. A beat-driven poller
([`app/tasks/periodic.py`](app/tasks/periodic.py)) reads the outbox with
`FOR UPDATE SKIP LOCKED` and does the enqueueing.

This closes the gap where a commit succeeds but the enqueue fails (a payout
nobody will ever process), or the enqueue succeeds but the commit rolls back (a
worker reading a row that does not exist).

**The outbox poller is the only code in the repository that enqueues anything.**
One rule, enforced in one place, instead of a convention everyone has to
remember.

### 3. Retries that do not make things worse

Transient failures retry with exponential backoff **and jitter**. Without
jitter, five hundred tasks that failed together retry together and re-kill the
service that just recovered.

Permanent failures do not retry at all — they go straight to the DLQ, because
retrying a closed bank account five times helps nobody and delays the alert by
five backoff intervals. The
[`Transient` vs `Permanent` split](app/services/gateway.py) is what makes that
decision mechanical rather than a judgement call at 3am.

### 4. A dead letter queue you can replay from

Failed jobs are parked in `dead_letters` with their exact arguments and
traceback, then re-run through `POST /dlq/{id}/replay` once a human has fixed
the cause. Never silently dropped — in a payments system every failed job has
to be explainable after the fact.

### 5. Queue routing, so slow work never blocks fast work

Three worker pools on three queues. A ten-minute report on `heavy` cannot delay
a receipt on `fast`, because they are not in the same line. You do not solve
starvation by prioritising within a queue; you solve it with a different queue.

### 6. Defence in depth against a dead worker

Three independent mechanisms recover the same failure, and any one of them is
enough:

| Mechanism | Recovers within |
|---|---|
| `acks_late` broker redelivery | visibility timeout (1h) |
| beat `sweep_stuck_payouts` | 60s |
| nightly reconciliation | 24h, catches whatever both missed |

They can all fire for the same payout. That is fine — it is exactly what the
idempotency ledger is for. Every layer here assumes the others might double up.

### 7. Observability

`GET /stats` reports **queue depth**, which is the one metric that distinguishes
"busy" from "failing". If depth is flat, workers are keeping up. If it trends
upward, latency is already heading for infinity and you cannot see it yet,
because the jobs currently running are still finishing on time.

## Layout

```
app/
  config.py            every knob, read from the environment
  celery_app.py        Celery config, queue routing, beat schedule
  models.py            4 tables — 3 of them exist purely for reliability
  main.py              FastAPI producer: accepts work, never does it
  schemas.py           validation at the edge, before anything becomes a message
  services/            business logic — deliberately Celery-free, so it is testable
    idempotency.py     ★ the insert-first, lease-based claim
    outbox.py          ★ DB write and enqueue, atomically
    gateway.py         simulated payment provider (transient vs permanent)
    payouts.py         state transitions
    dlq.py             parking and replay
  tasks/               thin adapters: delivery concerns only
    payouts.py         ★ the centrepiece — read this one
    webhooks.py        inbound at-least-once, deduped on the provider's event id
    reports.py         chord fan-out / fan-in on the heavy queue
    notifications.py   fire-and-forget (note how much less machinery it needs)
    periodic.py        the four beat jobs
tests/                 49 tests, no infrastructure required
docs/CONCEPTS.md       the same ideas explained in depth (Hinglish)
```

## Deliberate trade-offs

Things that are simplified here, and what production would do instead — stated
up front, because knowing where the demo ends is part of understanding it:

| Here | Production |
|---|---|
| `create_all()` on startup | Alembic migrations as a separate deploy step |
| Simulated payment gateway | A real provider, with signed webhooks |
| Beat's schedule in a local file | A DB-backed scheduler, or a leader-election lock |
| Queue depth via `/stats` | Prometheus + Grafana, alerting on backlog slope |
| `ignore_result` tuned by hand | Same, but measured against real Redis memory |
| No auth on the API | OAuth/mTLS, and webhook signature verification |

## The one-paragraph version

> Payment requests are accepted by FastAPI and committed to Postgres together
> with an outbox row in a single transaction, so a queued job can never
> disagree with the database. A beat-driven poller publishes those rows to
> Redis using `FOR UPDATE SKIP LOCKED`. Celery workers process them with
> `acks_late`, which gives at-least-once delivery, made safe by a lease-based
> idempotency ledger keyed on the business reference. Transient gateway
> failures retry with exponential backoff and jitter; permanent ones go to a
> replayable dead letter queue. Separate queues keep slow report generation
> from starving fast notification work, and a nightly reconciliation job
> verifies the ledger against the gateway.

---

More detail on the why behind each decision is in [`docs/CONCEPTS.md`](docs/CONCEPTS.md).
