"""
Celery app.

Used by three different processes:
  - the api      -> enqueues work
  - the worker   -> runs it
  - beat         -> enqueues on a schedule

They never talk to each other directly, only through redis and this config.
"""

from celery import Celery
from celery.schedules import crontab

from .config import settings

celery_app = Celery(
    settings.app_name,
    broker=settings.redis_broker_url,
    backend=settings.redis_backend_url,
    # task modules to import, otherwise the worker says "Received unregistered task"
    include=[
        "app.tasks.payouts",
        "app.tasks.notifications",
        "app.tasks.reports",
        "app.tasks.webhooks",
        "app.tasks.periodic",
    ],
)


celery_app.conf.update(
    # json only. pickle can run arbitrary code on load, so anyone who can write
    # to the broker could run code on the workers. also forces us to pass ids
    # instead of objects which is better anyway
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone=settings.timezone,
    enable_utc=True,

    # at-least-once: ack after the task finishes, not when it's picked up.
    # if the worker dies mid task, the message gets redelivered.
    # the catch is a task can run twice -> handled by idempotency.py
    task_acks_late=True,
    # without this a dead worker isn't treated as a lost task
    task_reject_on_worker_lost=True,

    # prefetch 1 = take one job, finish, take next. default (4) is fine for tiny
    # tasks but with slow ones a worker hoards jobs while others sit idle
    worker_prefetch_multiplier=1,

    # restart child process every 100 tasks so memory leaks don't pile up
    worker_max_tasks_per_child=100,

    # redis hides a picked message for visibility_timeout seconds. if not acked
    # by then another worker gets it. must be > slowest task or you get silent
    # duplicate runs
    broker_transport_options={"visibility_timeout": settings.visibility_timeout},

    # results
    task_track_started=True,        # so we can see STARTED state
    result_expires=settings.result_expires,  # don't let redis fill up

    # connection stuff
    broker_connection_retry_on_startup=True,
    # retry publishing a few times if the broker blips. kept small so the
    # api doesn't hang, outbox has the event anyway
    task_publish_retry=True,
    task_publish_retry_policy={"max_retries": 3, "interval_start": 0.2},

    # separate queues so a slow report doesn't block a quick receipt email
    task_routes={
        "payouts.process":        {"queue": "payouts"},
        "payouts.replay":         {"queue": "payouts"},
        "notifications.*":        {"queue": "fast"},
        "webhooks.*":             {"queue": "fast"},
        "reports.*":              {"queue": "heavy"},
        "periodic.*":             {"queue": "default"},
    },
    task_default_queue="default",

    # events for flower
    worker_send_task_events=True,
    task_send_sent_event=True,
)


# beat schedule
# beat only enqueues, workers do the actual work.
# NOTE: run only ONE beat process. two beats = every job fires twice
# (double reconciliation, double retry of payouts...)
celery_app.conf.beat_schedule = {
    # outbox poller - moves committed rows into the queue
    "publish-outbox": {
        "task": "periodic.publish_outbox",
        "schedule": 5.0,  # seconds
        # if nobody picked it up in 30s just drop it, otherwise after a worker
        # restart we'd run a big pile of stale ticks
        "options": {"expires": 30},
    },
    # pick up payouts whose worker died midway
    "sweep-stuck-payouts": {
        "task": "periodic.sweep_stuck_payouts",
        "schedule": 60.0,
        "options": {"expires": 55},
    },
    # nightly check: do our records match the gateway?
    "nightly-reconciliation": {
        "task": "periodic.reconcile_payouts",
        "schedule": crontab(hour=2, minute=0),
    },
    # old idempotency keys aren't needed, otherwise the table keeps growing
    "purge-old-idempotency-keys": {
        "task": "periodic.purge_idempotency_keys",
        "schedule": crontab(hour=3, minute=30),
    },
}
