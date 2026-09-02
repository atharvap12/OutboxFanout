"""Test-side access to every part of the running stack.

These tests drive the REAL system through its published ports rather than
importing it. That is a deliberate choice: Scenario A has to kill the relay and
Scenario C has to stop a consumer, and a test running inside a container cannot
stop the container it lives in.

Everything here talks to localhost, because compose publishes 5432, 6379, 4566
and 8000. Inside the containers the same services are `postgres`, `redis`,
`localstack` — the address lesson from Phase 0, seen from the other side.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from decimal import Decimal

import boto3
import psycopg
import redis
import requests
from botocore.config import Config

# --- endpoints (host side) -------------------------------------------------

ORDER_URL = os.environ.get("ORDER_URL", "http://localhost:8000")
PG_DSN = (
    f"host={os.environ.get('PG_HOST_TEST', 'localhost')} "
    f"port={os.environ.get('PG_PORT', '5432')} "
    f"user={os.environ.get('PG_USER', 'outbox')} "
    f"password={os.environ['PG_PASSWORD']} "
    f"dbname={os.environ.get('PG_DB', 'outboxfanout')}"
)
REDIS_URL = os.environ.get("REDIS_URL_TEST", "redis://localhost:6379/0")
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_TEST", "http://localhost:4566")

BILLING_QUEUE = os.environ.get("BILLING_QUEUE_NAME", "billing-queue")
SHIPPING_QUEUE = os.environ.get("SHIPPING_QUEUE_NAME", "shipping-queue")
NOTIFY_QUEUE = os.environ.get("NOTIFY_QUEUE_NAME", "notify-queue")
DLQS = {
    BILLING_QUEUE: os.environ.get("BILLING_DLQ_NAME", "billing-dlq"),
    SHIPPING_QUEUE: os.environ.get("SHIPPING_DLQ_NAME", "shipping-dlq"),
    NOTIFY_QUEUE: os.environ.get("NOTIFY_DLQ_NAME", "notify-dlq"),
}

MAX_RECEIVE_COUNT = int(os.environ.get("SQS_MAX_RECEIVE_COUNT", "5"))


def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=AWS_ENDPOINT,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        # Long enough to outlast the queues' own 20s ReceiveMessageWaitTime,
        # for the same reason shared/aws.py needs it.
        config=Config(read_timeout=40, connect_timeout=5),
    )


def rds() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def queue_url(name: str) -> str:
    return sqs().get_queue_url(QueueName=name)["QueueUrl"]


def queue_depth(name: str) -> int:
    """Visible messages. Approximate by SQS's own admission — never assert an
    exact depth in a way that a one-message lag would break."""
    attrs = sqs().get_queue_attributes(
        QueueUrl=queue_url(name), AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"])


def purge(name: str) -> None:
    try:
        sqs().purge_queue(QueueUrl=queue_url(name))
    except Exception:
        # PurgeQueueInProgress: SQS allows one purge per 60s per queue.
        pass


# --- SQL / Redis assertions ------------------------------------------------

def sql(query: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return []


def billing_count(order_id: str) -> int:
    return sql("SELECT count(*) FROM billing_records WHERE order_id = %s", (order_id,))[0][0]


def shipment_count(order_id: str) -> int:
    return sql("SELECT count(*) FROM shipments WHERE order_id = %s", (order_id,))[0][0]


def notify_key(order_id: str) -> str | None:
    return rds().get(f"notify:processed:{order_id}")


def outbox_row(order_id: str) -> dict | None:
    rows = sql(
        "SELECT id, published, attempts, failed_at, last_error "
        "FROM outbox WHERE order_id = %s",
        (order_id,),
    )
    if not rows:
        return None
    rid, published, attempts, failed_at, last_error = rows[0]
    return {
        "id": str(rid),
        "published": published,
        "attempts": attempts,
        "failed_at": failed_at,
        "last_error": last_error,
    }


# --- driving the system ----------------------------------------------------

def create_order(customer_id: str | None = None, item: str = "Test widget",
                 amount: str = "10.00") -> str:
    """POST /orders, return the new order_id."""
    body = {
        "customer_id": customer_id or f"cust-{uuid.uuid4().hex[:8]}",
        "item": item,
        "amount": amount,
    }
    response = requests.post(f"{ORDER_URL}/orders", json=body, timeout=10)
    response.raise_for_status()
    return response.json()["id"]


def republish(order_id: str) -> None:
    """Force the relay to publish this order's outbox row again.

    The most faithful way to produce a duplicate: it is exactly what a Scenario
    A crash leaves behind — a row that was published but still looks unsent.
    Resetting attempts too, so a row parked by an earlier test is revived.
    """
    sql(
        "UPDATE outbox SET published = false, published_at = NULL, "
        "attempts = 0, failed_at = NULL, last_error = NULL "
        "WHERE order_id = %s",
        (order_id,),
    )


def send_raw(queue_name: str, body: str) -> str:
    """Put an arbitrary body straight onto a queue, bypassing SNS."""
    return sqs().send_message(QueueUrl=queue_url(queue_name), MessageBody=body)["MessageId"]


# --- compose control -------------------------------------------------------

def compose(*args: str, env: dict | None = None, check: bool = True) -> subprocess.CompletedProcess:
    merged = {**os.environ, **(env or {})}
    return subprocess.run(
        ["docker", "compose", *args],
        capture_output=True, text=True, check=check, env=merged, timeout=180,
    )


def service_state(service: str) -> str:
    """'running', 'exited', or '' if the container does not exist."""
    result = compose("ps", "-a", "--format", "{{.Service}}\t{{.State}}", check=False)
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0] == service:
            return parts[1]
    return ""


def exit_code(service: str) -> int | None:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.ExitCode}}", f"outboxfanout-{service}-1"],
        capture_output=True, text=True, timeout=30,
    )
    return int(result.stdout.strip()) if result.returncode == 0 else None


def logs_since(service: str, seconds: int = 120) -> str:
    return compose("logs", service, "--no-log-prefix", "--since", f"{seconds}s", check=False).stdout


# --- waiting ---------------------------------------------------------------

class Timeout(AssertionError):
    pass


def wait_for(predicate, timeout: float = 60, interval: float = 1.0, what: str = "condition"):
    """Poll until predicate() is truthy. Returns its value.

    Every wait in these tests is a poll with a deadline, never a fixed sleep.
    A sleep long enough to be reliable is far longer than the usual case, and a
    sleep short enough to be quick is flaky — the exact reason
    VERIFY-PHASE-1.md replaced `sleep 8` with a readiness poll.
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise Timeout(f"timed out after {timeout}s waiting for {what} (last value: {last!r})")


def wait_for_all_three(order_id: str, timeout: float = 60) -> None:
    """Wait until every consumer has processed this order exactly once."""
    wait_for(
        lambda: billing_count(order_id) == 1
        and shipment_count(order_id) == 1
        and notify_key(order_id) is not None,
        timeout=timeout,
        what=f"all three consumers to process {order_id}",
    )
