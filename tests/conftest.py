"""Shared fixtures. Requires a running stack: `docker compose up -d --build`.

These are INTEGRATION tests against live Postgres, Redis and LocalStack. They
are slow (Scenario D waits for real SQS redeliveries) and they mutate the
stack — Scenario A crashes the relay, Scenario C stops a consumer. Both restore
what they touched, but do not point them at anything you care about.
"""

import pytest
import requests

from tests import helpers


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: takes tens of seconds (real SQS timers)")


@pytest.fixture(scope="session", autouse=True)
def stack_is_up():
    """Fail fast, with a useful message, if the stack is not running.

    Without this, every test fails with a connection error and the real cause
    ("you forgot docker compose up") is buried in four different tracebacks.
    """
    try:
        response = requests.get(f"{helpers.ORDER_URL}/health", timeout=5)
        response.raise_for_status()
    except Exception as exc:
        pytest.exit(
            f"Order Service not reachable at {helpers.ORDER_URL} ({exc}).\n"
            "Start the stack first:  docker compose up -d --build",
            returncode=1,
        )

    for service in ("relay", "billing", "shipping", "notifications"):
        state = helpers.service_state(service)
        if state != "running":
            pytest.exit(
                f"service {service!r} is {state or 'missing'}, expected 'running'.\n"
                "Start the stack first:  docker compose up -d --build",
                returncode=1,
            )

    # Redis and the three queues must be reachable too, or the assertions
    # below would fail for reasons unrelated to what is being tested.
    helpers.rds().ping()
    for queue in (helpers.BILLING_QUEUE, helpers.SHIPPING_QUEUE, helpers.NOTIFY_QUEUE):
        helpers.queue_url(queue)

    yield


@pytest.fixture
def restore_services():
    """Bring every app service back up after a test that stopped or killed one.

    A finalizer rather than a step at the end of the test body: if the test
    fails halfway, the next test still starts from a healthy stack. Otherwise
    one failure cascades into every test after it.
    """
    yield

    helpers.compose(
        "up", "-d", "relay", "billing", "shipping", "notifications",
        env={"CRASH_AFTER_PUBLISH": "0", "CRASH_AFTER_MARK": "0"},
        check=False,
    )
    for service in ("relay", "billing", "shipping", "notifications"):
        helpers.wait_for(
            lambda s=service: helpers.service_state(s) == "running",
            timeout=90, what=f"{service} to be running again",
        )
