"""The SQS receive → handle → delete loop, shared by every consumer.

Plumbing, not business logic: this module knows about queues, visibility
timeouts, redeliveries and shutdown. What a message MEANS is the handler's job.

The ordering is fixed and deliberate: handle first, delete second. Deleting
first would drop a message the handler never finished, which is the one loss
this system does not tolerate. Handling first means a crash in the gap causes
a redelivery instead — which is exactly what the handler's idempotency check
exists to absorb.
"""

import json
import signal
import threading
import types
from collections.abc import Callable

from botocore.exceptions import ClientError

from shared import aws, config, messages
from shared.log import correlation_scope, get_logger

log = get_logger(__name__)

# Takes our event envelope (event_id, event_type, order_id, occurred_at,
# payload). Raising means "not processed" — the message is left for redelivery.
Handler = Callable[[dict], None]

# Backoff after an infrastructure error (queue missing, LocalStack restarting),
# so a broken loop does not hammer the endpoint.
_ERROR_BACKOFF_SECONDS = 5


def _receive(queue_url: str) -> list[dict]:
    """One long poll. Returns up to SQS_MAX_MESSAGES messages, or [].

    WaitTimeSeconds is what makes this a *long* poll: the call blocks until a
    message arrives or the wait expires, instead of returning empty instantly
    and spinning. It is also why this loop needs no sleep of its own.
    """
    response = aws.sqs().receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=config.SQS_MAX_MESSAGES,
        WaitTimeSeconds=config.SQS_WAIT_TIME_SECONDS,
        # Delivery count, so a redelivery is visible in the logs rather than
        # looking like a fresh event. Phase 6's DLQ triggers on this number.
        AttributeNames=["ApproximateReceiveCount"],
    )
    return response.get("Messages", [])


def _handle_one(queue_url: str, message: dict, handler: Handler) -> None:
    """Parse, hand to the handler, then delete. Raises if the handler raises."""
    receives = int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1))

    try:
        event = messages.unwrap(message["Body"])
        event_id = event["event_id"]
    except (json.JSONDecodeError, KeyError, TypeError):
        # A poison message: retrying cannot help, but it is NOT deleted either.
        # Leaving it lets SQS redeliver it up to maxReceiveCount and then move
        # it to the DLQ (Phase 6), where it can be inspected. Deleting here
        # would destroy the evidence.
        log.exception(
            "unparsable body on delivery #%d — left on the queue for the DLQ", receives
        )
        return

    if receives > 1:
        log.warning("redelivery #%d of event %s", receives, event_id)

    handler(event)

    # Only after the handler returned cleanly, which means its transaction
    # committed. A crash between these two lines redelivers the message, and
    # the handler's idempotency check no-ops on it.
    aws.sqs().delete_message(
        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
    )


def run(queue_name: str, handler: Handler) -> None:
    """Poll `queue_name` forever, passing each event to `handler`.

    Never creates the queue: a consumer polling a queue nobody subscribed
    would look healthy and receive nothing. Provisioning is bootstrap's job.
    """
    shutdown = threading.Event()

    def _stop(signum: int, _frame: types.FrameType | None) -> None:
        # Ask the loop to stop; do not exit here. Dying mid-message is a real
        # failure mode worth testing deliberately, not on every Ctrl-C.
        log.info(
            "received %s — finishing the current message, then stopping",
            signal.Signals(signum).name,
        )
        shutdown.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log.info(
        "consumer starting: queue %r at %s (long poll %ss, batch %d)",
        queue_name,
        config.AWS_ENDPOINT_URL,
        config.SQS_WAIT_TIME_SECONDS,
        config.SQS_MAX_MESSAGES,
    )

    queue_url: str | None = None

    while not shutdown.is_set():
        try:
            if queue_url is None:
                queue_url = aws.queue_url(queue_name)
                log.info("queue %r resolved to %s", queue_name, queue_url)
            batch = _receive(queue_url)
        except ClientError as exc:
            # LocalStack forgets queues on restart, so a cached URL can 404
            # forever. Drop it and re-resolve on the next pass.
            if "NonExistentQueue" in exc.response.get("Error", {}).get("Code", ""):
                log.warning("queue %r is gone (LocalStack restart?); re-resolving", queue_name)
                queue_url = None
            else:
                log.exception("receive failed — retrying")
            shutdown.wait(_ERROR_BACKOFF_SECONDS)
            continue
        except Exception:
            log.exception("receive failed — retrying")
            shutdown.wait(_ERROR_BACKOFF_SECONDS)
            continue

        for message in batch:
            if shutdown.is_set():
                # The rest stay invisible until the visibility timeout expires,
                # then come back. Nothing is lost by stopping mid-batch.
                log.info("stopping with messages still in hand — they will be redelivered")
                break

            # One correlation id per message, so a message's log lines can be
            # grepped out as a unit even when consumers interleave.
            with correlation_scope():
                try:
                    _handle_one(queue_url, message, handler)
                except Exception:
                    # NOT deleted, on purpose: SQS makes it visible again after
                    # the visibility timeout and redelivers. The loop must
                    # outlive one bad message.
                    log.exception("handler failed — message not deleted, SQS will redeliver")

    log.info("consumer stopped cleanly")
