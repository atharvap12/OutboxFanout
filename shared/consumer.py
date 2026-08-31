"""The receive-handle-delete loop every consumer runs. Plumbing, not meaning.

PICTURE A PARCEL DEPOT.

Parcels arrive on a conveyor belt (the SQS queue). A worker (this loop) takes
one off, does something with it, and ticks it off the manifest. Three details
of how a REAL depot works are exactly the three details SQS gets right, and all
three matter to us:

    (1) THE WORKER DOESN'T SPRINT TO THE BELT EVERY SECOND TO LOOK.
        He stands at the belt and waits. If a parcel comes in 3 seconds, he
        takes it in 3 seconds. If none comes for 20, he has wasted no effort.
        SQS calls this LONG POLLING (WaitTimeSeconds). The alternative — asking
        "anything there?" over and over — is SHORT POLLING, and it burns CPU
        and API calls to learn "no" thousands of times an hour.

    (2) TAKING A PARCEL OFF THE BELT DOES NOT DESTROY IT.
        It goes into the worker's locker, INVISIBLE to every other worker, for
        a fixed time. If he comes back and says "done", it is destroyed. If he
        drops dead, the locker springs open when the timer expires and the
        parcel goes back on the belt for someone else. That timer is the
        VISIBILITY TIMEOUT (ours: 30s). It is why a crashed consumer loses
        nothing, and it is the whole reason SQS is "at-least-once" and not
        "at-most-once".

    (3) "DONE" IS AN EXPLICIT, SEPARATE ACT.
        delete_message() is the tick on the manifest. Receiving is not
        deleting. A message you received but never deleted WILL come back.

THE ONE ORDERING DECISION IN THIS FILE

Two things must happen for each message — process it, and delete it — and you
must pick an order. As with the relay's publish-then-mark, neither is safe,
because there is no transaction spanning Postgres and SQS.

    DELETE FIRST, THEN PROCESS
        Crash in the gap: the parcel is already ticked off and destroyed, but
        nobody did the work. The order is never billed. SILENT AND PERMANENT —
        nothing anywhere records that it should have happened.

    PROCESS FIRST, THEN DELETE          <-- WHAT WE DO
        Crash in the gap: the locker springs open, the message comes back, and
        we process it a SECOND time. Loud, visible, and stopped dead by one
        idempotency check using an id we already have.

Same trade as the relay, one layer further down the pipe, and for the same
reason: PREFER THE RECOVERABLE FAILURE. Duplicates are a problem you can solve
in one line; a silently dropped order is a problem you can only discover from
outside the system.

WHAT THIS FILE DOES *NOT* KNOW

It has no idea what an order is. It cannot tell you what "billing" means. It
knows queues, timers, retries and shutdown — and it hands the parsed event to
a `handler` function that knows the business. That split is deliberate: Phase 5
(Notifications, Redis) reuses this file completely untouched, which is the real
test of whether the boundary was drawn in the right place.

Reference: Amazon SQS short and long polling —
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
Reference: Amazon SQS visibility timeout —
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
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

# What a consumer must supply. It receives our event envelope —
# {event_id, event_type, order_id, occurred_at, payload} — and returns None.
#
# THE CONTRACT IS THE EXCEPTION BEHAVIOUR, not the return value:
#
#     returns normally  ->  "I have dealt with this."  The loop deletes it.
#     raises            ->  "I have NOT dealt with this."  The loop leaves it
#                           alone, and SQS redelivers after the visibility
#                           timeout.
#
# Note "dealt with" covers BOTH processing it and recognising it as a
# duplicate. A duplicate is dealt with — deliberately doing nothing is still an
# answer, and the message must be deleted or it will arrive forever.
Handler = Callable[[dict], None]

# How long to pause after an INFRASTRUCTURE error (queue vanished, LocalStack
# rebooting). Without a pause, a broken loop retries as fast as the CPU allows
# and buries the real error under thousands of identical log lines.
_ERROR_BACKOFF_SECONDS = 5


def _receive(queue_url: str) -> list[dict]:
    """Wait at the belt for up to SQS_WAIT_TIME_SECONDS. Return what arrived."""
    response = aws.sqs().receive_message(
        QueueUrl=queue_url,

        # (a) UP TO 10 AT A TIME (the SQS maximum). One network round trip
        #     carrying ten messages instead of ten round trips carrying one.
        MaxNumberOfMessages=config.SQS_MAX_MESSAGES,

        # (b) THE LINE THAT MAKES IT A *LONG* POLL. With 0, SQS answers
        #     instantly — and, because it samples only a subset of its servers,
        #     it can even answer "nothing here" while messages exist. With 20,
        #     it holds the connection open until a message shows up or 20
        #     seconds pass, and checks everywhere. Fewer API calls, lower
        #     latency, no false "empty". You almost never want short polling.
        #
        #     CONSEQUENCE ELSEWHERE, worth knowing before it bites you: this
        #     call blocks for up to 20 seconds and CANNOT be interrupted. That
        #     is why shared/aws.py gives SQS its own longer HTTP read timeout,
        #     and why compose gives these containers a 25s stop_grace_period.
        WaitTimeSeconds=config.SQS_WAIT_TIME_SECONDS,

        # (c) ASK FOR THE DELIVERY COUNT. Without this, a message being
        #     delivered for the 4th time looks EXACTLY like a brand-new one in
        #     the logs, and you lose the single most useful clue when debugging
        #     "why does this keep happening?". It is also the number a
        #     dead-letter queue triggers on in Phase 6 (maxReceiveCount).
        AttributeNames=["ApproximateReceiveCount"],
    )

    # (d) NO "Messages" KEY AT ALL when the queue is empty — SQS omits it
    #     rather than sending an empty list. `.get(..., [])` is not defensive
    #     padding here; it is the normal, expected case on most polls.
    return response.get("Messages", [])


def _handle_one(queue_url: str, message: dict, handler: Handler) -> None:
    """Parse one message, hand it to the handler, then tick it off.

    Raises whatever the handler raises — deliberately. The caller turns that
    into "leave it on the queue".
    """
    # How many times SQS has handed this out, including now. 1 = first time.
    receives = int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1))

    # ---------------------------------------------------------------------
    # STEP 1 — READ IT.
    #
    # Two parses, because RawMessageDelivery is off and SNS wraps our JSON in
    # an envelope of its own. shared/messages.py explains the letter-inside-a-
    # letter shape; here we just ask for the inner letter.
    # ---------------------------------------------------------------------
    try:
        event = messages.unwrap(message["Body"])
        event_id = event["event_id"]
    except (json.JSONDecodeError, KeyError, TypeError):
        # A POISON MESSAGE: malformed beyond repair. Retrying is pointless —
        # it will be exactly as unparsable in 30 seconds.
        #
        # SO WHY NOT DELETE IT AND MOVE ON? Because deleting destroys the
        # evidence. Something produced this; you will want to look at it.
        # Leaving it alone lets SQS count the deliveries and, once Phase 6
        # attaches a dead-letter queue, move it there automatically — off the
        # main queue, out of the way, but SAVED.
        #
        # ⚠️ UNTIL PHASE 6 EXISTS, THIS MESSAGE REDELIVERS FOREVER (every 30s).
        # That is the correct behaviour with the wrong safety net attached yet.
        # Purge the queue by hand after testing this.
        log.exception(
            "unparsable body on delivery #%d — left on the queue for the DLQ", receives
        )
        return

    # A redelivery is not an error, but it IS the interesting case — this is
    # the line that proves the duplicate machinery is being exercised.
    if receives > 1:
        log.warning("redelivery #%d of event %s", receives, event_id)

    # ---------------------------------------------------------------------
    # STEP 2 — DO THE WORK.
    #
    # If this raises, we never reach step 3, the message is not deleted, and
    # SQS gives it to someone else in 30 seconds. Failure is a no-op, not a
    # half-op — which is only true because the handler's own work is wrapped
    # in a database transaction.
    # ---------------------------------------------------------------------
    handler(event)

    # ---------------------------------------------------------------------
    # STEP 3 — TICK IT OFF.
    #
    # Reached only if the handler returned cleanly, which means its
    # transaction committed. A crash BETWEEN step 2 and step 3 redelivers the
    # message; the handler's idempotency check then sees it has already done
    # the work and no-ops. That gap is the price of at-least-once, and paying
    # it is the entire point of Phase 4.
    #
    # The ReceiptHandle — not the MessageId — identifies WHICH DELIVERY you are
    # acknowledging. It is a fresh token each time the message is handed out,
    # so an old handle from a previous delivery cannot delete work someone else
    # is currently holding.
    # ---------------------------------------------------------------------
    aws.sqs().delete_message(
        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
    )


def run(queue_name: str, handler: Handler) -> None:
    """Poll `queue_name` forever, handing every event to `handler`.

    Returns only on SIGTERM/SIGINT, after finishing the message in hand.
    """
    # ------------------------------------------------------------------
    # SHUTDOWN — ASK, DON'T KILL.
    #
    # `docker compose stop` sends SIGTERM and then waits. Python's default
    # response to SIGTERM is to die instantly — which, if it lands in the
    # middle of a handler, is Scenario A: work half done, message undeleted.
    #
    # That failure is worth DEMONSTRATING deliberately; it is not worth
    # suffering every single time you stop the stack. So the handler here only
    # sets a flag, and the loop reads it at a safe boundary.
    # ------------------------------------------------------------------
    shutdown = threading.Event()

    def _stop(signum: int, _frame: types.FrameType | None) -> None:
        log.info(
            "received %s — finishing the current message, then stopping",
            signal.Signals(signum).name,
        )
        shutdown.set()

    signal.signal(signal.SIGTERM, _stop)   # docker stop / compose stop
    signal.signal(signal.SIGINT, _stop)    # Ctrl-C

    log.info(
        "consumer starting: queue %r at %s (long poll %ss, batch %d)",
        queue_name,
        config.AWS_ENDPOINT_URL,
        config.SQS_WAIT_TIME_SECONDS,
        config.SQS_MAX_MESSAGES,
    )

    # Resolved lazily, and RE-resolved if it ever goes stale. Not fetched once
    # at import, because LocalStack forgets every queue when it restarts and a
    # cached URL would then 404 for the rest of the process's life.
    queue_url: str | None = None

    while not shutdown.is_set():
        try:
            if queue_url is None:
                # NOTE aws.queue_url() looks the queue up and REFUSES to create
                # it. A consumer that auto-created its own queue would sit
                # there looking perfectly healthy, polling a queue that no SNS
                # topic is subscribed to, processing nothing, forever. Failing
                # loudly is the feature.
                queue_url = aws.queue_url(queue_name)
                log.info("queue %r resolved to %s", queue_name, queue_url)
            batch = _receive(queue_url)

        except ClientError as exc:
            # AWS answered, and the answer was "no". The one worth handling
            # specially is a queue that has ceased to exist — which on
            # LocalStack means someone restarted it. Forget the URL so the next
            # pass looks it up again instead of retrying a dead address.
            if "NonExistentQueue" in exc.response.get("Error", {}).get("Code", ""):
                log.warning(
                    "queue %r is gone (LocalStack restart?); will re-resolve", queue_name
                )
                queue_url = None
            else:
                log.exception("receive failed — retrying")
            # .wait() rather than sleep(): a SIGTERM during the backoff should
            # end the process now, not in 5 seconds.
            shutdown.wait(_ERROR_BACKOFF_SECONDS)
            continue

        except Exception:
            # Network down, DNS gone, LocalStack still booting. NEVER fatal: a
            # consumer that exits because a dependency blinked is worse than
            # one that waits, since the queue is durable and the work is still
            # there when it comes back.
            log.exception("receive failed — retrying")
            shutdown.wait(_ERROR_BACKOFF_SECONDS)
            continue

        for message in batch:
            if shutdown.is_set():
                # Stop between messages, not mid-message. The ones we are
                # holding were never deleted, so their visibility timeout
                # expires and they return to the belt. Walking away mid-batch
                # costs a 30-second delay and nothing else.
                log.info("stopping with messages still in hand — they will be redelivered")
                break

            # One correlation id per MESSAGE. Two consumers writing to the same
            # log, each processing a batch of ten, is otherwise unreadable;
            # with this you can grep one id and see one message's whole life.
            with correlation_scope():
                try:
                    _handle_one(queue_url, message, handler)
                except Exception:
                    # THE MOST IMPORTANT `except` IN THE FILE, for two reasons.
                    #
                    # (1) NOT DELETED. Swallowing the error and deleting anyway
                    #     would silently drop the order — the exact failure
                    #     this whole architecture exists to prevent.
                    # (2) NOT RE-RAISED. One bad message must not take down the
                    #     service; the other nine in this batch, and every
                    #     message after it, are still fine. The loop has to
                    #     outlive any single message.
                    log.exception(
                        "handler failed — message not deleted, SQS will redeliver"
                    )

        # NOTE there is no sleep at the bottom of this loop, and that is not an
        # oversight. _receive() already blocked for up to 20 seconds. The relay
        # needs an explicit poll interval because Postgres has no way to say
        # "wake me when a row appears"; SQS does, and this is it.

    log.info("consumer stopped cleanly")
