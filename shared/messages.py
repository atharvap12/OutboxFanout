"""Reading what actually arrives on an SQS queue.

Raw message delivery is deliberately OFF on our subscriptions, so SNS wraps our
JSON in an envelope of its own and puts ours inside the "Message" field AS A
STRING. The body therefore needs parsing TWICE:

    raw SQS body   {"Type":"Notification","MessageId":"...","Message":"{\\"event_id\\":...}"}
    one parse      a dict whose "Message" value is STILL a string
    two parses     our envelope: event_id, event_type, order_id, occurred_at, payload

A letter inside a letter: the outer one is from the post office and says when
and how it was delivered; the inner one is what we actually wrote.

Why accept the extra parse? The outer envelope carries SNS's own metadata —
MessageId, Timestamp, TopicArn — which is genuinely useful when tracing a
duplicate. Note the trap though: that MessageId is minted fresh on EVERY
publish, so it must never be used as an idempotency key. Dedupe on order_id
(or event_id), which come from us and do not change when a row is republished.

Living in one place means the three consumers do not each reinvent it — and
if we ever flip RawMessageDelivery on, only this file changes.
"""

import json
from typing import Any


def unwrap(body: str) -> dict[str, Any]:
    """Return our event envelope from a raw SQS message body.

    Also accepts a body that is ALREADY unwrapped, so turning
    RawMessageDelivery on later does not break every consumer at once.
    """
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        return json.loads(outer["Message"])
    return outer


def sns_metadata(body: str) -> dict[str, Any]:
    """SNS's own fields (MessageId, Timestamp, TopicArn), or {} if raw delivery.

    Useful for logging: it lets a consumer record which PHYSICAL delivery it
    saw, next to the logical event_id it deduped on. When a duplicate shows up,
    those two together tell you whether it was one event sent twice or two
    genuinely different events.
    """
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification":
        return {k: v for k, v in outer.items() if k != "Message"}
    return {}
