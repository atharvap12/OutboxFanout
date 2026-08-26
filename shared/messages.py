"""Reading what actually arrives on an SQS queue.

Raw message delivery is deliberately OFF on our subscriptions, so SNS wraps
our JSON in an envelope of its own and puts ours in the "Message" field AS A
STRING. The body therefore needs parsing twice:

    raw SQS body   {"Type":"Notification","MessageId":"…","Message":"{\\"event_id\\":…}"}
    one parse      a dict whose "Message" value is still a string
    two parses     our envelope: event_id, event_type, order_id, occurred_at, payload

The cost is one extra json.loads. The benefit is SNS's own metadata —
MessageId, Timestamp, TopicArn — which is worth having when tracing a
duplicate. Note that MessageId is SNS's, minted fresh on every publish, so it
must never be used as an idempotency key; dedupe on order_id or event_id.
"""

import json
from typing import Any


def unwrap(body: str) -> dict[str, Any]:
    """Return our event envelope from a raw SQS message body.

    Also accepts an already-unwrapped body, so turning RawMessageDelivery on
    later does not break every consumer at once.
    """
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification" and "Message" in outer:
        return json.loads(outer["Message"])
    return outer


def sns_metadata(body: str) -> dict[str, Any]:
    """SNS's own fields (MessageId, Timestamp, TopicArn), or {} if raw delivery.

    Useful for logging: it lets a consumer record which PHYSICAL delivery it
    saw, alongside the logical event_id it deduped on.
    """
    outer = json.loads(body)
    if isinstance(outer, dict) and outer.get("Type") == "Notification":
        return {k: v for k, v in outer.items() if k != "Message"}
    return {}
