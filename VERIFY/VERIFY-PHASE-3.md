# Verifying Phase 3 — SNS → SQS Fan-Out

Commands to prove the Phase 3 STOP condition on demand:

> **One publish results in a message appearing in all 3 queues, verified by
> actually receiving from each — not just trusting the subscription was
> created.**

**What Phase 3 is NOT.** No consumers yet. Nothing reads these queues; messages
accumulate and we inspect them by hand. Phases 4–5 add the readers.

**What changed.** Phase 2 published into a topic with no listeners, so every
message was accepted and discarded. Now three queues are subscribed, and one
publish becomes three independent copies.

---

## Step 0 — Environment and helpers

```bash
cd ~/Projects/OutboxFanout
set -a; source .env; set +a

LS="docker compose exec -T localstack awslocal"
psql_() { docker compose exec -T postgres psql -U $PG_USER -d $PG_DB "$@"; }

qurl()   { $LS sqs get-queue-url --queue-name "$1" --output text; }
qdepth() { $LS sqs get-queue-attributes --queue-url "$(qurl "$1")" \
             --attribute-names ApproximateNumberOfMessages \
             --query 'Attributes.ApproximateNumberOfMessages' --output text; }
depths() { for q in billing-queue shipping-queue notify-queue; do
             printf '  %-16s %s\n' "$q" "$(qdepth $q)"; done; }

until_ready() {
  for _ in $(seq 1 60); do
    curl -sf localhost:8000/health >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "order service did not become ready" >&2; return 1
}
```

---

## Step 1 — Clean slate; watch the bootstrap run

```bash
docker compose down -v
docker compose up -d --build order relay
until_ready
docker compose logs bootstrap
```

```
bootstrapping AWS resources at http://localstack:4566
topic order-events     ready  arn:aws:sns:us-east-1:000000000000:order-events
queue billing-queue    ready  arn:aws:sqs:us-east-1:000000000000:billing-queue
subscribed              arn:aws:sns:...:order-events:e8f6318f-...
queue shipping-queue   ready  arn:aws:sqs:us-east-1:000000000000:shipping-queue
subscribed              arn:aws:sns:...:order-events:f2ff2f33-...
queue notify-queue     ready  arn:aws:sqs:us-east-1:000000000000:notify-queue
subscribed              arn:aws:sns:...:order-events:1ee899a5-...
bootstrap complete — 3 queues subscribed to 'order-events'
```

You never asked for `bootstrap`. It ran because `relay` now declares:

```yaml
depends_on:
  bootstrap:
    condition: service_completed_successfully
```

That waits for the container to **exit 0**, not merely to start. It matters:
SNS silently drops a message with no subscribers, so a relay that published
before the queues were wired would lose those events with no error anywhere.

```bash
docker compose ps -a bootstrap     # Exited (0)
```

A one-shot container that exits is unusual here — everything else runs forever.
That is the shape infrastructure setup should have.

> **Why a container for something that is only setup?** Compose has exactly one
> way to say "run this once and prove it finished before starting Y": a
> container plus `service_completed_successfully`. A healthcheck can only answer
> *is it alive*, never *has it completed*.
>
> It does **not** get its own image, though — `bootstrap` and `relay` are the
> same image (`outboxfanout-relay`) with different commands, since the script
> needs exactly what the relay already has. Watch for the gotcha: without an
> explicit `image:` key, Compose names images after the *service* and quietly
> builds a second identical copy.
>
> **LocalStack init hooks (`/etc/localstack/init/ready.d/`) are the obvious
> alternative, and they are unsafe here.** `/_localstack/health` goes green
> *while hooks are still running* — measured: at the moment health reported
> healthy, `sns list-topics` returned `{"Topics": []}` and `/_localstack/init`
> showed `READY: false`. The relay would be released mid-hook and could publish
> into an unsubscribed topic. Gating the healthcheck on `/_localstack/init`
> fixes that, but a hook is still LocalStack-only, and it cannot read
> `shared/config.py`, so queue names would be duplicated.

---

## Step 2 — THE STOP CONDITION

```bash
echo "before:"; depths

curl -s -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"fanout-1","item":"Fan-out test","amount":"150.00"}' | jq -r .id

sleep 5
echo "after:"; depths
```

```
before:
  billing-queue    0
  shipping-queue   0
  notify-queue     0

order_id: ca93207e-48d1-4de4-8c83-cd7e2bb559e4

after:
  billing-queue    1
  shipping-queue   1
  notify-queue     1
```

One `POST`. One outbox row. One `sns.Publish`. **Three messages.**

The relay still publishes exactly once and knows nothing about queues — that
duplication is SNS's job. Adding a fourth consumer means one more entry in
`bootstrap/main.py` and **zero changes to the relay**. That is the whole reason
the relay does not loop over three destinations itself.

### 2a. Actually receive from each — the part that counts

Counting depths proves messages arrived. The STOP condition asks you to read
them.

```bash
python3 - <<'PY'
import json, subprocess
from shared.messages import unwrap, sns_metadata

def awslocal(*args):
    return subprocess.run(["docker","compose","exec","-T","localstack","awslocal",*args],
                          capture_output=True, text=True).stdout.strip()

for q in ("billing-queue", "shipping-queue", "notify-queue"):
    url = awslocal("sqs","get-queue-url","--queue-name",q,"--output","text")
    msgs = json.loads(awslocal("sqs","receive-message","--queue-url",url,
                               "--visibility-timeout","0","--output","json") or "{}").get("Messages",[])
    print("="*60); print(q, "->", len(msgs), "message(s)")
    if not msgs: continue
    body = msgs[0]["Body"]
    env, meta = unwrap(body), sns_metadata(body)
    print("  SNS MessageId:", meta.get("MessageId"))
    print("  event_id     :", env["event_id"])
    print("  order_id     :", env["order_id"])
    print("  amount       :", repr(env["payload"]["amount"]), "<- still a string")
PY
```

```
============================================================
billing-queue -> 1 message(s)
  SNS MessageId: aad71889-7d67-4021-8c29-73ceee1a7408
  event_id     : c392da90-d7fb-4906-b07b-df7e82213936
  order_id     : ca93207e-48d1-4de4-8c83-cd7e2bb559e4
  amount       : '150.00' <- still a string
============================================================
shipping-queue -> 1 message(s)
  SNS MessageId: aad71889-7d67-4021-8c29-73ceee1a7408
  ... identical ...
============================================================
notify-queue -> 1 message(s)
  SNS MessageId: aad71889-7d67-4021-8c29-73ceee1a7408
  ... identical ...
```

✅ **Pass.** Three queues, identical content.

Note the **SNS MessageId is the same in all three** — proof this really was one
publish fanned out, not three publishes. The *SQS* MessageIds differ, because
each queue mints its own id for its own copy.

> Yet another reason a broker id is useless as a dedup key: here one logical
> event has one SNS id and three SQS ids. Dedupe on `order_id`.

`--visibility-timeout 0` means "show me this but don't hide it from anyone
else", so you can run the script repeatedly. Without it, the message would
disappear for 30 seconds.

---

## Step 3 — The SNS envelope (the `RawMessageDelivery` decision)

Look at what the raw body actually is:

```bash
$LS sqs receive-message --queue-url "$(qurl billing-queue)" \
   --visibility-timeout 0 --query 'Messages[0].Body' --output text | python3 -m json.tool
```

```json
{
    "Type": "Notification",
    "MessageId": "aad71889-7d67-4021-8c29-73ceee1a7408",
    "TopicArn": "arn:aws:sns:us-east-1:000000000000:order-events",
    "Message": "{\"event_id\": \"c392da90-…\", \"payload\": {…}}",
    "Timestamp": "2026-08-25T11:29:02.331Z",
    "MessageAttributes": {"event_type": {"Type": "String", "Value": "OrderCreated"}}
}
```

**Our JSON is inside `Message` — as a string.** That is `RawMessageDelivery`
left at its default of `false`.

Note what this does *not* do: it does not alter our payload. Both settings
deliver our bytes intact; the only question is whether SNS wraps them. What the
wrapper buys is the metadata around it — `MessageId`, `Timestamp`, `TopicArn`,
`MessageAttributes` — which is worth having when tracing a duplicate.

The cost is that consumers parse twice. `shared/messages.py` does it in one
place so all three consumers don't reinvent it:

```python
from shared.messages import unwrap
event = unwrap(message["Body"])     # {"event_id": ..., "payload": {...}}
```

It also tolerates a raw body, so flipping the subscription attribute later does
not break every consumer at once.

---

## Step 4 — The filter policy really filters

Each subscription carries `{"event_type": ["OrderCreated"]}`. Since that is the
only event type we publish, it changes nothing today — it exists to show where
per-consumer routing lives. Prove it works by publishing something else:

```bash
TOPIC=arn:aws:sns:us-east-1:000000000000:order-events

# NOT in the filter policy
$LS sns publish --topic-arn "$TOPIC" \
  --message '{"event_type":"OrderCancelled"}' \
  --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCancelled"}}'
sleep 4; depths
```

```
  billing-queue    1     <- unchanged
  shipping-queue   1
  notify-queue     1
```

```bash
# matching attribute
$LS sns publish --topic-arn "$TOPIC" \
  --message '{"event_type":"OrderCreated"}' \
  --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCreated"}}'
sleep 4; depths
```

```
  billing-queue    2     <- delivered
  shipping-queue   2
  notify-queue     2
```

The publish **succeeded** both times — SNS accepted the message and then
discarded it per-subscription. Filtering happens at delivery, not at publish.

**Crucially, SNS matched on the message ATTRIBUTE, not the body.** Both messages
had `"event_type"` in their JSON body; only the attribute decided. That is why
`relay/publisher.py` duplicates `event_type` into `MessageAttributes` — to SNS
the body is an opaque blob it never opens.

To route a real subset later, change one subscription and restart nothing:

```bash
$LS sns set-subscription-attributes --subscription-arn <notify-sub-arn> \
   --attribute-name FilterPolicy \
   --attribute-value '{"event_type":["OrderCancelled"]}'
```

Notifications would stop seeing `OrderCreated` with no code change in the relay
or any consumer. Reset it by re-running the bootstrap.

---

## Step 5 — The bootstrap is idempotent

The reason this must be a script, not commands you type: LocalStack's free tier
**forgets every topic, queue and subscription on restart**. So setup runs on
every boot, and must therefore be safe to run any number of times.

```bash
docker compose run --rm bootstrap
docker compose run --rm bootstrap

$LS sns list-subscriptions-by-topic --topic-arn "$TOPIC" --query 'length(Subscriptions)' --output text
$LS sqs list-queues --query 'length(QueueUrls)' --output text
```

```
3
3
```

Still 3 and 3, not 9 and 9 — and the log prints the *same* subscription ARNs
each run. Three AWS calls make this work:

| Call | Behaviour on a repeat |
| --- | --- |
| `create_topic` | returns the existing ARN |
| `create_queue` | returns the existing URL — **only if attributes match** |
| `subscribe` | same topic+protocol+endpoint returns the existing subscription |

That middle caveat is why the script creates queues **bare** and then calls
`set_queue_attributes` separately. Pass attributes to `create_queue` and a later
edit to, say, `VisibilityTimeout` makes the next run raise
`QueueAlreadyExists`. Create-then-configure is re-runnable under any change.

---

## Step 6 — The queues are genuinely independent

This is the property Scenario C depends on: one consumer's outage must not
affect another's.

```bash
$LS sqs purge-queue --queue-url "$(qurl billing-queue)"
sleep 2; depths
```

```
  billing-queue    0
  shipping-queue   2     <- untouched
  notify-queue     2
```

Emptying one queue does nothing to the others. Each holds its **own copy** with
its own delivery state — its own receive counts, its own visibility timers.
There is no shared cursor, which is exactly why a consumer can be offline for an
hour while the others run normally.

---

## Step 7 — The queue policy, and why this test can't prove it

```bash
$LS sqs get-queue-attributes --queue-url "$(qurl notify-queue)" \
   --attribute-names Policy --query 'Attributes.Policy' --output text | python3 -m json.tool
```

```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "AllowOrderEventsTopicToSendMessages",
        "Effect": "Allow",
        "Principal": {"Service": "sns.amazonaws.com"},
        "Action": "sqs:SendMessage",
        "Resource": "arn:aws:sqs:us-east-1:000000000000:notify-queue",
        "Condition": {"ArnEquals": {"aws:SourceArn": "arn:aws:sns:...:order-events"}}
    }]
}
```

A queue is **private by default**. In real AWS, subscribing without this
succeeds and then delivers nothing — green subscription, empty queue, no error.
The design doc calls it the most common first-time gotcha, and it is.

Each clause is scoped deliberately:

| Clause | Why not looser |
| --- | --- |
| `Principal: sns.amazonaws.com` | `"*"` would allow every AWS account on earth |
| `Action: sqs:SendMessage` | SNS never needs to read or delete |
| `Resource` | this one queue |
| `Condition: aws:SourceArn` | **and only from our topic** |

Without the `Condition`, any SNS topic in any account could write here — the
**confused deputy** problem, where a trusted service is tricked into acting on a
stranger's behalf. `aws:SourceArn` is the standard fix.

### ⚠️ LocalStack does not enforce this

Try deleting it:

```bash
NURL=$(qurl notify-queue)
$LS sqs set-queue-attributes --queue-url "$NURL" --attributes 'Policy='
$LS sns publish --topic-arn "$TOPIC" --message '{"event_type":"OrderCreated"}' \
   --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCreated"}}'
sleep 4; qdepth notify-queue
```

**The message is delivered anyway.** LocalStack does not evaluate IAM unless you
set `ENFORCE_IAM=1`.

This inverts the usual trap. The familiar failure is "works locally, breaks in
production." Here you could omit the policy entirely, watch every local test
pass, and only discover on deployment day that no consumer ever receives
anything. **A local test passing is not evidence the permission is correct.**

Restore the correct state:

```bash
docker compose run --rm bootstrap
```

---

## Phase 3 checklist

- [ ] `bootstrap` runs automatically and exits **0**; relay waits for it
- [ ] One `POST /orders` → depth 1 in **all three** queues
- [ ] Message **received** from each queue, contents identical
- [ ] Same **SNS MessageId** in all three (one publish, fanned out)
- [ ] Body is the SNS envelope; `unwrap()` recovers our event
- [ ] `amount` is still a **string**
- [ ] Publishing `OrderCancelled` reaches **no** queue; `OrderCreated` reaches all three
- [ ] Re-running bootstrap leaves **3** queues and **3** subscriptions
- [ ] Purging one queue leaves the other two untouched
- [ ] Every queue has a policy scoped to the topic via `aws:SourceArn`

---

## Troubleshooting

**A queue exists but never receives anything (in real AWS)** — the queue policy.
Check `Policy` in `get-queue-attributes`. This is the classic one, and
LocalStack will not reproduce it (Step 7).

**`QueueAlreadyExists` / `QueueNameExists`** — `create_queue` was called with
attributes that differ from the live queue. Our bootstrap avoids this by
creating bare and configuring separately; if you hit it by hand, either match
the attributes or `set_queue_attributes` instead.

**Messages published but no queue receives them** — check the filter policy
first (`get-subscription-attributes`). A publish whose `MessageAttributes` do
not match is accepted and silently dropped, which looks identical to a broken
subscription.

**`docker compose up` says bootstrap is unhealthy / dependency failed** — the
bootstrap exits non-zero when its final verification finds a queue unsubscribed.
Read `docker compose logs bootstrap`. The relay is deliberately blocked in that
case rather than allowed to publish into the void.

**Everything disappeared after restarting LocalStack** — expected. Persistence
is a paid feature. Run `docker compose run --rm bootstrap`, or just
`docker compose up -d relay`, which does it for you.

**Consumer sees `{"Type":"Notification",...}` instead of the event** — that is
the SNS envelope; use `shared.messages.unwrap()`. See Step 3.
