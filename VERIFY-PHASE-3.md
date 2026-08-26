# Verifying Phase 3 — SNS → SQS Fan-Out

Commands to prove the Phase 3 STOP condition on demand:

> **One publish results in a message appearing in all 3 queues, verified by
> actually receiving from each — not just trusting the subscription was
> created.**

**What Phase 3 is NOT.** No consumers yet. Nothing reads these queues; messages
pile up and we inspect them by hand. Phases 4–5 add the readers.

**What changed.** In Phase 2 the mail clerk carried letters to a post office
with no delivery addresses registered — every letter was accepted and thrown
away. Now three pigeonholes are registered, and one letter posted becomes three
independent copies.

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
subscribed              arn:aws:sns:...:order-events:edee5503-...
queue shipping-queue   ready  arn:aws:sqs:us-east-1:000000000000:shipping-queue
subscribed              arn:aws:sns:...:order-events:57228c49-...
queue notify-queue     ready  arn:aws:sqs:us-east-1:000000000000:notify-queue
subscribed              arn:aws:sns:...:order-events:2403a2f6-...
bootstrap complete — 3 queues subscribed to 'order-events'
```

You never asked for `bootstrap`. It ran because `relay` now declares:

```yaml
depends_on:
  bootstrap:
    condition: service_completed_successfully
```

That waits for the container to **exit 0** — not merely to start. It is
load-bearing: **SNS accepts a message with no subscribers and silently discards
it**, so a relay that published before the queues were wired would lose those
events with no error anywhere.

```bash
docker compose ps -a
```

```
bootstrap: Exited (0)      [outboxfanout-relay]
relay:     Up              [outboxfanout-relay]
```

Two things worth noticing there.

**A container that exits.** Everything else here runs forever. That is the shape
setup should have — do the job, prove it worked, get out of the way.

**Both use the same image.** `bootstrap` needs exactly what the relay needs
(boto3 + `shared/`), so it is the relay image with a different `command`.

```bash
docker images | grep outboxfanout      # 2 images, not 3
```

> **Why a container at all, for something that is only setup?** Compose has
> exactly one way to say *"run this once and prove it finished before starting
> Y"*: a container plus `service_completed_successfully`. A healthcheck can only
> answer *is it alive* — never *has it finished*.
>
> **LocalStack init hooks** (`/etc/localstack/init/ready.d/`) look like the
> obvious alternative, and they are unsafe here. `/_localstack/health` turns
> green while hooks are **still running** — measured: at the instant health
> reported healthy, `sns list-topics` returned `{"Topics": []}` and
> `/_localstack/init` said `READY: false, state: RUNNING`. The relay would be
> released mid-hook. You can fix that by gating the healthcheck on
> `/_localstack/init` instead, but a hook is still LocalStack-only, and it
> cannot read `shared/config.py` — so the queue names would have to be written
> down a second time.
>
> **Watch the image gotcha too:** without an explicit `image:` key, Compose
> names images after the *service*, so two services sharing one Dockerfile
> would quietly build and tag two identical copies.

---

## Step 2 — THE STOP CONDITION

```bash
echo "before:"; depths

curl -s -X POST localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"learn3","item":"Fan-out on learning","amount":"150.00"}' | jq -r .id

sleep 5
echo "after:"; depths
docker compose logs localstack | grep -c 'sns.Publish => 200'
```

```
before:
  billing-queue    0
  shipping-queue   0
  notify-queue     0

order_id: a7dc6ffc-42e8-4126-bb27-f7061ec2bb60

after:
  billing-queue    1
  shipping-queue   1
  notify-queue     1

sns.Publish calls: 1
```

**One POST. One outbox row. One publish. Three messages.**

The relay published exactly once and knows nothing about queues — duplicating is
SNS's job. That is why the relay does not loop over three destinations itself:
adding a fourth consumer means one more entry in `bootstrap/main.py` and **zero
changes to the relay**.

### 2a. Actually receive from each — the part that counts

Counting depths shows messages arrived. The STOP condition asks you to read them.

```bash
python3 - <<'PY'
import json, subprocess
from shared.messages import unwrap, sns_metadata

def al(*a):
    return subprocess.run(["docker","compose","exec","-T","localstack","awslocal",*a],
                          capture_output=True, text=True).stdout.strip()

for q in ("billing-queue", "shipping-queue", "notify-queue"):
    url = al("sqs","get-queue-url","--queue-name",q,"--output","text")
    msgs = json.loads(al("sqs","receive-message","--queue-url",url,
                         "--visibility-timeout","0","--output","json") or "{}").get("Messages",[])
    print("="*62); print(q, "->", len(msgs), "message(s)")
    if not msgs: continue
    b = msgs[0]["Body"]; env = unwrap(b); meta = sns_metadata(b)
    print("  SNS MessageId:", meta.get("MessageId"))
    print("  SQS MessageId:", msgs[0]["MessageId"])
    print("  event_id     :", env["event_id"])
    print("  amount       :", repr(env["payload"]["amount"]), "<- still a string")
PY
```

```
==============================================================
billing-queue -> 1 message(s)
  SNS MessageId: 0e5d11c0-9e1b-4cc5-a0e4-f26eb8d3cb68
  SQS MessageId: 81049b26-7a12-499a-b8bc-74eeb09d60ae
  event_id     : 4269f265-e528-4fc5-aa20-d630e5f091df
  amount       : '150.00' <- still a string
==============================================================
shipping-queue -> 1 message(s)
  SNS MessageId: 0e5d11c0-9e1b-4cc5-a0e4-f26eb8d3cb68     <- SAME
  SQS MessageId: a209753f-d876-4580-8579-65b824b891d8     <- different
  event_id     : 4269f265-e528-4fc5-aa20-d630e5f091df     <- SAME
==============================================================
notify-queue -> 1 message(s)
  SNS MessageId: 0e5d11c0-9e1b-4cc5-a0e4-f26eb8d3cb68     <- SAME
  SQS MessageId: 53f40517-a5c5-40e9-ae52-c36401b185af     <- different
  event_id     : 4269f265-e528-4fc5-aa20-d630e5f091df     <- SAME
```

✅ **Pass.**

Read those three ids carefully, because they make the Phase 2 lesson concrete:

| Id | Copies | Comes from |
| --- | --- | --- |
| SNS MessageId | 1 (identical everywhere) | the post office, per **posting** |
| SQS MessageId | 3 (all different) | each pigeonhole, per **copy** |
| `event_id` | 1 (identical everywhere) | **us** — the outbox row |

The same SNS MessageId in all three proves this really was one publish fanned
out, not three publishes. And one real-world event now carries **one SNS id and
three SQS ids** — which is the clearest possible demonstration of why a
transport id can never be a dedup key. Dedupe on `order_id`.

> `--visibility-timeout 0` means "show me this but don't hide it from anyone
> else", so the script is re-runnable. Without it the message vanishes for 30
> seconds — which is the visibility timeout doing its job.

---

## Step 3 — The SNS envelope (the RawMessageDelivery decision)

```bash
$LS sqs receive-message --queue-url "$(qurl shipping-queue)" \
   --visibility-timeout 0 --query 'Messages[0].Body' --output text | python3 -m json.tool
```

```json
{
    "Type": "Notification",
    "MessageId": "0e5d11c0-9e1b-4cc5-a0e4-f26eb8d3cb68",
    "TopicArn": "arn:aws:sns:us-east-1:000000000000:order-events",
    "Message": "{\"event_id\": \"4269f265-…\", \"payload\": {…}}",
    "Timestamp": "2026-08-26T07:56:03.221Z",
    "MessageAttributes": {"event_type": {"Type": "String", "Value": "OrderCreated"}}
}
```

**Our JSON is inside `Message` — as a string.** A letter inside a letter: the
outer one is from the post office and records when and how it was delivered; the
inner one is what we actually wrote.

That is `RawMessageDelivery` left at its default of `false`. Note what it does
**not** do: it never alters our payload. Both settings deliver our bytes intact;
the only question is whether SNS wraps them. The wrapper buys SNS's metadata —
`MessageId`, `Timestamp`, `TopicArn` — worth having when tracing a duplicate.

The cost is one extra parse, done once in `shared/messages.py` so three
consumers don't each reinvent it:

```python
from shared.messages import unwrap
event = unwrap(message["Body"])     # {"event_id": ..., "payload": {...}}
```

It also accepts an already-unwrapped body, so flipping the subscription
attribute later would not break every consumer at once.

---

## Step 4 — The filter policy really filters

Each subscription carries `{"event_type": ["OrderCreated"]}`. That is the only
type we publish, so it changes nothing today — it exists to show *where*
per-consumer routing lives. Prove it works by publishing something else:

```bash
T=arn:aws:sns:us-east-1:000000000000:order-events

$LS sns publish --topic-arn "$T" --message '{"event_type":"OrderCancelled"}' \
  --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCancelled"}}'
sleep 4; depths
```

```
  billing-queue    1     <- unchanged
  shipping-queue   1
  notify-queue     1
```

```bash
$LS sns publish --topic-arn "$T" --message '{"event_type":"OrderCreated"}' \
  --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCreated"}}'
sleep 4; depths
```

```
  billing-queue    2     <- delivered
  shipping-queue   2
  notify-queue     2
```

Two things to take away.

**The rejected publish SUCCEEDED.** It returned a MessageId and a 200. SNS
accepted the message and *then* discarded it per subscription — **filtering
happens at delivery, not at publish**. So "the publish worked" tells you nothing
about whether anyone received it, which matters the first time you debug a
consumer that sees nothing.

**SNS matched on the ATTRIBUTE, not the body.** Both messages had `event_type`
in their JSON body; only the attribute decided. To SNS the body is a sealed
envelope it never opens — which is why `relay/publisher.py` copies `event_type`
into `MessageAttributes`, back in Phase 2 before there was anything to filter.

To route a real subset later, change one subscription and restart nothing:

```bash
$LS sns set-subscription-attributes --subscription-arn <notify-sub-arn> \
   --attribute-name FilterPolicy --attribute-value '{"event_type":["OrderCancelled"]}'
```

Notifications would stop seeing `OrderCreated`, with no code change anywhere.
Re-run the bootstrap to reset it.

---

## Step 5 — The bootstrap is idempotent

The reason this must be a script and not commands you type: LocalStack's free
tier **forgets every topic, queue and subscription on restart**. So setup runs on
every boot, and must therefore be safe to run any number of times.

```bash
docker compose run --rm bootstrap
docker compose run --rm bootstrap

$LS sns list-subscriptions-by-topic --topic-arn "$T" --query 'length(Subscriptions)' --output text
$LS sqs list-queues --query 'length(QueueUrls)' --output text
```

```
3
3
```

Still 3 and 3 — not 9 and 9 — and the log prints the *same* subscription ARNs
each run. Three AWS calls make that work:

| Call | On a repeat |
| --- | --- |
| `create_topic` | returns the existing ARN |
| `create_queue` | returns the existing URL — **only if attributes match** |
| `subscribe` | same topic+protocol+endpoint returns the existing subscription |

That middle caveat is why the script creates queues **bare** and calls
`set_queue_attributes` separately. Pass attributes to `create_queue` and a later
edit to `VisibilityTimeout` makes the next run raise `QueueAlreadyExists`.
Create-then-configure survives any change — which is the whole point.

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
There is no shared cursor, no shared position. That is exactly why a consumer
can be offline for an hour while the other two run normally.

---

## Step 7 — The queue policy, and why this test cannot prove it

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

A queue is **private by default**. Installing a pigeonhole is not the same as
authorising the postman to put things in it. In real AWS, subscribing without
this succeeds and then delivers nothing — green subscription, empty queue, no
error. The design doc names it the most common first-time gotcha, and it is.

Every clause is deliberately narrow:

| Clause | Why not looser |
| --- | --- |
| `Principal: sns.amazonaws.com` | `"*"` would allow every AWS account alive |
| `Action: sqs:SendMessage` | SNS never needs to read or delete |
| `Resource` | this one queue |
| `Condition: aws:SourceArn` | **and only from our topic** |

Without the `Condition`, any SNS topic in any account could write here. That is
the **confused deputy** problem: a trusted middleman (SNS) tricked into acting
for a stranger, because the queue trusts the middleman rather than the sender.

### ⚠️ LocalStack does not enforce this

```bash
$LS sqs set-queue-attributes --queue-url "$(qurl notify-queue)" --attributes 'Policy='
$LS sns publish --topic-arn "$T" --message '{"event_type":"OrderCreated"}' \
   --message-attributes '{"event_type":{"DataType":"String","StringValue":"OrderCreated"}}'
sleep 4; qdepth notify-queue
```

**The message is delivered anyway.** LocalStack does not evaluate IAM unless you
set `ENFORCE_IAM=1`.

This inverts the usual trap. The familiar failure is "works locally, breaks in
production". Here you could omit the policy entirely, watch every local test
pass, and only discover on deployment day that no consumer receives anything.
**A local test passing is not evidence the permission is correct.**

Restore it:

```bash
docker compose run --rm bootstrap
```

---

## Phase 3 checklist

- [ ] `bootstrap` runs automatically, exits **0**, and the relay waits for it
- [ ] Only **2** images exist — bootstrap shares the relay's
- [ ] One `POST /orders` → depth 1 in **all three** queues, one `sns.Publish`
- [ ] Message **received** from each queue; contents identical
- [ ] Same **SNS MessageId** everywhere, **different SQS MessageIds**, one `event_id`
- [ ] Body is the SNS envelope; `unwrap()` recovers our event
- [ ] `amount` is still a **string**
- [ ] `OrderCancelled` reaches **no** queue; `OrderCreated` reaches all three
- [ ] Re-running bootstrap leaves **3** queues and **3** subscriptions
- [ ] Purging one queue leaves the other two untouched
- [ ] Every queue has a policy scoped to the topic via `aws:SourceArn`

---

## Troubleshooting

**A queue exists but never receives anything (in real AWS)** — the queue policy.
Check `Policy` in `get-queue-attributes`. LocalStack will not reproduce this
(Step 7).

**`QueueAlreadyExists` / `QueueNameExists`** — `create_queue` was called with
attributes differing from the live queue. Our bootstrap avoids it by creating
bare and configuring separately.

**Published fine, but no queue received it** — check the filter policy first
(`get-subscription-attributes`). A publish whose `MessageAttributes` do not
match is accepted and silently dropped, which looks exactly like a broken
subscription.

**`dependency failed to start` on relay** — the bootstrap exited non-zero
because its final check found a queue unsubscribed. Read
`docker compose logs bootstrap`. The relay is deliberately blocked in that case
rather than allowed to publish into the void.

**Everything vanished after restarting LocalStack** — expected; persistence is a
paid feature. Run `docker compose run --rm bootstrap`, or just
`docker compose up -d relay`, which does it for you.

**Consumer sees `{"Type":"Notification",...}` instead of the event** — that is
the SNS envelope. Use `shared.messages.unwrap()`. See Step 3.
