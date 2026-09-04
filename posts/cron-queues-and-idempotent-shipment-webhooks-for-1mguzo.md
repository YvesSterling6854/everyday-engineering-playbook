# Cron, Queues, and Idempotent Shipment Webhooks for Node.js SaaS

A shipment update stops being a simple timer problem as soon as one game purchase must reach many subscribers, because each subscriber can accept, reject, time out, or ask for a later retry independently. **Short answer: use a delayed message queue to wake individual webhook attempts, keep the authoritative delivery state in a database ledger, and reserve cron for reconciliation rather than primary dispatch.**

This choice is about failure ownership. The queue owns when work becomes eligible; the ledger owns what happened; the receiving service must use the stable delivery key to suppress a repeated business effect. A queue by itself doesn't provide exactly-once delivery, while a cron process plus a `next_attempt_at` column doesn't become wrong merely because it polls. The architecture decision is which component should carry the complexity under concurrency, restart, and partial failure.

## Retention, privacy, and audit invariants

For a gaming SaaS that fans out shipment event `shipment.dispatched` to many store, guild, and notification subscribers, create one immutable delivery identity per event-subscriber pair. A useful key is `(event_id, subscription_id)`, enforced by a database uniqueness constraint. Attempts belong beneath that delivery record; they don't get new business identities merely because attempt 2 occurs thirty seconds after attempt 1.

The design has four invariants. First, fan-out commits every intended delivery before any worker can send it. Second, an attempt outcome becomes durable before the triggering message is acknowledged. Third, every retry carries the same delivery identity, even though its attempt number and due time change. Fourth, the request body and the audit record agree on the event identity, subscriber, payload version, signature algorithm, attempt, response status, and timestamps. Retention for bodies, headers, and response evidence must follow the applicable privacy, contractual, and compliance rules; there is no universal retention period that can be inferred from the transport.

Duplicates still happen.

Consider the awkward boundary: the subscriber accepts a shipment update, then the worker loses its database connection before recording the successful response. On redelivery, the sender cannot prove from its local ledger that the first request took effect. The practical exactly-once mindset is therefore cooperative, not magical: send the same idempotency key again, require the receiver to store that key with its business transaction, and make a repeated request return the prior outcome without applying shipment credits or notifications twice. HMAC authenticates a message; it doesn't deduplicate one. RFC 2104 defines HMAC as keyed hashing for message authentication, so signing and idempotency remain separate controls.

The ledger should also expose the uncomfortable states rather than compressing them into `failed=true`: due, leased, attempted, accepted, retryable, terminal, and exhausted are operationally different. A lease expiry permits recovery after a worker disappears. A terminal response prevents an endless retry. An exhausted delivery remains visible for review. This is the audit trail that answers “which subscribers did we owe an update, which attempts did we make, and why did we stop?” without reconstructing intent from application logs.

## How does cost shape cron or message queue delayed webhook retries?

Choose against the workload's unit of time. Cron describes a recurring calendar trigger, while a delayed queue describes an individual item becoming eligible later. Shipment webhook retries have per-subscriber due times, so the queue maps more directly to the work: one slow subscriber doesn't force all other subscribers to wait for the next sweep. The database still remains authoritative, because broker state is not an adequate compliance record and because a replay must be reconciled against current delivery state.

| Option | State and concurrency burden | Operational fit | When it is the simpler choice |
| --- | --- | --- | --- |
| Cron polling a delivery table | The application implements leases, competing-worker claims, overdue scans, and polling cadence | Predictable recurring scans; dispatch precision follows the polling interval | Low volume, an existing database, and no desire to operate another component |
| Delayed queue plus delivery ledger | The broker wakes work; the application still owns idempotency, outcomes, and an outbox boundary | Many independent due times and horizontally scaled workers | Subscriber fan-out where retries should become eligible independently |
| Database due-time scheduler plus workers | The database owns due rows and atomic claims; workers own HTTP delivery | Tight transactional control with database load that must be measured | Teams prepared to tune indexes, leases, and cleanup as a first-class subsystem |

“Cheapest” can't be decided from a component count. A cron poller may have no new service bill yet consume engineering time in claim logic, hot due-time indexes, recovery drills, and reconciliation; a managed or self-operated queue adds direct and operational costs but removes some scheduling machinery. I'm not sure which wins for a particular SaaS until its event rate, retry distribution, database headroom, staffing, and audit workload are measured. Put those values into a small cost model instead of treating free-looking infrastructure as free labor.

Do not confuse priority with delay. Priority decides which ready item a consumer prefers; a delay decides when an item becomes ready. RabbitMQ's priority queue documentation also warns that priorities have resource and scheduling consequences, and it recommends using a small range of priority values. For this shipment path, customer tier may influence priority after an attempt is due, but it must not rewrite the retry timestamp or starve ordinary deliveries indefinitely.

Backpressure belongs in the decision as well. Bound concurrency per destination, apply retry delays with jitter, and honor a receiver's retry signal according to the integration contract.

## Implementation model: ledger first, acknowledgement last

The Node.js service can implement the same protocol, but the following Go sketch makes the interfaces and ordering explicit. `Complete` must atomically append the attempt outcome and, for a retryable result, write an outbox row containing the next due message. An outbox relay later publishes that message. This avoids pretending that a database commit and queue acknowledgement share a transaction.

```go
package delivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
)

type Delivery struct {
	MessageID      string
	EventID        string
	SubscriptionID string
	Attempt        int
	Endpoint       string
	Payload        []byte
}

type Attempt struct {
	ID             string
	IdempotencyKey string
}

type Ledger interface {
	// Begin returns send=false when this delivery is already terminal.
	Begin(context.Context, Delivery) (attempt Attempt, send bool, err error)
	// Complete records the outcome and writes any retry outbox row atomically.
	Complete(context.Context, Attempt, int) error
}

type Queue interface {
	Ack(context.Context, string) error
}

func signature(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func Deliver(
	ctx context.Context,
	d Delivery,
	secret []byte,
	ledger Ledger,
	queue Queue,
	client *http.Client,
) error {
	attempt, send, err := ledger.Begin(ctx, d)
	if err != nil {
		return err // No acknowledgement: the current message may be redelivered.
	}
	if !send {
		return queue.Ack(ctx, d.MessageID)
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, d.Endpoint, strings.NewReader(string(d.Payload)),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", attempt.IdempotencyKey)
	req.Header.Set("X-Signature-SHA256", signature(secret, d.Payload))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := ledger.Complete(ctx, attempt, resp.StatusCode); err != nil {
		return fmt.Errorf("record attempt outcome: %w", err)
	}
	return queue.Ack(ctx, d.MessageID)
}
```

The short branch for an already-terminal delivery matters. It turns a stale broker redelivery into an acknowledgement without another HTTP side effect. The longer failure branch matters more: if the request outcome cannot be committed, the worker leaves the message unacknowledged, accepts that the same idempotency key may be sent again, and relies on the receiver's durable deduplication record. Don't generate the key from the attempt number; doing so would authorize a fresh business effect on every retry.

An implementation review should demand evidence around the interfaces hidden by the sketch. `Begin` needs a bounded lease or an equivalent atomic claim. `Complete` needs a documented classification policy for response codes and network ambiguity. The outbox relay needs its own idempotent publish key. Metrics should separate queue age, due-to-start latency, attempts by outcome, exhausted deliveries, and destination-level throttling, while logs include identifiers rather than sensitive shipment payloads. Deployment tests should run old and new workers together against a versioned payload contract, because a retry created before a deployment can execute after it.

## Evaluation harness for the fan-out state machine

Run the failure sequence with three subscriptions for one shipment event: `store-us` accepts the first request, `guild-feed` returns `429`, and `player-mail` is still waiting behind its destination concurrency limit. Stop the worker immediately after `store-us` accepts but before the local outcome commits. On restart, the queue may present that delivery again; the second request must carry the identical idempotency key, and the test receiver must return its stored result without issuing a second shipment notification. Meanwhile, only `guild-feed` should acquire a later due time, and `player-mail` should proceed without waiting for that retry. Then run reconciliation against the fan-out manifest: it should find three delivery obligations, an ambiguous repeated attempt for `store-us`, a retryable outcome plus a scheduled outbox entry for `guild-feed`, and an accepted result for `player-mail`. This single drill crosses the dangerous boundaries — outbound HTTP versus local commit, one subscriber versus the rest of the fan-out, and an outbox transaction versus broker publication — while producing assertions that survive a worker restart. Repeat it during deployment with mixed payload versions, verify that old delivery records remain readable, and inspect metrics rather than only the final green response; a system that eventually sends all three updates can still violate its contract by applying one of them twice.

No dashboard can substitute for that exercise.

## When should the rejected scheduler remain?

Cron is rejected as the primary shipment retry dispatcher because the concrete workload contains many unrelated due times and many subscriber-specific failure histories. Making a periodic process scan `next_attempt_at` can be correct, but correctness then depends on careful row claiming, lease recovery, indexed queries, batch fairness, and protection against two scheduler instances dispatching the same row. For this workload, that is more application-owned scheduling machinery than a delayed queue plus ledger requires.

The catch is that the queue design is not suitable when shipment volume is tiny, retry timing can tolerate the polling interval, and the team already operates a database but no queue. In that case, stick with a cron-triggered database scheduler, use an atomic lease with a uniqueness constraint, and keep the same receiver idempotency contract. Cron also remains a good fit for fixed housekeeping: reconcile accepted deliveries each hour, expire audit material under the retention policy, or alert on exhausted records. Those are recurring scans, not thousands of individual clocks.

There is another stopping point. If a shipment process grows into a long-running business workflow with human approval, compensation, and dependent steps, a delayed-message dispatcher is too narrow; select a workflow model after documenting its history, cancellation, and replay semantics. For ordinary outbound fan-out, however, the decision stays modest: ledger the obligation, queue the wake-up, reuse the delivery key, commit the outcome, then acknowledge.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority
