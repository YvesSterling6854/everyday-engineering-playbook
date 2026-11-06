# Logistics Email Delivery Status Polling: Audit Evidence With Bounded Integration Work

Polling reduces the inbound integration work for a logistics compliance notice, but it moves responsibility for collecting delivery evidence into a scheduled reconciliation process. Resolve that trade-off by keeping a durable notice ledger and making each scheduled read safe to repeat. Short answer: for a Node.js application using cron without webhooks, persist the email provider's message identifier after submission, poll a documented status or events read interface, and commit each observation together with the next reconciliation decision. The focused Go example below expresses that contract independently of the application runtime; a Node.js cron entry point should invoke the same sequence.

Keep evidence separate.

A shipment notice can be submitted, observed as delivered under a provider's definition, and still lack whatever acknowledgment the applicable compliance process requires. Treat those as separate assertions. The integration objective is an auditable account of what the system observed and when, with a bounded path to manual review when automated collection cannot settle the question.

## What does polling transactional email delivery status actually prove?

Start with the notice obligation, before choosing a polling interval. For a proposed logistics workflow, define a record containing a shipment reference, notice revision, intended recipient, provider message identifier, submission time, evidence collection deadline, and reconciliation state. The deadline is a policy input: neither an email API response nor a scheduler should determine how long the organization has to discharge its obligation. Preserve the approved notice revision so that an audit can relate the evidence to the communication it concerns, rather than merely to a recipient address.

An illustrative notice key might be `shipment-482/notice-2`. These are example identifiers, not production observations. If the recipient or notice revision changes, the workflow should explicitly decide whether that is a new obligation; silently overwriting the original record would make subsequent reconciliation ambiguous.

The application should distinguish evidence of submission from evidence of delivery and from evidence that a person acknowledged the notice. A provider status is useful only with its documented meaning attached. Do not translate a label such as “delivered” into “legally served” without a separately approved rule establishing that equivalence for the notice, recipient, and jurisdiction involved.

The FTC's CAN-SPAM business guide distinguishes commercial messages from transactional or relationship messages and explains that primary purpose matters when content is mixed. Classify the actual message before assigning its requirements; a shipment-related subject line alone should not be the application's compliance decision. That guide addresses email obligations within its scope. It does not establish a general legal standard for proving receipt of a logistics notice.

## Derive the scheduled worker from the evidence ledger

The first durable boundary belongs around submission. Assign the notice its own stable identity before sending, then retain the returned message identifier as the lookup handle. Use an idempotency facility only where the selected submission interface documents one, including its scope and retention. If a submission times out before the identifier is recorded, leave an explicit unresolved submission state and reconcile it through documented lookup capabilities or review. Automatically creating another email would substitute a possible duplicate for an unanswered accounting question.

That ambiguity precedes polling.

For status collection, let the scheduler select due ledger records in bounded batches. Claim each record with a lease or equivalent concurrency control, read the provider's documented evidence interface, and apply the observation in a database transaction. The transaction should append evidence, update the derived state, and set the next check time together. This is an exactly-once mindset applied to local effects: repeated execution should produce one durable effect for the same observation. It is not a claim that network delivery happens exactly once.

The important crash boundary is between the remote read and the local commit. If the worker stops there, its next run should repeat the read without inventing another state transition. Where the source supplies a stable event identifier, use it within the provider account and message scope. For a snapshot interface without event identifiers, define a repeatable observation key from the documented fields that actually distinguish evidence. Keep collection time as metadata; using a fresh local timestamp as the deduplication key would make every retry appear new.

A snapshot cannot supply an event history it never returned. Store observations as observations, retaining both source time, when available, and collection time. If a later read contradicts an earlier one, preserve the evidence and apply a documented transition rule instead of deleting the earlier record to make the ledger look consistent.

## A focused cron worker contract

The example deliberately defines internal interfaces rather than guessing a provider route or response schema. Implement `Reader` against a documented status or events read operation; implement `Ledger.Apply` as one local transaction with uniqueness enforcement for the observation key and notice scope. The example assumes an already claimed notice. Its caller owns lease acquisition, release, and recovery after expiry.

```go
package reconciliation

import (
	"context"
	"errors"
	"time"
)

type Notice struct {
	ID        string
	MessageID string
	Deadline  time.Time
}

type Observation struct {
	Key         string
	SourceState string
	SourceTime  time.Time
	CollectedAt time.Time
}

type Reader interface {
	Read(context.Context, string) (Observation, error)
}

type Ledger interface {
	// Apply deduplicates evidence and updates scheduling atomically.
	Apply(context.Context, Notice, Observation, time.Time) error
}

func Check(ctx context.Context, n Notice, r Reader, l Ledger,
	now time.Time, interval time.Duration) error {
	if n.ID == "" || n.MessageID == "" || n.Deadline.IsZero() {
		return errors.New("notice identity and deadline are required")
	}
	if interval <= 0 {
		return errors.New("poll interval must be positive")
	}
	if !now.Before(n.Deadline) {
		return errors.New("evidence deadline reached; review required")
	}
	obs, err := r.Read(ctx, n.MessageID)
	if err != nil {
		return err
	}
	if obs.Key == "" || obs.SourceState == "" {
		return errors.New("incomplete observation")
	}
	obs.CollectedAt = now
	next := now.Add(interval)
	if next.After(n.Deadline) {
		next = n.Deadline
	}
	return l.Apply(ctx, n, obs, next)
}
```

`SourceState` retains the source label; the ledger's policy mapping decides whether that evidence satisfies an application milestone, requires another read, or needs review. Do not impose one universal terminal-state ordering across providers. A source might expose only a current snapshot, while a different interface may expose individual events; integration effort includes understanding that distinction and testing the mapping.

The outer worker must persist failed attempts and schedule retries, including failures returned by `Apply`. It should distinguish authentication failures, temporary read failures, missing records, and local commit failures according to documented responses. Retry timing should respect documented service limits, use bounded backoff, and stop automated collection at the policy deadline. At that point, atomically move the notice into a review queue. The returned deadline error is a signal for that outer workflow, not a durable review record by itself.

No read error proves nondelivery.

For a Node.js cron implementation, keep the scheduler entry point thin: select and claim due notices, call the equivalent check operation with a timeout, persist the outcome, and exit with a bounded amount of work. Scheduling the command every 60 seconds is an illustrative configuration, not a delivery guarantee. A backlog, a missed invocation, or a retry delay can make evidence older than that interval; monitor the age of the oldest unresolved notice and the time since the last successful collection.

## Compare the integration work after defining the contract

Only now compare collection approaches. The useful question is how much code and operational responsibility are required to meet the evidence contract, including recovery, rather than how few lines are needed to launch a timer.

| Collection approach | Work to budget for | Evidence boundary to verify |
| --- | --- | --- |
| Scheduled status snapshots | Due-record selection, claims, repeatable observation keys, and deadline handling | Which states are returned, and whether earlier transitions remain observable |
| Scheduled event reads | Event identity, pagination, checkpoint commits, and replay handling | Event retention, ordering guarantees, and access to older pages |
| Webhook ingestion | An inbound receiver, documented authenticity checks, duplicate handling, and replay operations | Retry behavior and a documented way to recover missed evidence |

For the no-webhook requirement, use a polling design only after verifying that the chosen interface exposes the evidence needed by the notice policy. A sending interface alone does not establish that a retrievable delivery history exists. If the required evidence cannot be read, the open question is a capability gap in the proposed design, not a reason to label a successful submission as delivery.

Choose a narrow adapter around the documented read operation, while leaving notice identity, deadlines, and audit rules in the application. This limits the initial integration surface, although it leaves the team responsible for reconciliation operations. Request volume and evidence storage belong in capacity planning; neither substitutes for proving that missed runs and ambiguous submissions have a recoverable outcome.

## Roll out through replay and reconciliation

Begin with shadow collection for a limited set of notice records, keeping the existing approved compliance decision process authoritative. Exercise repeated observations, an expired claim, a crash before commit, a missing message identifier, and evidence arriving near the deadline. Verify the resulting ledger and review queue. A successful scheduled process exit is insufficient evidence that every due notice was examined.

Then enable automatic milestone updates for the tested state mappings, with explicit review for unmapped states. Restrict access to recipient data, define evidence retention with the relevant compliance owners, and keep credentials out of audit records. The rollout criterion is concrete: an operator can trace each notice revision from submission through collected evidence to its current decision, and can explain why any unresolved record is still waiting.

## Sources

References:

- FTC, CAN-SPAM Act: A Compliance Guide for Business — https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
