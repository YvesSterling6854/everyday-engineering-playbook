# A Node.js Guide to 4 Record Custom Domain Password Reset Email Deliverability Setup

Short answer: treat a password reset email as a stateful delivery transaction, not a fire-and-forget message. Use a dedicated custom-domain identity, align SPF and DKIM with DMARC, increase traffic gradually, consult a durable suppression list before every send, and record four transitions: accepted, handed off, delivered or failed, and consumed or expired. In a healthtech system that also routes contact forms to support queues, the same ledger should preserve routing evidence without retaining the patient's free-text message.

The bill is mostly a retention equation before it is a sending equation. Stored bytes grow with event volume, event size, replica count, and retention time; message bodies and provider payloads are usually the terms an architecture can remove. A useful first estimate is `daily attempts x retained bytes per attempt x retention days x replicas`, calculated separately for immutable delivery facts and bulky diagnostic material. Changing a mail library does not move that dominant term. Keeping compact status records for 90 days while deleting rendered bodies after handoff does.

That decision has a price when something goes wrong: an operator can prove which template version, recipient hash, domain, and disposition participated, but cannot reconstruct sensitive form text or recover the exact email body from the audit store. This is deliberate data minimization, and it makes template versioning and reproducible rendering operational requirements rather than conveniences.

## How should custom domain setup improve password reset email deliverability?

A custom From domain is only the visible identity. SPF authorizes sending infrastructure for a domain, DKIM attaches a verifiable signature to selected headers and the body, and DMARC evaluates aligned identifiers and publishes a handling policy. DKIM is particularly important because RFC 6376 defines the signature mechanism and also explains its boundary: a valid signature associates a message with a signing domain, but does not by itself establish that the message is desirable or trustworthy.

Alignment matters.

A message can pass a low-level authentication check while still failing the organizational-domain relationship expected by DMARC. Configure the return path, signing domain, and visible From domain intentionally; verify the exact DNS records after deployment; then inspect received-message authentication results at test mailboxes. DNS presence alone is a weak release test. I prefer a release gate that saves the observed result beside the configuration version because a screenshot of DNS state cannot explain a later message.

Use separate streams for password resets and support notifications even if they share infrastructure. A healthtech contact form may generate bursts, attachments, or user-supplied text, while account recovery has a narrow template and an urgent delivery expectation. Separate identities, queues, and rate controls contain reputation and operational failures. They also make reconciliation legible: an account-recovery attempt should never disappear inside a backlog created by support routing.

## The four-record delivery ledger

Exactly-once email delivery is not a promise SMTP can give an application. The practical target is exactly-once intent plus idempotent execution: one durable intent per reset request, repeatable handoff attempts, and monotonic status updates. Duplicate transport attempts remain possible, so the reset token must be single-use and the message must make repeated clicks harmless.

I would retain four logical records, although they may live in two tables. The first captures accepted intent with an idempotency key. The second captures each handoff attempt. The third records normalized delivery disposition such as delivered, transient failure, permanent failure, or suppressed. The fourth records the security outcome: token consumed or expired. A contact-form route can use the same pattern with `queue_assigned` replacing token consumption, while its free text stays outside the delivery ledger.

| Record | Durable fields | Retention purpose |
| --- | --- | --- |
| Intent | request ID, recipient hash, template version, created time | Deduplicate and explain why mail was requested |
| Attempt | attempt ID, transport message ID, signing domain, handoff time | Reconcile application and transport |
| Disposition | normalized status, reason class, observed time | Drive retries and suppression |
| Outcome | consumed or expired time | Close the recovery workflow |

Never overwrite a permanent failure with a later ambiguous event.

Append the observation and derive current state under explicit precedence rules. Auditability depends on preserving disagreement. The trade-off is explicit: append-only observations take more space and require a projection before an operator can read current state, but overwriting an older event destroys the evidence needed to reconcile reordered callbacks.

## A small Go boundary behind Node.js

The public application can remain Node.js while the delivery worker boundary is expressed as a small Go service or library, which keeps the example focused on transactional invariants rather than a provider SDK. The database must enforce the unique idempotency key; an in-memory check cannot survive concurrent workers or restarts.

```go
package recovery

import (
    "context"
    "errors"
    "time"
)

type Intent struct {
    ID             string
    IdempotencyKey string
    RecipientHash  string
    Template       string
    CreatedAt      time.Time
}

type Store interface {
    InsertIntent(ctx context.Context, in Intent) (inserted bool, err error)
    IsSuppressed(ctx context.Context, recipientHash string) (bool, error)
    AppendDisposition(ctx context.Context, intentID, status, reason string, at time.Time) error
}

type Transport interface {
    SendReset(ctx context.Context, in Intent) (messageID string, err error)
}

func Dispatch(ctx context.Context, store Store, tx Transport, in Intent) error {
    inserted, err := store.InsertIntent(ctx, in)
    if err != nil {
        return err
    }
    if !inserted {
        return nil // The durable uniqueness constraint already accepted this intent.
    }

    blocked, err := store.IsSuppressed(ctx, in.RecipientHash)
    if err != nil {
        return err
    }
    if blocked {
        return store.AppendDisposition(ctx, in.ID, "suppressed", "policy", time.Now().UTC())
    }

    messageID, err := tx.SendReset(ctx, in)
    if err != nil {
        if recordErr := store.AppendDisposition(ctx, in.ID, "failed", "handoff", time.Now().UTC()); recordErr != nil {
            return errors.Join(err, recordErr)
        }
        return err
    }
    return store.AppendDisposition(ctx, in.ID, "handed_off", messageID, time.Now().UTC())
}
```

The `Transport` implementation should render a fixed template, place the one-time URL in both plain-text and HTML parts, set a stable From identity, and return the transport's message identifier. It should not decide whether a recipient is suppressed. That policy belongs before handoff, where it remains testable and cannot be bypassed by swapping transports.

There is one sharp edge in this minimal sample: recording `failed` after an uncertain handoff does not prove the remote system rejected the message. Production retry logic must distinguish a confirmed pre-handoff failure from an ambiguous timeout and reconcile by message identifier where possible. Blind retry is how duplicate reset messages are created.

## Failure containment during traffic growth

Sender warming is a controlled change in volume, not a calendar ritual. Begin with traffic the system can observe closely, avoid abrupt changes in domain or stream identity, and increase volume only while authentication, permanent-failure, complaint, deferral, and latency signals remain within predeclared limits. There is no universal daily schedule in the cited standards, so a fixed percentage presented as a rule would be false precision.

Suppression needs equally explicit semantics. Confirmed permanent address failures and complaint signals should prevent another routine attempt; transient failures should enter bounded retry with delay, not permanent suppression. Security complicates this rule because an attacker can repeatedly request recovery for another person's address. Keep the external response indistinguishable, rate-limit requests independently of email state, and let an authenticated support process review disputed suppression without exposing whether an account exists.

For contact-form routing, suppressing mail must not discard the support request.

Persist the routing intent first, assign the correct queue, and treat email as one notification channel. Delivery reliability is end-to-end completion, not an SMTP acceptance count. A limitation of this architecture is the operational burden: a small team must maintain reconciliation jobs, precedence rules, retention policy, and a review path for disputed suppression. A simpler synchronous handoff can be appropriate at very low volume when losing the process also fails the request visibly; it becomes the wrong choice once the application must accept work independently of the mail transport.

## Release gates and the evidence worth keeping

Before production, test authentication at receiving systems, duplicate requests with the same idempotency key, simultaneous workers, delayed and reordered delivery events, permanent and transient failures, suppression lookup failure, token reuse, and expiry. Run the same cases after DNS or transport changes. A deployment is incomplete until reconciliation can match every accepted intent to a terminal or explicitly pending state.

The operating dashboard should show counts that balance: accepted intents equal suppressed plus handed-off plus pending; handed-off attempts eventually map to a known disposition or an aged exception. Alert on the exceptions. Percentages without denominators hide small samples, and aggregate delivery rates hide the particular domain or stream that is failing.

Keep compact, immutable evidence: identifiers, hashes, timestamps, template versions, authentication configuration versions, reason classes, and state transitions. Stop keeping reset secrets, rendered bodies, and health-related free text in the delivery ledger. During an investigation, the loss is real: operators can establish the path and policy decision but may be unable to reproduce content byte for byte. That trade-off is preferable when the retained content would enlarge the security and compliance surface without improving routine reconciliation. Four records are enough to answer the routine questions, but they are not a forensic archive of message content; accepting that boundary is the cost of retaining less sensitive data.

## Further reading

- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- Transactional Email Best Practices: https://postmarkapp.com/guides/transactional-email-best-practices
