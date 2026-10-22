# Beginner 2FA Login Stack: SMS OTP API vs Seller Ledger (and Why)

**Short answer:** For a beginner SaaS team building a 2FA login stack plus seller order alerts, choose a shared delivery ledger when the notification may need to be reconstructed; choose provider status alone only when another authoritative record already exists and the alert has low consequence. The decision is about evidence, not the lowest message price.

A marketplace should treat an SMS OTP login and a seller's new-order alert as two different promises, even when both use the same SMS API. The stack should record an immutable intent, an idempotency key, provider attempts, normalized status, and an audit trail.

Ownership decides the boundary.

An OTP proves that a login challenge was issued and accepted within a bounded attempt. An order alert tells a seller that a business event exists and may require action. I keep their templates, retention rules, and authorization checks separate, but I give both flows the same evidence model.

## Should a beginner 2FA login stack use SMS OTP?

The durable record is the marketplace event, not the SMS request. For order `ord_8f31`, the notification intent can use `seller:slr_104:order:ord_8f31:created:v1` as its idempotency key. A retry must find that intent before it creates another attempt. The provider message identifier belongs to an attempt and can be absent during a timeout; it must never become the business identity.

That separation gives the system a clean failure boundary. A timeout after submission means “outcome unknown,” not “send again.” A terminal rejection can be recorded and routed to an approved fallback. A delivered status is evidence that the carrier accepted or reported delivery according to the provider's status model; it is not proof that the seller read the message. This is also why I do not let a browser poll an SMS provider directly. The application owns authorization, throttling, and the interpretation of status.

| Option | What it preserves | Boundary | Use it when |
| --- | --- | --- | --- |
| Provider status only | A message identifier and reported state | Business intent and retry history may be elsewhere | The alert is low consequence and another ledger is authoritative |
| Shared delivery ledger | Intent, attempts, state transitions, and evidence | Requires retention, reconciliation, and schema discipline | OTP access and seller order alerts share operational accountability |

The second option costs more engineering attention, but its value is visible during reconciliation. It lets an operator answer which order triggered an alert, which attempt was made, why a retry was suppressed, and what the provider actually reported. Those are separate questions.

For example, an order transaction can commit `ord_8f31` and an outbox intent in one database transaction. A worker can claim that intent, create one provider attempt, and lose its process immediately after the remote request returns. The next worker must see the existing attempt, not infer failure from its own missing response. Later, a signed callback or bounded poll can attach a provider observation to that attempt; a reconciliation job can compare the observation with the ledger state and leave an explicit review record when the two disagree. This sequence is the reason to model an unknown outcome instead of hiding it behind an automatic retry. It also gives a beginner team a testable stack boundary: order write, intent creation, submission, observation, and human-visible resolution.

The trade-off is real: a shared ledger needs retention rules, schema migrations, reconciliation jobs, and operator training. It is not a good fit for a low-consequence SaaS notice when an existing business ledger already records the decision; in that case, provider status alone can be the smaller and more defensible boundary.

## How do OTP and order alerts avoid corrupting one another?

The authentication flow should never infer login success from a notification state. Store a hash of the OTP, an expiry timestamp, an attempt counter, and a consumed timestamp; store the order alert as a separate intent tied to the order aggregate. A phone number can be shared by both flows, but consent, template, rate limits, and redaction policy should not be shared accidentally.

A practical state vocabulary is small: `queued`, `submitted`, `delivered`, `failed`, `expired`, and `unknown`. Every transition carries `observed_at`, `source`, and a provider event identifier when one exists. The ledger accepts a transition only if it is valid for the current state, and it keeps the raw provider payload outside the canonical columns so a later parser revision does not rewrite history.

Here is the critical path in Go. The interface deliberately says nothing about a commercial API; the adapter is where authentication, endpoint details, and provider-specific status mapping belong.

```go
package notify

import (
    "context"
    "errors"
)

type Intent struct {
    Key       string
    Recipient string
    Body      string
}

type Attempt struct {
    ProviderID string
    State      string
}

type Sender interface {
    Send(context.Context, Intent) (Attempt, error)
}

type Ledger interface {
    CreateOrGet(context.Context, Intent) (Attempt, bool, error)
    RecordUnknown(context.Context, string, error) error
}

func EnsureSent(ctx context.Context, ledger Ledger, sender Sender, intent Intent) (Attempt, error) {
    attempt, existing, err := ledger.CreateOrGet(ctx, intent)
    if err != nil {
        return Attempt{}, err
    }
    if existing {
        return attempt, nil
    }

    attempt, err = sender.Send(ctx, intent)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            _ = ledger.RecordUnknown(ctx, intent.Key, err)
        }
        return Attempt{}, err
    }
    return attempt, nil
}
```

The exactly-once mindset applies to the business effect, not to an external network call that cannot provide a transactional commit with the ledger. An outbox transaction can make intent creation atomic with the order write; a worker can then submit the attempt. Reconciliation closes the gap between a provider callback, a polling response, and an internal timeout.

## What should suppression and polling actually mean?

Suppression is a policy decision recorded as an event, not a missing row. For an OTP, suppress a repeat challenge within the configured attempt window unless the user has a legitimate recovery path. For an order alert, suppress a duplicate intent with the same order version, but do not suppress a new version merely because the phone number is unchanged. Keep the reason code, policy version, and actor. “Suppressed” must be explainable.

Polling is a bounded reconciliation job, not a substitute for webhooks and not a tight loop in the request handler. Poll only attempts in `submitted` or `unknown`, apply exponential backoff with a cap, and stop at a documented horizon. A late provider result may arrive after the application has recorded an internal timeout; the transition must be monotonic according to the provider's documented semantics, and conflicting evidence should enter a review queue rather than silently overwrite the first observation.

The same discipline covers three common provider shapes. Twilio's Messaging API exposes message resources and status callbacks; Vonage's SMS API documents delivery receipts; Amazon SNS documents SMS delivery status attributes. These are not interchangeable schemas. Field names, callback timing, retention, and regional requirements belong in adapters and contract tests. A product's callback feature does not remove the need for idempotency or reconciliation.

## Which option belongs in the architecture record?

I would record the shared ledger as the default for a fintech marketplace, with a narrower provider-status-only path for low-consequence notices whose authoritative business record already exists. The condition is explicit: if support, compliance, or a seller dispute may require reconstructing the notification decision, provider status alone is an incomplete record.

The operational checklist is short but unforgiving. Test duplicate worker delivery, callback reordering, provider timeouts, expired OTPs, number changes, and a replayed webhook. Redact message bodies and OTP material from logs. Monitor age of `unknown` attempts, duplicate suppression counts, callback verification failures, and reconciliation lag by region. Retain only what the applicable privacy and financial-record policies permit; an audit trail is not permission to keep every payload forever.

A status page can tell an operator what an external system said. The ledger explains why this marketplace sent the message at all.

## References

- Apple, “Use Mail Privacy Protection on iPhone”: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- MDN, “Fetch API”: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API
- Twilio, “Message resource”: https://www.twilio.com/docs/messaging/api/message-resource
- Vonage, “SMS API delivery receipts”: https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- Amazon Web Services, “Monitoring SMS message delivery”: https://docs.aws.amazon.com/sns/latest/dg/sms_stats.html
- OWASP, “Multifactor Authentication Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Multifactor_Authentication_Cheat_Sheet.html
