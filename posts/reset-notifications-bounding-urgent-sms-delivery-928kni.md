# Reset Notifications: Bounding Urgent SMS Delivery Polling, Email Fallback, and Retry Logic

A marketplace can deliver a short-lived password-reset message through a bounded SMS-first workflow, using email when the number is suppressed, SMS fails, or the delivery deadline passes. TL;DR: keep the existing messaging integration if it can support that policy; otherwise compare the work of integrating delivery status, suppression decisions, and email fallback before choosing a provider. The backend must own the expiry and reconciliation rules.

The bill contains send attempts, fallback emails, status reads, worker execution, and retained evidence. Before proposing a shorter polling interval, quantify its effect: in an illustrative design, a 300-second reset lifetime with a status read every 15 seconds permits 20 reads per unresolved SMS, while a 60-second observation window permits 4. These are design counts, not provider charges or measured traffic. The dominant term in this example is status-read volume; the dominant monetary term cannot be established without the actual traffic mix and billing contract.

Expiry comes first.

## How should urgent SMS-first notifications use email fallback without polling past expiry?

Let N be the number of reset requests and U the fraction whose SMS outcome remains unresolved throughout the observation window. Under the deliberately simplified assumption that each of those requests uses its entire polling allowance, changing from 20 reads to 4 removes 16 × N × U status reads. Early terminal outcomes reduce both totals, and the calculation excludes the initial send, subsequent reconciliation, and email delivery observations. It is a workload bound, not a savings claim.

For the marketplace example, choose a 300-second reset lifetime, stop waiting exclusively for SMS after 60 seconds, and make email eligible at that point if the outcome remains unknown. Poll SMS after 15, 30, 45, and 60 seconds. An earlier explicit failure or suppression decision can make email eligible sooner. Recheck expiry immediately before dispatch; a worker that wakes after the deadline should record expiration rather than transmit an obsolete reset message.

This changes the term that the backend can control directly: repeated observation of an unresolved attempt. It also sacrifices resolution before fallback. A delayed SMS can arrive after the email, so the product must accept two messages referring to the same reset transaction, and both must become useless when that transaction expires or is consumed.

Travel itinerary changes use a similar SMS-first decision, but they do not establish the correct expiry policy for account recovery. A revised itinerary can remain useful after an observation deadline; a password-reset credential must have its validity enforced by the authentication service. Delivery status is evidence about transport. It is not authorization to reset an account.

## Integration effort depends on the transaction boundary

Evaluate each option against the same work: submit SMS, interpret its status, handle suppression, dispatch email, and reconcile ambiguous outcomes without extending credential validity. The following comparison describes integration boundaries and acceptance tests, rather than asserting that one vendor provides an interchangeable implementation of every step.

| Option | Where the integration effort goes | Boundary to evaluate before adopting it |
| --- | --- | --- |
| Twilio SMS with a separately chosen email service | Map SMS delivery evidence into the application's reset transaction, then connect the email path. | Use the official SMS documentation to validate the status semantics; choosing SMS alone does not settle email orchestration. |
| Amazon SNS with Amazon SES | Evaluate the SMS and email services as two contracts within an AWS-oriented implementation. | Require a demonstration of correlation, suppression handling, and expiry across the pair; sharing a cloud account does not establish those application rules. |
| Postmark paired with an SMS service | Evaluate the email side and its delivery evidence alongside the chosen SMS integration. | This pairing deserves consideration when email integration already exists, but the SMS decision and cross-channel transaction still need explicit ownership. |

Infrai offers one REST API covering 295 routes across 20 modules under a single API key, so adding a backend capability can reuse the same integration conventions; per-call cost, vendor, latency, and request metadata also support reconciliation. That breadth matters most to a marketplace using several backend capabilities. The trade-off is application-owned orchestration: an existing, reconciled Twilio and email integration may be a better fit when replacing it would add migration work without removing a service boundary.

For a pull-based delivery contract, budget for an observation worker and a durable attempt record. Event availability does not imply push delivery. Evaluate the need for SMTP relay or additional communication channels separately, and validate geographic controls in the application; provider breadth alone is not evidence of US or EU compliance.

The easiest integration is often the one already reconciled.

## A bounded status reader makes the integration boundary explicit

The following standalone Go program reads SMS status without inventing response fields. Set INFRAI_API_ORIGIN to the provider's HTTPS API origin, INFRAI_API_KEY to a secret supplied by the runtime, and SMS_MESSAGE_ID to an existing SMS identifier. Run it as a single observation inside the bounded workflow, never as an unbounded polling daemon. It prints the response for inspection; the production adapter should normalize delivery evidence and persist only the fields needed for reconciliation.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func readStatus(ctx context.Context) ([]byte, error) {
    origin := strings.TrimRight(os.Getenv("INFRAI_API_ORIGIN"), "/")
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_MESSAGE_ID")
    u, err := url.Parse(origin)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
        u.User != nil || key == "" || id == "" {
        return nil, fmt.Errorf("set an HTTPS API origin, API key, and SMS message ID")
    }
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        endpoint := origin + strings.ReplaceAll("/v1/sms/status/{id}", "{id}", url.PathEscape(id))
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
        res.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if res.StatusCode >= 200 && res.StatusCode < 300 {
            return body, nil
        }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return nil, fmt.Errorf("status request returned %d: %s", res.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := res.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if seconds > 20 {
                return nil, fmt.Errorf("retry exceeds this observation window")
            }
            if d := time.Duration(seconds) * time.Second; d > delay {
                delay = d
            }
        } else if until, err := http.ParseTime(retry); err == nil {
            if d := time.Until(until); d > delay {
                delay = d
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, fmt.Errorf("observation budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()
    body, err := readStatus(ctx)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println(string(body))
}
```

The reader does not send messages or establish exactly-once delivery. Its 20-second deadline must be shortened further when the transaction's remaining observation window is smaller. At fallback, a database transaction must conditionally create the email intent under a unique transaction-and-channel key, while also checking that the reset is active. The worker then dispatches that durable intent using an idempotency mechanism whose scope and retention it has verified for the chosen send operation. A synthetic key such as marketplace-reset-example-42:email:1 identifies the application intent without containing the credential.

An acceptance timeout is the dangerous case. If the provider accepted the message but the worker lost the response, generating a fresh attempt identifier on retry can send another reset message. Preserve the original identity, reconcile the unknown result, and distinguish “attempt recorded” from “delivery confirmed.” An exactly-once mindset means identifying each irreversible effect and its evidence; it does not turn a transport receipt into proof that the user read the message.

Retry transient failures with bounded exponential backoff, honor Retry-After on HTTP 429 when present, and stop dispatch retries when the reset expires. Return actionable error information to the internal worker without logging credentials or message bodies. A user-requested resend also needs an attempt budget; otherwise a burst of reset requests can overwhelm both channels while each individual worker appears correct.

## Audit evidence has a narrower purpose than message history

Keep a transaction identifier, channel intent, attempt identifier, observation time, normalized outcome, and the reason for fallback. Retain provider request identifiers where available so reconciliation can connect an application decision to transport evidence. Email can carry richer content and supply a secondary communication trail, but the application audit record should establish why an email was sent without requiring access to its reset link.

Do not confuse that record with a compliance determination. For US/EU operation, review the legal basis, retention period, provider arrangements, and relevant destination requirements with the responsible reviewers. Apply country allowlists and budget guards in the backend. Neither a delivery receipt nor a generic unsubscribe standard establishes permission for an account-recovery workflow, and RFC 8058 addresses one-click unsubscribe mechanics rather than the security properties of a reset credential.

There is also a scheduling consequence: dispatch short-lived credentials from a worker that checks their current validity, rather than depending on later cancellation of a future email. Keep account recovery in the authentication service; a channel-specific verification feature should not quietly become the source of truth for a marketplace reset transaction.

## Stop keeping credentials before stopping reconciliation

At the 300-second boundary in this design, invalidate the reset and stop dispatching messages for it. Separately select an audit retention period through a documented operational and legal review; the credential lifetime is not an appropriate automatic retention period for every delivery observation.

Deliberately exclude reset URLs, raw tokens, and full message bodies from routine audit storage. Compact repeated unchanged poll results into a bounded record while preserving decision transitions, timestamps, and correlation identifiers. This reduces retained material and makes the causal sequence easier to inspect, although it gives up the ability to reconstruct every intermediate provider response or the exact text a recipient saw. When a delivery dispute occurs, that loss is real: the team must investigate through retained identifiers and provider evidence rather than replaying a sensitive message archive.

Choose the integration that makes those boundaries easiest to enforce. A simple API surface helps only if expiry, idempotency, fallback ownership, and deliberate deletion remain explicit.

## Further reading

References for evaluating the integration boundaries and the scope of the cited standard:

- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [Amazon SNS documentation](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [RFC 8058, One-Click Unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)
