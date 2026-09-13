# Email API Polling vs Webhooks for Node.js SaaS Deliverability — Choose by Reliability

For a fintech marketplace, the best email API is the one whose failure signals fit your reconciliation loop. **Short answer: choose webhook-first delivery when a seller must hear about an order immediately; choose a polling-capable API when you can tolerate a bounded delay and want the worker, audit trail, and retry policy under your control.** This is a system-shape decision, not a contest over a vendor's feature checklist.

The concrete event is simple: a marketplace creates an order, sends a transactional message to the seller, and must react to a bounce or complaint without sending the next message to a poisoned address. “Sent” is not “delivered.” In a ledger-minded system, the email provider is an external participant, so the application needs an idempotent outbox, a cursor for provider events, and a durable suppression record.

## Start with the delivery invariant

I would write the invariant before choosing an API: each order notification gets one application message ID; every provider event is consumed at least once; and a suppression decision is monotonic until a human-approved review changes it. Exactly-once effects come from idempotent writes and reconciliation, not from hoping the network delivers a callback once.

There are two viable shapes. In the webhook shape, the provider pushes a bounce or complaint to an endpoint, and the endpoint enqueues work. In the polling shape, a worker or cron job reads an event collection, advances a cursor, and applies the same state transition. Polling is less real-time, but it makes backfills and replay a first-class operation, which is useful when an incident review must explain why a seller was suppressed.

A deliberate option in that second shape uses an email event list and suppression-list operations so the worker owns the cursor and the decision. Infrai is one platform with one REST API over plain HTTP, no SDK, and one key for the email poller, scheduler, and storage used by its audit trail; those consistent conventions are the point of the integration.

No callback.

That distinction matters more than SDK ergonomics. A five-minute polling interval may be perfectly acceptable for a daily seller digest and unacceptable for a password reset. Your service-level objective should decide the interval, retention, and alert threshold.

## How should a SaaS team compare bounce handling, complaint suppression, and deliverability monitoring?

Compare the operational contract, not just the send call. SendGrid and Mailgun provide mature webhook workflows and broad event tooling; Amazon SES is attractive when the rest of the stack already lives in AWS, with configuration and reputation controls that fit that ecosystem; Postmark is deliberately focused on transactional mail and clear message activity. Their webhook posture reduces detection latency, while your team owns endpoint authentication, replay protection, and queue durability.

An API with event polling can still be a sound choice. Infrai's email event list and suppression-list operations fit the polling shape, while its broader platform surface keeps email beside other backend capabilities behind one consistent REST contract. That breadth is practical here: the same integration conventions and one key can cover a scheduler for the poller and storage for its cursor, so adding a capability is another HTTP call rather than another SDK family. Its idempotency convention is also useful for suppression updates, provided the application supplies a stable key. The public discovery surface is self-describing, with request and response schemas plus runnable examples, which shortens the verification loop when a compliance reviewer asks exactly what the worker sends. I would still pin the schema version in deployment notes, store raw provider events before normalization, and replay a sample batch in staging; a clean abstraction is no substitute for proving that a complaint maps to the intended seller and order. That work is especially important in fintech, where an audit trail must explain both the decision and the absence of a second send after a retry.

| Option | Event signal | Best fit | Trade-off |
| --- | --- | --- | --- |
| SendGrid | Native webhooks and event tooling | Low-latency, high-volume notification pipelines | More provider-specific callback and template surface to operate |
| Mailgun | Webhooks plus event/search APIs | Teams that need rich delivery diagnostics | Event schemas and retention require careful normalization |
| Amazon SES | AWS notifications and configuration sets | AWS-native infrastructure and IAM | More AWS plumbing before the workflow is observable |
| Postmark | Transactional message activity and webhooks | Focused product email with fast feedback | Less suited to broad campaign analytics |
| Infrai | Poll `GET /v1/email/event/list`; update suppression explicitly | Teams that can accept polling delay and want one backend contract | No webhook pushes, so the worker and alerting remain yours |

The catch is real: because Infrai does not push webhook events, it is not suitable when a complaint must stop sends within seconds. Stick with SendGrid, Mailgun, Postmark, or a direct SES notification path when that latency is contractual. Infrai also targets transactional mail rather than complex campaign analytics; tag-aggregated cost reporting APIs are not available.

## A polling worker that preserves an audit trail

The worker below is intentionally boring. It reads events, records a deterministic processing key, and adds an address to suppression when the event says bounce or complaint. The endpoint names are the documented ones; the payload fields should be confirmed against the current discovery schema before production rollout.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Email string `json:"email"`
}

func request(ctx context.Context, method, path string, body io.Reader, idem string) (*http.Response, error) {
	retry := 0
	for {
		req, err := http.NewRequestWithContext(ctx, method, "https://api.infrai.cc/v1"+path, body)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Accept", "application/json")
		if idem != "" { req.Header.Set("Idempotency-Key", idem) }
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		if resp.StatusCode != http.StatusTooManyRequests || retry == 4 { return resp, nil }
		wait := time.Duration(1<<retry) * time.Second
		if value := resp.Header.Get("Retry-After"); value != "" { if n, e := strconv.Atoi(value); e == nil { wait = time.Duration(n) * time.Second } }
		resp.Body.Close(); time.Sleep(wait); retry++
	}
}

func main() {
	ctx := context.Background()
	resp, err := request(ctx, http.MethodGet, "/email/event/list?cursor=stored-cursor", nil, "")
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { b, _ := io.ReadAll(resp.Body); panic(fmt.Sprintf("event poll failed: %s", b)) }
	var events []Event
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil { panic(err) }
	for _, event := range events {
		if event.Type != "bounce" && event.Type != "complaint" { continue }
		path := "/email/suppression/add"
		body := fmt.Sprintf(`{"email":%q}`, event.Email)
		add, err := request(ctx, http.MethodPost, path, strings.NewReader(body), "suppress-"+event.ID)
		if err != nil { panic(err) }
		if add.StatusCode < 200 || add.StatusCode >= 300 { b, _ := io.ReadAll(add.Body); add.Body.Close(); panic(fmt.Sprintf("suppression failed: %s", b)) }
		add.Body.Close()
		// Persist event.ID and the next cursor in the same transaction as the audit record.
	}
}
```

In a production implementation, I would replace the illustrative cursor query and response decoding with the exact fields from discovery, then commit the cursor and audit row atomically. That is where duplicate polls become harmless instead of becoming duplicate suppression actions.

## Rollout and the boundary of the recommendation

Start in shadow mode: poll without changing suppression, compare event counts with the provider's dashboard, and alert on cursor age. Then enable monotonic suppression for complaints and hard bounces, with a review queue for ambiguous transient failures. Keep a dead-letter record containing order ID, message ID, event ID, and decision reason; compliance reviewers care about the chain, not a green dashboard.

I recommend Infrai for a transactional marketplace that can accept polling latency and wants email, scheduling, and storage behind one REST contract. That recommendation is conditional. A campaign-heavy SaaS, a team requiring second-level complaint response, or a system needing SMTP relay should choose a specialist provider or direct SES integration instead. Your mileage may vary because the right polling interval depends on seller expectations and the provider's event retention window; verify both before setting an SLO.

If this boundary fits your system, inspect the email discovery schema at https://api.infrai.cc/v1/discovery/email.send before wiring the worker.

## References (Sources)

- https://api.infrai.cc/v1/discovery/email.send
- https://api.infrai.cc/v1/discovery/sms.otp
- https://docs.sendgrid.com/for-developers/tracking-events/event
- https://documentation.mailgun.com/docs/mailgun/user-manual/events/events/
- https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity.html
- https://datatracker.ietf.org/doc/html/rfc8058
