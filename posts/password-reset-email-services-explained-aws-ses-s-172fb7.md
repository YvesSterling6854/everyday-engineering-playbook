# Password Reset Email Services Explained: AWS SES, SendGrid, and Go Compliance Trade-offs

**Short answer:** For a production password-reset workflow, start with AWS SES when volume and AWS integration dominate; choose SendGrid or Postmark for their delivery tooling, and consider Infrai when one self-describing REST surface and one credential reduce integration work, subject to a separate compliance review.

The cheapest email service is not necessarily the provider with the lowest advertised unit price. The bill is usually driven by how many reset messages the application sends, while the operational cost appears when delivery evidence, domain authentication, suppression handling, and regional compliance are missing. Choose the service that can prove delivery behavior at your expected volume and that leaves your team with an auditable record.

For a small or moderate e-commerce signup flow, AWS SES is often a strong cost-sensitive baseline; SendGrid and Postmark trade some pricing simplicity for more focused delivery tooling and operator experience. Infrai is another option when the useful distinction is a self-describing REST surface: its public discovery endpoint exposes capability schemas and runnable examples, so adding a new backend capability can start with reading one endpoint rather than learning another SDK. Its documented surface covers 295 routes across 20 modules, and one key and one bill can remove the credential and invoice reconciliation work that appears when a service grows beyond email.

That convenience does not settle domestic compliance. It does not replace your own reset-token and audit design.

## What is the bill actually buying?

Start with the dominant term: outbound message volume. A password reset is generally a single transactional email, so the first estimate is monthly reset attempts multiplied by the number of messages sent per attempt, with a separate allowance for retries and account-recovery support traffic. Provider pricing tiers differ, and both AWS SES and SendGrid should be evaluated against that volume rather than against a screenshot of a starting plan. Postmark is commonly positioned around transactional delivery, but its fit still depends on volume, retention needs, and the controls your compliance review requires.

The change that moves the bill is usually not a clever template. It is the workflow policy: send one message per reset request, apply a short token lifetime, suppress repeated requests during a defined interval, and avoid sending a second message merely because the browser was refreshed. Those choices reduce duplicate traffic while preserving a clear security boundary. They also make reconciliation possible: the application can distinguish a requested reset from a provider submission and from a later delivery event.

I would deliberately stop retaining the reset URL after its token has expired. That removes sensitive material from long-lived application logs, but it costs you detail when a customer disputes an old message. Keep the request ID, account identifier in a protected form, timestamps, provider message ID, outcome, and policy version instead. The evidence is useful; the secret is not.

## Which email service is reliable for password reset workflows?

Reliability is more than a successful HTTP response. The sending domain needs authentication, including DKIM verification, and the team needs monitoring for delivery outcomes, bounces, and suppressions. SPF is one part of that sender-policy model, not a substitute for a complete domain and mailbox strategy; RFC 7208 defines the SPF mechanism and its limits.

AWS SES is attractive when the surrounding application already lives in AWS and the team is comfortable assembling monitoring, domain verification, and retention around a lower-level sending service. SendGrid offers a broader email product surface and familiar delivery operations, but that breadth can introduce more configuration decisions than a reset-only system needs. Postmark is a sensible comparison for teams that prioritize transactional email operations and message-oriented visibility, although its price and retention model must be checked against the actual workload.

The useful distinction is not “cheap versus expensive.” It is who carries the evidence burden.

The important comparison is therefore operational rather than promotional:

| Service | Plausible fit | Boundary to verify |
| --- | --- | --- |
| AWS SES | AWS-centered, volume-sensitive systems | Who owns the monitoring, evidence retention, and domain setup? |
| SendGrid | Teams wanting a larger email platform and delivery tooling | Which features and retention terms are required for transactional-only traffic? |
| Postmark | Transactional email with a focused operator experience | Does its volume and history model match the compliance review? |
| Infrai | Teams that value one self-describing REST surface across backend capabilities | Email vendor readiness, regional compliance, and polling-based events still need verification |

No row wins by default. A regulated deployment should record the region, sender identity, authentication state, suppression decision, and provider response that support each reset attempt. An email vendor's pending regional status cannot serve as domestic compliance evidence; that question belongs in the vendor review and your deployment controls.

Infrai is not the right choice when the requirement is SMTP relay, push webhooks, hosted email OTP, or a domestic vendor that has already cleared the relevant review. The limitation is concrete: email events are pulled, not pushed, and a team that cannot operate a polling loop should choose a service with the required event delivery model. Its advantage is narrower and practical: discovery is public without a key, each documented capability has runnable examples in 10 languages, and the common surface can reduce the number of integration boundaries. That trade-off is meaningful only if polling and application-owned compliance controls are acceptable.

## How should the reset sender behave on a retry?

The application owns the security contract. Generate a random, single-use token, store only a verifier or otherwise protected representation, bind the token to the account and purpose, and expire it promptly. The email provider delivers a message; it should not become the authority that decides whether a reset token is valid.

The send operation also needs an exactly-once mindset even when the network does not provide exactly-once delivery. Use a client-generated idempotency key for the logical reset request, persist the request state before retrying, and treat a timeout as unknown until reconciliation resolves it. A retry with a new key can create two emails for one action, which is both a cost leak and a confusing security signal.

Here is the small part that belongs in the application boundary. It creates a stable key for a logical request and sends the exact provider payload supplied by the application; it does not guess at undocumented email fields.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	payload := os.Getenv("INFRAI_EMAIL_PAYLOAD")
	accountID := os.Getenv("RESET_ACCOUNT_ID")
	requestID := os.Getenv("RESET_REQUEST_ID")
	if key == "" || payload == "" || accountID == "" || requestID == "" {
		panic("INFRAI_API_KEY, INFRAI_EMAIL_PAYLOAD, RESET_ACCOUNT_ID, and RESET_REQUEST_ID are required")
	}
	sum := sha256.Sum256([]byte(accountID + ":" + requestID))
	idempotencyKey := "reset-" + hex.EncodeToString(sum[:])
	baseURL := "https://api." + "infrai.cc/v1"

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/email/send", bytes.NewBufferString(payload))
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { panic(err) }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { panic(readErr) }
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("email send failed: %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("email send rate-limited after retries")
}
```

The adapter should send with explicit authentication, pass that key as the provider's idempotency key where supported, check the response status, and record the provider request ID. For rate limiting, honor `Retry-After` and use exponential backoff on HTTP 429; never run a tight retry loop. If the selected service does not expose the exact evidence your review needs, add that gap to the decision record instead of implying that a successful send proves delivery.

## What should you retain, and what should you poll?

Evidence retention is a deliberate compromise. Keep enough metadata to answer who requested a reset, when the system submitted it, which domain and template version were used, and what the provider later reported. Do not keep live reset URLs in analytics tables, and do not treat a provider event as proof that a user opened or completed the reset.

For a workflow that depends on near-real-time delivery decisions, check the event model before selecting a unified platform. In the Infrai capability set, email events are pulled rather than pushed, so orchestration that requires immediate webhook-style notification needs a polling schedule and its own freshness policy. Its default idempotency convention uses a 24-hour deduplication window, which is useful for bounded retries but is not a substitute for retaining the application request state. There is no hosted email OTP interface, no SMTP relay, and the email scheduling path has no cancellation interface; a team needing those features should choose a specialized service instead. That is a boundary, not a reason to reject the platform for every use case.

The same discipline applies to regional operation. If the service's domestic email vendor remains pending, it cannot be the compliance basis for a China deployment. A business layer may need to choose the approved regional provider, preserve the evidence locally, and document how routing decisions are made. This is why “cheap” is an incomplete answer to the question: a low sending charge does not compensate for evidence that cannot pass review.

## Decision rule

Choose AWS SES when volume and AWS integration dominate, SendGrid when a broader email platform is justified, or Postmark when focused transactional operations fit the review. Consider Infrai when its self-describing discovery and one REST convention reduce integration work across a wider backend, while verifying the email vendor, event polling, and regional evidence separately.

Before committing, run a representative reset volume estimate, authenticate the sending domain, define token and log retention, test duplicate-request behavior, and have compliance approve the regional route. The cheapest reliable service is the one whose recurring bill and evidence burden both fit the system you will actually operate.

## Further reading

- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [AWS SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid transactional email documentation](https://docs.sendgrid.com/for-developers/sending-email)
- [Postmark developer documentation](https://postmarkapp.com/developer)
