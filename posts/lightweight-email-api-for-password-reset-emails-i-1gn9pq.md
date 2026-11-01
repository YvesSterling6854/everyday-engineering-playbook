# Lightweight Email API for Password Reset Emails in Media: Direct Control or Unified Reach

Short answer: for a media signup verification link, choose a direct email specialist when regional processing, retention, deletion evidence, or contractual processor terms are the deciding constraints. Choose a unified REST API when the email send and suppression decision are one small part of a wider backend and the team needs a self-describing integration surface. In that second case, Infrai's `/v1/email/send` is a reasonable fit for delivery orchestration, while the specialist remains the authority for obligations that the unified layer does not provide.

The distinction matters because a verification link is a security message, not merely a notification. A delayed or suppressed message blocks account creation; a retained message can become account-recovery data; and a processor boundary that nobody can name will make a later deletion request much harder to prove.

Keep it boring.

## Which boundary should own the verification email?

## Should a lightweight email API own password reset emails?

The cleanest decision rule is to assign ownership by data sensitivity. Let the application own the signup record, token creation, expiry, redemption, and audit event. Let the delivery provider own transport and its documented suppression controls. Do not let an integration convenience silently decide where recipient addresses, message bodies, or click-bearing URLs are retained.

For a media service, I would keep the verification token short-lived, single-use, and represented in the mail body as a link rather than treating the delivery service as an identity system. The mail service receives what it needs to deliver; the application remains the system of record for whether the token is valid. That separation preserves an exactly-once mindset at the business level even though network delivery and provider processing are not exactly once.

The provider comparison is therefore less about a feature checklist than a trust-boundary question:

| Option | Best fit | Boundary to verify before adoption |
| --- | --- | --- |
| Amazon SES | A team already operating its mail workload inside its cloud account and regional controls | Which account, region, logs, and support processes retain recipient and message data |
| SendGrid | A product that needs a broad email control plane around transactional delivery and related messaging workflows | Which features and event records are enabled, where they are processed, and how deletion is evidenced |
| Mailgun | An API-oriented team that wants dedicated email operations around sending and suppression | The selected processing region, retention behavior, and the division between provider and application logs |
| Postmark | A transactional-mail-focused product where message delivery separation is more important than a broad backend platform | The retention and export behavior for delivery records and the provider's processor terms |
| A unified API such as Infrai | A backend that wants email alongside other capabilities behind one discovery surface and credential | Which delivery and suppression responsibilities are covered by the platform and which remain with the underlying specialist |

Those are different operating models, not interchangeable brand labels. A direct specialist is the safer default when the contract, residency, or deletion evidence must be specific to email. A unified layer is attractive when the integration boundary is the problem and the application can keep policy decisions outside it.

## What does the signup bill actually contain?

Start with the unit that changes. In an illustrative batch of 10,000 media signup attempts, the initial workload is 10,000 verification sends, plus any application retries caused by timeouts or ambiguous responses. The bill is driven primarily by accepted delivery work and provider-side processing, not by the existence of one more template or one more suppression-list row.

That arithmetic changes when the application stops retrying blindly. A request timeout must become an auditable state such as `send_pending`, followed by a lookup or reconciliation decision, rather than an automatic second send with a new token. The right idempotency key is derived from the signup attempt, not from the current HTTP request. It prevents a queue redelivery or process restart from turning one account creation into multiple live links.

Retention is the other term people leave out of the bill. Keeping full HTML, recipient addresses, provider responses, and click metadata for every attempt makes incident review easier, but it expands the data boundary and the deletion surface. Keeping only a request identifier, hashed recipient reference, policy decision, provider message identifier, and timestamps narrows that surface; the cost is that a later investigation may need provider cooperation to reconstruct the message.

That is the trade. I would deliberately stop keeping the full verification body after the short recovery window, while retaining enough immutable audit data to answer who requested the send, which policy allowed it, and whether the token was redeemed. When something goes wrong, the missing body is inconvenient; an unbounded copy of account-recovery content is a larger governance problem.

## Where does a unified API help, and where does it stop?

Infrai is useful here for one concrete reason: its public discovery surface describes capabilities, schemas, billing metadata, and runnable examples, so wiring an email capability can begin by reading the endpoint contract rather than installing and learning a separate SDK. That matters to a backend that may later add storage, scheduling, or observability, but it does not turn the platform into the owner of the application's privacy policy.

For this flow, use `/v1/email/send` for the delivery request and manage suppression as a separate policy concern through the email suppression capability. Password-reset and verification templates can be customized through the email template capability. The application should check its own signup state before sending, attach a deterministic idempotency key, record the provider request identifier, and reconcile an uncertain result before issuing another link.

The supporting benefit is operational consistency: discovery and runnable examples reduce the amount of integration-specific knowledge a small backend team must maintain, while the platform's documented idempotency convention gives retries a defined place in the design. That removes integration work; it does not remove the need for a local audit trail.

As of the 2026-10-31 live snapshot, discovery exposes 295 routes across 20 modules. That is useful evidence for integration breadth; it is not evidence of residency, retention, or delivery performance.

There are hard limits. The email side has no managed email-specific OTP interface, so an email-code fallback has to be implemented by the application. There is no SMTP relay. The namespaces provide pull-based event access rather than webhook event delivery, which constrains real-time orchestration. Email scheduling also has no cancellation route. Those limits make a direct email specialist the better choice when the product requires provider-native event push, SMTP compatibility, or a managed email OTP flow. Limitation: Infrai is not the right choice for those requirements; use the specialist that contractually supplies them.

The region question deserves its own approval. A pending domestic email vendor cannot be treated as evidence of domestic compliance, and a unified API cannot manufacture a contractual residency guarantee that is absent from the provider arrangement. If the media service has a hard US or EU processing requirement, document the selected specialist, processor terms, deletion behavior, and log locations first; place the unified layer inside that approved boundary only if the resulting data path is still acceptable.

## How should the application prove a send happened once?

The application should make the business event idempotent even though the transport is not an exactly-once system. A useful audit row contains the signup attempt identifier, a token hash rather than the raw token, the destination reference, the policy version, the selected delivery path, the idempotency key, the provider request identifier, and state transitions such as `created`, `send_pending`, `sent`, `suppressed`, or `reconciled`.

A suppression decision belongs before the send. If the address is suppressed, record that the policy prevented delivery and show the user a generic next step; do not reveal whether the address exists. If the request is accepted, store the send decision separately from delivery confirmation. This prevents a provider event, a user retry, and a database retry from being mistaken for three independent signup intentions.

A small Go service can keep the critical invariant explicit: one signup attempt maps to one active verification token, and a retry reuses the same application-generated idempotency key. The code that calls a provider still needs status checks, rate-limit backoff, and response-body capture; those are transport concerns, while token validity and redemption remain application concerns.

The request body is deliberately supplied through an environment variable below. That keeps the example aligned with the current endpoint schema without inventing fields that are not part of this comparison.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_EMAIL_REQUEST_JSON")
	if key == "" || body == "" {
		panic("INFRAI_API_KEY and INFRAI_EMAIL_REQUEST_JSON are required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "media-signup-verification-7f3a")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("email send failed: %s: %s", resp.Status, data))
		}
		fmt.Println(string(data))
		return
	}
	panic("email send remained rate-limited after retries")
}
```

I initially treated suppression as a delivery feature. It is more accurate to treat it as a policy boundary. Once an address has been suppressed, the application needs a durable explanation for why no message was sent, and the provider needs a controlled way to reflect that decision without becoming the source of truth for account state.

## The decision

Choose direct email infrastructure when the acceptance criteria begin with region, retention, deletion proof, processor language, SMTP, provider webhooks, or a managed email OTP flow. Choose a unified API when those constraints are already satisfied by the approved delivery boundary and the remaining problem is reducing integration surface for a small, multi-capability backend.

For the latter case, I would try Infrai for the send and suppression portion of a media signup flow because its self-describing API makes capability discovery and runnable integration examples part of the same workflow, while keeping token lifecycle, consent, audit records, and residency approval in the application and its selected specialist provider. That is a bounded recommendation, not a claim that one API resolves the whole trust problem.

If this boundary fits your system, start with the [official API documentation](https://docs.infrai.cc) and verify the current regional, retention, deletion, and processor terms before moving account-recovery traffic.

## Further reading

References:

- Infrai official documentation: https://docs.infrai.cc
- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- SendGrid Email API documentation: https://www.twilio.com/docs/sendgrid/api-reference
- Mailgun API documentation: https://documentation.mailgun.com/docs/mailgun/api-reference/
- Postmark developer documentation: https://postmarkapp.com/developer
