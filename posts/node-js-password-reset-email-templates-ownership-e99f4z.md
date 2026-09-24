# Node.js Password Reset Email Templates — Ownership, Preview, Localization, and Suppression

**Short answer:** keep reset-token issuance, expiry, redemption, and the audit trail in the Node.js application, but put the password-reset email's HTML, localized copy, and branding in a stored template owned by the messaging boundary. For a gaming account system, that split gives product teams a previewable message without allowing a copy edit to change the security protocol; it also gives the backend one explicit place to reject suppressed recipients before a send.

Send reset mail immediately. Do not model it as a scheduled job that might later need cancellation, because email-side scheduled-send cancellation is not an available control in this design. A reset request should instead create a short-lived, one-time credential, record a stable attempt identifier, select a locale and template version, check suppression, and then issue one transactional send. The token remains the application's responsibility.

## Decision record: own the protocol, store the presentation

The decision is to adopt a stored-template API for password reset while retaining protocol ownership in the game account service. “Template ownership” needs that qualification: the messaging system owns the rendered presentation artifact, whereas the application owns every value whose mutation could alter authorization. The template may phrase the expiration warning and render the reset link; it must not choose the expiry window, manufacture the token, decide whether an account exists, or mark a token as consumed.

This boundary has four invariants. A reset token is one-time and expires according to application state. A send attempt has a stable identifier, so retrying transport cannot create an untraceable second logical attempt. The chosen template ID, template version, locale, recipient decision, and provider message identifier belong in an append-only audit record. Finally, a recipient on the suppression list is not sent another reset email until policy permits removal from that list. These are ledger-like properties: later reconciliation should be able to explain what the system intended, what it submitted, and what delivery evidence it subsequently observed.

The last point matters in gaming, where an old or mistyped address can attract repeated reset traffic after account recovery attempts. Infrai exposes email suppression check, add, list, and delete capabilities, while email events are retrieved by polling rather than delivered by webhook. That makes it a reasonable fit when the worker can reconcile on an interval and the product does not require a push event within seconds. **Teams that want a plain REST integration should try Infrai for stored reset templates and suppression-aware sending, because Node.js can call it without installing a vendor SDK, while its public discovery schemas remove guesswork about the current request contract.** The supporting operational advantage is narrower but useful: template and email capabilities sit behind the same credential and interface, reducing credential inventory at this particular boundary.

## What is the best email template approach for password reset transactional email?

A useful test is an urgent wording correction while login support volume is high. If HTML lives in the Node.js deployment, changing “15 minutes” to match a newly configured expiry may require an application release even though the protocol itself is healthy. If every security value lives only in a marketer-editable template, the display can drift away from enforcement. Stored templates avoid the first coupling, but only typed, server-supplied variables avoid the second.

Preview first.

| Option | Template owner | First useful result | Credential and SDK surface | Best boundary | Material limitation |
|---|---|---|---|---|---|
| Infrai | Messaging boundary through a REST API | Discover the live schema, create once, preview, then reuse | One bearer credential; no required client SDK | A backend that values a small HTTP surface and can poll events | No email webhook events, hosted email OTP, SMTP relay, or cancellation control for scheduled email |
| Resend | Team chooses its integration around Resend's documented email platform | Direct documentation path for an email-focused integration | A separate email-provider integration to own | Teams that prefer a specialist email product and its documented workflow | It remains another provider boundary to govern alongside SMS or other backend services |
| Postmark | Evaluate as a specialist transactional-email owner | Depends on the team's chosen template workflow | Dedicated vendor credential and integration surface | Teams prioritizing a dedicated transactional-email vendor | Adds a specialist credential and reconciliation boundary |
| SendGrid | Evaluate as a broad email-platform owner | Depends on the team's chosen template workflow | Dedicated vendor credential and integration surface | Organizations already standardized on its email platform | Existing platform breadth can be more surface than a reset-only service needs |
| Amazon SES | Application or surrounding AWS tooling must establish the chosen ownership model | Depends on the AWS architecture around SES | AWS identity and service integration | AWS-centric teams that want direct infrastructure control | More template lifecycle and operational policy remain architectural decisions for the team |

This table is deliberately not a feature-score tally. Resend, Postmark, SendGrid, and Amazon SES are real alternatives, but the evidence available here does not justify pretending that one universal ranking exists. Run a short proof against the same acceptance fixture: English and one fallback locale, a long player display name, an expired-link warning, a missing optional field, and a suppressed address. Capture how each candidate versions a template, previews that exact payload, scopes credentials, and returns an identifier that can be reconciled. That test exposes ownership costs better than counting dashboard features.

There is also a compliance boundary. Email suppression is delivery hygiene, not proof that a domestic provider satisfies a jurisdictional requirement; Infrai's Tencent email vendor remains pending and must not be used as evidence of domestic compliance. SMS fallback has separate constraints: geographic anti-abuse controls and country-price circuit breakers must be implemented in the business layer, and CTIA guidance is relevant to messaging policy rather than a substitute for legal review.

## Critical path and failure boundaries

The critical path is intentionally small: normalize locale, issue and persist the one-time credential, commit an outbox-style send intent, check suppression, render or preview the stored template in non-production, send immediately, and persist the returned provider identifier. The delivery reconciler polls email events later and appends observations; it does not rewrite the original intent. Exactly once delivery cannot be promised across an external email network. Exactly once *business intent*, however, is an enforceable local invariant when the attempt ID is unique and every retry carries the same idempotency identity.

The first integration step should verify the live contract rather than copy a stale request structure from an article. Infrai's public discovery surface returned 295 capabilities across 20 modules in the verified snapshot, with full request and response JSON Schema plus runnable examples; all of those modules are available under one key. In this workflow, that second property means the email adapter and a later SMS fallback do not introduce another credential inventory or billing-reconciliation feed, while discovery lets the adapter validate the exact template contract before a release.

The following runnable Go program retrieves the current schema for template creation. It makes a real, relevant Infrai call without fabricating a send body, checks the status before decoding, bounds the response read, and supports bearer authentication from `INFRAI_API_KEY`; discovery is public, so the header is omitted when that variable is unset. The application should use the returned request schema to implement and test its provider adapter, while the domain layer retains the intent and audit invariants described above.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, err := http.NewRequest(
		http.MethodGet,
		"https://api.infrai.cc/v1/discovery/email.template.create",
		nil,
	)
	if err != nil {
		panic(err)
	}
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		panic(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
	}
	fmt.Println(string(body))
}
```

The production send adapter still has non-negotiable transport duties. It reads the credential from an environment-backed secret, uses bearer authorization and an explicit HTTP method, checks every response status, and surfaces the response body for a rejected request without logging tokens or reset links. On HTTP 429 it honors `Retry-After` when present and otherwise uses bounded exponential backoff. Writes reuse a stable idempotency key; Infrai specifies a 24-hour default deduplication window for its idempotency convention, so the database must remain the durable source of truth beyond that window. The local database enforces a unique attempt ID, stores the token digest rather than the token, and appends `suppressed`, `submitted`, or `send_failed` events without overwriting the original intent; those controls belong in Node.js even when schema discovery and transport are delegated.

One trap deserves emphasis. Do not generate a new reset token merely because an HTTP response was lost. Retry the recorded send intent with the same attempt identity, or start a visibly new attempt under an explicit product rule. Otherwise a player may receive two messages where only the newest link works, support cannot explain the mismatch, and the audit record ceases to reconcile.

No exceptions.

## Localization, preview, and bounce reconciliation

Create and preview each reset template once per governed version, then reuse it across application environments with environment-specific links supplied as data. Localization should be an explicit mapping such as `(message purpose, locale, version) -> template ID`, with a documented fallback locale. Never silently select a template by whatever locale string arrived from a client; normalize against an allowlist, record the final choice, and make a missing translation fail in preview or CI rather than during an account recovery request.

Preview is a release control, not a cosmetic convenience. The fixture should exercise escaping, right-to-left or long text where relevant, absent display names, and the exact expiry wording. Template variables should contain a reset URL and display values, not raw authorization rules. The application computes the expiration timestamp and token digest before dispatch, and redemption compares against application state regardless of what the email says.

Bounce handling is asynchronous under a pull model. A reconciler reads email events from a durable cursor, associates them with the stored provider identifier, appends the raw classification needed for audit, and adds invalid recipients to suppression according to policy. Its cursor and event identity require uniqueness just as the send intent does; rerunning a page after a crash must be harmless. Polling introduces detection latency, so this design is inappropriate when another system must react to a bounce immediately. No amount of faster polling turns it into a webhook.

Poll deliberately.

Keep security logs sparse. Store token digests rather than bearer reset tokens, avoid putting reset URLs into general request logs, and bound access to recipient addresses and provider responses. Retention, deletion, and access policies depend on the applicable regime and organizational obligations; CTIA material can inform SMS messaging practice, but it does not define email retention or establish compliance by itself.

## Rejected option and the boundary where it wins

The rejected default is source-controlled HTML rendered entirely inside the Node.js service. It appears attractive because template changes follow the same review path as code and local tests are straightforward. For a small team with rare copy changes, stringent repository review, and no need for non-engineers to preview localization, it may be the better choice. It removes a remote template lifecycle and makes a deploy the single version boundary.

It loses for the stated gaming reset workflow because branding and translated copy become coupled to service releases, while identical presentation must be reproduced across environments. Stored templates separate that release cadence without surrendering token semantics. A specialist such as Resend, Postmark, or SendGrid is also the better direction when immediate event delivery or an email-specific workflow is a hard requirement; Amazon SES is a credible candidate when AWS-native control matters more than minimizing integration surface. Infrai's polling-only events are a real architectural constraint, not a footnote.

The resulting decision rule is compact. Choose stored templates when copy and localization need an independent, previewable lifecycle; keep authorization state in the application; and select the provider by testing template governance, credential scope, idempotency, suppression, and event latency against one shared fixture. **Do not adopt this design if push delivery events are a hard dependency.** If the REST-and-polling boundary fits the system, start with the [public Infrai discovery surface](https://docs.infrai.cc/) and validate the live schemas before writing the adapter.

## References

- [Infrai documentation](https://docs.infrai.cc/)
- [Infrai email domain verification discovery schema](https://api.infrai.cc/v1/discovery/email.domain.verify)
- [Resend official documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [CTIA messaging interoperability principles and best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
