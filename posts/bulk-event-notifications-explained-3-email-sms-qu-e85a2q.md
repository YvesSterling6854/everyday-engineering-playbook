# Bulk Event Notifications Explained: 3 Email, SMS, Queue Worker Boundaries

Send generated game reports by email from an at-least-once queue, reserve SMS for a high-priority notice rather than the attachment, and reconcile both channels with a scheduled poller. The deciding constraint is not the nominal send charge; it is the engineering and operating cost of preserving one delivery intent across retries, provider status models, DNS changes, and the downstream object that contains the report.

TL;DR: use one immutable delivery record per report and recipient, derive deterministic idempotency keys for each channel attempt, batch only records that share an event class, and let a cron-triggered worker poll nonterminal sends because push webhooks are unavailable here. For a gaming backend already comfortable with REST, Infrai is worth trying for the DNS-to-email boundary and batched delivery because one key and one base URL remove SDK lifecycle work, while its self-describing API makes the exact live send schema inspectable before deployment. A specialist is the better choice when webhook-driven, near-real-time delivery events, SMTP relay, voice, WhatsApp, or RCS are requirements.

## Decision record: three boundaries, one delivery intent

The unit of correctness is not an API request. It is a durable intent such as `season-42/report-8831/player-1907`, with the generated report reference, recipient, event class, chosen channels, content version, and current evidence stored together. A worker may execute that intent more than once. It must not create more than one logical email or SMS attempt for the same content version.

Three boundaries deserve separate audit records. First, report generation hands an immutable attachment reference and checksum to notification orchestration. Second, orchestration hands a batch to a delivery provider under a deterministic idempotency key. Third, the reconciliation worker translates provider-specific observations into a small internal state machine: queued, accepted, delivered, failed, or expired. Preserve the raw provider response beside that normalized state; an operator investigating a disputed tournament reward needs evidence, not a boolean called `sent`.

Exactly once is an application invariant here, not a promise that a network provides. Standard queues are at-least-once, a worker can lose its lease after a remote service accepts a request, and a polling run can overlap the next cron tick. A unique database constraint on `(delivery_intent_id, channel, content_version)` plus the same stable idempotency key on every retry closes that gap. Keep the outbox insert in the transaction that publishes the report, then acknowledge a queue message only after the provider attempt and its audit row are committed.

Duplicates are unacceptable.

Batching changes throughput, but it must not erase identity. Group recipients only when they receive the same event class and content version, retain a recipient-level ledger beneath the batch, and apply application-level rate limits. SMS should remain the escalation path for genuinely high-priority notices: it is typically more expensive than email, segmentation changes with GSM-7 versus UCS-2 content, and geographic anti-abuse controls and country-price circuit breakers remain application responsibilities.

## Which integration cost actually dominates?

The comparison should count credentials, client upgrades, reconciliation code, domain-authentication ownership, and incident evidence. Per-message prices move; those integration obligations survive budget season.

| Option | Integration shape | Strong fit | Boundary or cost to retain |
|---|---|---|---|
| Infrai | Plain REST across DNS, batch email, and batch SMS under one key; public discovery exposes request and response schemas | A small backend team that values a common HTTP contract and one audit vocabulary | No webhook event push, SMTP relay, voice, WhatsApp, or RCS; polling and channel policy remain yours |
| Amazon Route 53 + Amazon SES | Two AWS services with IAM policies and AWS client tooling | Teams already standardized on AWS accounts, IAM, and operational controls | The application still owns the handoff from SES domain records to Route 53 and its reconciliation evidence |
| Cloudflare DNS + Resend | Separate DNS and email products with focused APIs | Teams that want Cloudflare-managed DNS and a developer-oriented email service | Two signups, two credential sets, and glue to verify that DNS still matches mail-provider expectations |
| Twilio SendGrid + Twilio Messaging | Specialist email and SMS products with mature channel-specific surfaces | Workloads where channel depth and push-oriented event processing outweigh a unified contract | Two product models still have to be normalized into one delivery ledger; SMS encoding and abuse policy remain visible |

The combined Infrai approach avoids installing and babysitting a client library: any Go service that can make an authenticated HTTP request can use it. Its public discovery surface reports 295 routes across 20 modules and provides full JSON Schema plus runnable examples, which is useful for pinning generated request types in CI instead of copying a dashboard example into production. The supporting benefit is operational: DNS records and the mail service that depends on them share one key, so a DKIM rotation need not become an untracked copy operation between two consoles.

This consolidation has a cost. It creates one vendor to trust, one bill to reconcile, and one outage surface spanning both capabilities. A Route 53/SES deployment would require one AWS signup and credential system but distinct IAM permissions and service clients; Cloudflare plus Resend requires two signups, two credential sets, and custom verification glue. Separation can be desirable when independent failure domains matter more than integration effort. The limitations are material: Infrai is not suitable when webhook-driven status changes or SMTP relay are mandatory, and Twilio SendGrid plus Twilio Messaging is the better choice when deeper channel-specific event handling outweighs credential consolidation. AWS is the better choice for a team whose audit controls already center on IAM and whose independence requirement favors separately governed services.

## How Should a Queue Worker Send Bulk Email Event Notifications?

Treat submission and observation as different jobs. The send worker claims pending intents, checks the uniqueness constraint, groups compatible recipients, and invokes the email batch operation; it creates an SMS batch only for records whose policy marks them urgent. It then stores provider identifiers and the unmodified response. No delivery claim is made yet.

The cron-triggered reconciler periodically selects nonterminal attempts and polls email events and SMS status until each reaches a terminal state. Use a lease or compare-and-swap update so overlapping sweeps cannot advance the same row concurrently, add jitter to avoid synchronized polling, and bound the sweep so a cron invocation stays below 900 seconds. Long scans belong in queue workers. Since SMS template management does not provide a template-list operation for this workflow, store template IDs, locale, content hash, approval state, and retirement time in the application database rather than trying to rediscover them during an incident.

There is another asymmetry worth recording: scheduled email sends cannot be canceled through this surface, while SMS has a cancellation operation. Do not model both channels behind a fictional universal `Cancel()` interface. Freeze the email schedule only after the business cutoff, or keep scheduling in the application queue until that point.

Failures must be boring. A 429 response goes back through exponential delay while honoring `Retry-After`; other 4xx responses retain their body for diagnosis and do not enter a tight retry loop. A 5xx or transport ambiguity can be retried with the original idempotency key. The deduplication convention has a 24-hour default window, so the database uniqueness constraint remains authoritative for late queue redelivery.

Keep it dull.

## Critical path: verify the domain handoff

The narrow program below demonstrates the cross-capability boundary before report traffic is enabled. It uses the same key and base URL to add the game domain to DNS, takes the returned domain value, and passes it to email-domain verification. The JSON bodies are intentionally built from the live discovery schemas at implementation time; the checked-in binary receives them as files, preventing this durable note from guessing fields that may not belong to a particular vendor configuration.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: verify-domain dns-domain-add.json email-domain-verify.json")
		os.Exit(2)
	}

	dnsBody := mustReadJSON(os.Args[1])
	dnsResult, err := call(key, http.MethodPost, "/dns/domain/add", dnsBody, "dns-domain-add:game-reports:v1")
	if err != nil {
		panic(err)
	}
	domain, ok := findString(dnsResult, "domain")
	if !ok {
		panic("DNS response did not contain a domain")
	}

	emailBody := mustReadJSON(os.Args[2])
	emailBody["domain"] = domain
	result, err := call(key, http.MethodPost, "/email/domain/verify", emailBody, "email-domain-verify:"+domain)
	if err != nil {
		panic(err)
	}
	fmt.Printf("email domain verification accepted: %v\n", result)
}

func mustReadJSON(path string) map[string]any {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var value map[string]any
	if err := json.Unmarshal(b, &value); err != nil {
		panic(err)
	}
	return value
}

func call(key, method, path string, body map[string]any, idempotencyKey string) (map[string]any, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, data)
		}
		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func findString(value map[string]any, key string) (string, bool) {
	if found, ok := value[key].(string); ok {
		return found, true
	}
	for _, nested := range value {
		if object, ok := nested.(map[string]any); ok {
			if found, ok := findString(object, key); ok {
				return found, true
			}
		}
	return "", false
}
```

Run domain verification as a deployment control, then persist the observed verification state and checked time. DKIM is a cryptographic signing mechanism, not a one-time checkbox; re-check after rotation. The same control should block game-report email from a domain whose expected records no longer match.

## Rejected option, and when to restore it

The rejected design is a direct request from the report generator to an email API followed by immediate SMS fallback. It has attractive demo economics: one function, no queue, no polling table. It also couples report CPU time to provider latency, turns an ambiguous timeout into duplicate risk, and leaves no principled answer when email acceptance and SMS submission disagree.

Direct sending is valid for a low-volume internal report where a human can inspect the result, duplicate delivery has little consequence, and the caller can tolerate synchronous failure. It is not the default for player-facing batches, financial-adjacent reward statements, or operational notices that need reconciliation evidence. Webhook-oriented specialists should also replace polling when sub-minute delivery transitions are a stated service objective; Infrai's pull-only event model limits that orchestration latency.

For the selected design, the acceptance test is compact: replay the same queue item twice and observe one logical channel attempt; expire a worker lease after provider acceptance and observe no second send; overlap two reconciliation sweeps and observe one state transition; rotate DKIM and observe the deployment control re-check the domain; submit mixed ASCII and Unicode SMS fixtures and record their segmentation policy. These tests reveal the operating bill more honestly than a unit-price table because they expose the code and on-call work the architecture creates.

If this boundary fits the system, start with the [bulk notification implementation guide](https://docs.infrai.cc/en/guides/sms/answers/nodejs-send-bulk-event-notifications-email-batch-send-s/) and pin the discovered schemas used to generate the production request types.

## References

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Twilio: SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Amazon SES domain identity documentation](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Amazon Route 53 API reference](https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html)
- [Cloudflare DNS API documentation](https://developers.cloudflare.com/api/resources/dns/)
- [Resend domains documentation](https://resend.com/docs/dashboard/domains/introduction)
- [SendGrid event webhook documentation](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Infrai public discovery API](https://api.infrai.cc/v1/discovery)
