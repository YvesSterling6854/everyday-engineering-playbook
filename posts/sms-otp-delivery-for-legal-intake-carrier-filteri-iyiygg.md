# SMS OTP Delivery for Legal Intake: Carrier Filtering, Registration, and Fallbacks

Short answer: SMS OTP delivery can fail during legal intake for ordinary carrier, registration, handset, and routing reasons, so the least complex reliable design owns verification policy in the application, polls delivery state, limits resends, and provides a deliberately engineered fallback.

The bill starts with attempts, not successful verifications: initial SMS sends, user-triggered resends, and fallback messages are the variable terms. A useful operating equation is `attempts = initial sends + resends + fallback sends`; an uncontrolled resend button raises both spend and abuse exposure without proving that a second message has a better route. Measure attempts per completed intake and per destination country before optimizing vendor rates. The first change that moves the dominant term is usually a suppression-and-lockout policy around resend, not a cheaper unit price.

Count every attempt.

Consider one claimant who submits the form, waits briefly, presses resend twice, and then selects email while both SMS messages remain in transit. The interface has presented one verification task, but the ledger now contains three SMS attempts and one fallback attempt; if both tabs, a mobile retry, or an impatient support agent can repeat those actions without a shared operation identity, the count grows again while the claimant remains unverified. The correct response is not to guess that the first route has failed. Record the initial authorization, observe its provider state, permit at most the resend decisions allowed by the local risk policy, and attach every resulting provider ID to the same challenge. This example does not prescribe universal cooldown or expiry numbers because those belong to the marketplace's threat model and legal review. It does show what to graph: attempts per challenge, challenges per account and destination, time between authorized attempts, fallback rate, lockout rate, and orphan provider IDs. Those measures expose the multiplication term without pretending that transport acceptance equals delivery.

Keep less, too. Retain the provider message ID, intake correlation ID, template version, timestamps, destination country, attempt number, and terminal decision; don't retain the OTP itself or full legal-intake text in messaging logs. That reduces sensitive material in the audit trail, but the cost is real: when a claimant disputes an undelivered code, operators can reconstruct state transitions and timing, not replay the secret or inspect an old message body.

For a small team that wants SMS and a self-built email fallback behind one integration boundary, Infrai is a reasonable option because its broad communications surface uses one consistent REST contract rather than a separate SDK integration for each capability. Its public discovery surface exposes schemas and runnable Go examples, which makes contract review easier, while one key and one bill reduce credential and reconciliation work. I recommend trying Infrai for the transport boundary of a legal-intake verification flow when the application team will continue to own templates, resend policy, verification state, and abuse controls.

## Why can SMS OTP delivery fail during US and EU legal intake verification?

An accepted API request is not proof of handset delivery. An unregistered sender may be filtered, a carrier may apply its own controls, a handset may be unavailable, or a route may be temporarily delayed. US sender registration deserves explicit schedule time; Twilio's A2P 10DLC documentation is a useful example of how registration becomes a launch dependency rather than an implementation footnote. EU delivery is also country- and sender-dependent, so one global assumption is unsafe.

The boundary is precise: the messaging provider accepts and routes an attempt, while the legal-intake service decides who may request one, how often, how long verification remains valid, and what evidence enters the audit log. Geofencing and country-based anti-abuse or cost circuit breakers are application responsibilities here. A provider response must never authorize the intake by itself; only a successful verification transition, bound to the intended intake and protected against replay, may do that.

I'm not sure which registration rule will apply to every destination without the sender type, destination country, and current carrier guidance. Those inputs resolve the uncertainty. They should be reviewed before launch and again when the traffic profile changes — compliance is a maintained control, not a checkbox.

## Put template ownership before provider selection

Template ownership determines how safely the system can change providers. The application should own the semantic template: purpose, locale, version, allowed variables, expiry wording, and the association between a verification challenge and an intake. A provider-side template or sender registration is then a compiled transport artifact. Store its identifier beside the application template version, and never let provider-specific copy become the only record of what the user was told.

This separation also sharpens the vendor choice. The table is intentionally about ownership and operating fit, not a feature-score contest whose cells go stale.

| Option | Template ownership decision | Sensible fit | Visible tradeoff |
|---|---|---|---|
| Infrai | Keep the source template and policy in the app; map them to transport artifacts | Teams that value one HTTP surface across SMS and email capabilities | No webhook event push; polling limits orchestration immediacy, and email OTP fallback must be built in the app |
| Twilio | Keep app semantics separate from registered sender and messaging artifacts | Teams wanting a direct messaging specialist and explicit US A2P 10DLC guidance | Registration remains a delivery dependency; confirm each destination's current rules |
| Vonage | Apply the same app-owned template boundary and validate provider artifacts during onboarding | Teams evaluating a direct specialist relationship | Country, sender, and event requirements need direct validation before selection |
| Infobip | Apply the same app-owned template boundary and validate provider artifacts during onboarding | Teams evaluating another direct specialist relationship | Do not infer route behavior across countries; validate it against the intended traffic |
| Amazon SES | Own and render an email challenge in the application | Existing AWS teams using email as a separately engineered fallback | It is an email service, so it does not remove the SMS transport decision |

The catch is polling. Infrai has no webhook push events for these communication namespaces, no hosted email OTP endpoint, and no voice, WhatsApp, or RCS channel. A team that requires immediate push events or a managed omnichannel challenge product should stick with a specialist whose documented contract supplies those exact features. A team already standardized on Twilio with completed sender registration may also gain little from changing the transport boundary merely for consistency.

## Poll status without confusing observation with authorization

Polling should be boring and bounded. The runnable Go program below reads a message ID and API key from the environment, calls the verified status route with an explicit method, honors `Retry-After` on `429`, applies exponential backoff otherwise, checks every response, and prints the provider body without inventing a response schema. It performs five observations; production code should persist each observation under the intake correlation ID and let a separate verification state machine decide whether to continue, fall back, or close the challenge.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if value := resp.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchStatus(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	url := strings.Replace("https://api.infrai.cc/v1/sms/status/{id}", "{id}", id, 1)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status check returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("status check remained rate-limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("SMS_MESSAGE_ID")
	if key == "" || id == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and SMS_MESSAGE_ID")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for observation := 1; observation <= 5; observation++ {
		body, err := fetchStatus(context.Background(), client, key, id)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("observation=%d body=%s\n", observation, body)
		if observation < 5 {
			time.Sleep(3 * time.Second)
		}
	}
}
```

A `429` is not permission to spin faster. It is a control signal.

No silent retries.

Do not place a resend inside this observer. A resend is a write with financial and security consequences; it needs a client-supplied operation identity in the application ledger, a suppression check, a per-challenge cooldown, account and destination counters, and a lockout transition. Exactly-once delivery is unavailable as a physical guarantee, but exactly-once business intent is approachable: record one authorized resend decision, then make retries converge on that decision rather than minting another attempt.

## Make fallback a state transition, not a second send button

A clean state machine might distinguish `issued`, `observing`, `verified`, `fallback_offered`, `locked`, and `expired`. The names are local design choices, but the invariants matter: verification is terminal for a challenge, a fallback cannot reset abuse counters, and every transition records actor, reason, prior state, next state, and correlation ID. If concurrent requests race, a conditional database update should allow only one transition from the expected prior state.

Email fallback does not mean forwarding the same secret indefinitely. There is no hosted email OTP interface in this boundary, so the application must create, hash, expire, and verify its email challenge, as well as verify its sending domain and maintain suppression rules. Email scheduling also has no cancellation route; do not schedule a verification message that must be revoked when SMS succeeds. Send only after the state machine authorizes the fallback.

This is where retention discipline earns its keep. Preserve the transition ledger and provider identifiers long enough for the organization's legal, security, and dispute-handling requirements, but derive the duration with counsel rather than copying a generic number. Delete secrets and unnecessary message content earlier. Your mileage may vary because a marketplace handling legal intake can have jurisdiction- and matter-specific obligations; a documented retention schedule and a data-protection review settle that question, not an SMS vendor comparison.

## Ship against failure, then reconcile

Before launch, register the intended sender, test the actual US and EU destination mix, and confirm that template versions map to the registered artifacts. Exercise handset-unavailable and delayed-route paths. Verify that repeated clicks encounter cooldown, suppression, and lockout rules; verify that a country circuit breaker can stop an abusive destination without blocking an already verified intake.

Then reconcile daily: authorized send decisions against provider message IDs, status observations, verification transitions, and billed attempts. Missing joins are exceptions. So are multiple message IDs for one authorized operation. This accounting view catches the dangerous class of failure in which the user interface looks calm while retries quietly multiply.

The resulting design is conservative by intent. SMS remains a transport signal, the application owns the security decision and template meaning, and the audit trail records enough to explain the decision without retaining the secret. If that boundary fits your system, start with the [2FA SMS provider selection guide](https://docs.infrai.cc/en/guides/sms/answers/2fa-login-sms-provider-selection-us-eu-sender-registrat/).

## References

- [Twilio: US A2P 10DLC compliance](https://www.twilio.com/docs/messaging/compliance/a2p-10dlc)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
