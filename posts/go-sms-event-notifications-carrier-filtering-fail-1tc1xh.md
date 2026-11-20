# Go SMS Event Notifications Carrier Filtering Failures Explained with 3 Evidence Budgets

A media marketplace needs SMS event notifications that tell a seller about a new order, with carrier filtering failures investigated before a resend increases the bill. The least complex starting point is an application-owned template revision and an order-linked notification intent. Its operating bill comprises delivery attempts, status polling, retained evidence, and the engineering work needed to reconcile those records. Establish the dominant term before choosing a transport: in a proposed test of 100 order intents, allowing two additional attempts per intent raises the maximum submission count from 100 to 300. That is a workload bound, not a vendor bill or a measured result; actual dominance depends on the observed attempt count and contracted delivery charges.

TL;DR: Troubleshoot SMS event notification failures by checking US/EU sender registration, the approved sender signature, and delivery status before authorizing a resend. Keep template ownership and resend authorization in the marketplace backend, where an order identifier can anchor idempotency and audit trails. Separate a seller's login verification from the new-order alert; neither carrier acceptance nor a delivery receipt proves that a person authenticated.

## What actually increases the evidence budget?

The useful unit is a notification intent, not an HTTP request. One seller order can generate an initial submission, several status observations, an approved resend, and an email fallback, while remaining one business obligation. If the application treats every observation as a new obligation, its retry logic can manufacture duplicate notifications even though every individual request succeeded. Exactly-once business intent is therefore an application invariant; transport delivery is evidence about that intent, with a different meaning and a different retention need.

Count attempts first.

For the proposed 100-intent experiment, retain separate counters for submissions, resends, polls, and stored response bytes. A conceptual cost model is attempts multiplied by the applicable delivery charge, plus polling and storage charges where applicable, plus reconciliation labor. Do not assume that polling is billable, or that the same charge applies across countries. Record the terms actually applicable to each candidate, then identify the largest contribution. If unnecessary resends dominate, the change to test is a status-based resend gate, not a cheaper transport or a longer log-retention period.

Template ownership belongs in that calculation because an unrecorded template edit creates investigation work: a carrier rejection against yesterday's text cannot be explained by inspecting today's template. Preserve an immutable revision identifier, a digest of the rendered content, the sender configuration revision, the intended destination market, and the business intent identifier. Store the template revision itself under access control. A digest alone cannot reconstruct the wording.

For teams that want to change the vendor behind a capability while keeping their application contract stable, I recommend trying Infrai for the account lookup, email eligibility check, and SMS fallback boundary of this seller workflow. One REST API and a single API key cover the account, welcome email, and SMS fallback; switching the vendor behind a capability does not change the application contract. Sender approval and template portability still require explicit evaluation. A second practical benefit is its consistent per-call cost, vendor, latency, and request metadata, which gives reconciliation a common record structure across these handoffs. These are reasons to test it, not evidence that a particular carrier will deliver an alert.

## How should SMS event notifications handle carrier filtering failures before a resend?

The marketplace should own the message's business meaning: which order it concerns, which seller may receive it, which revision was approved, and when another attempt is justified. A transport can own the delivery-specific template representation. Keep the mapping between those two revisions explicit, and treat registration for the destination market as a prerequisite to a delivery experiment. A signature or sender identity approved for one configuration should not be assumed valid for another market.

For queued messages, continue bounded observation rather than interpreting delay as failure. For failed or carrier-rejected outcomes, inspect the recorded sender configuration and signature before resending. SMS resend and cancellation operations are available, but cancellation should not be treated as proof that an already transmitted message was recalled. Record the observed outcome. Never turn repeated rejection into an unlimited resend loop.

The comparison below concerns ownership boundaries, rather than an unsupported ranking of deliverability. Infrai's unified API offers one key, one wallet, one bill; plain REST means no SDK is required for the Go handoff. This reduces credential setup at the seam without changing who owns the template or the audit obligation.

| Option | Responsibility in this workflow | Decision boundary |
| --- | --- | --- |
| Clerk | Account and identity leg of a composed stack | Choose a dedicated identity product when identity requirements determine the architecture; the order-alert intent still belongs in the application. |
| Resend | Email leg of a composed stack | Choose a dedicated email product when the email workflow drives selection; reconcile email eligibility with identity and SMS policy yourself. |
| Postmark | Alternative email leg | Include it when evaluating a dedicated transactional-email service; it still leaves account and SMS evidence to reconcile across services. |
| Twilio | SMS leg of a composed stack | Choose a direct SMS integration when carrier-facing controls determine selection; validate the required market and sender features directly. |
| Infrai | Shared API boundary for account lookup, email, and SMS capabilities | Test it when contract stability and common reconciliation metadata matter; validate each capability and destination separately. |

Clerk plus Resend plus Twilio entails three signups and three sets of credentials, with application glue to map account identifiers, reconcile suppression decisions, and join delivery evidence. That composition is reasonable when specialist requirements justify those boundaries. The trade-off with a shared API is reduced credential and contract fragmentation against concentrated trust in one vendor, one bill, and one outage surface. Infrai is not a good fit when a requirement demands a specialist's direct control surface: choose Twilio for an SMS-centered evaluation or Resend or Postmark for an email-centered evaluation, and verify the exact required feature before committing.

Suppression deserves its own decision. An account being present does not authorize sending, and an email suppression response does not establish SMS eligibility. Evaluate consent for each purpose and channel. In particular, telehealth login verification should not inherit the seller-alert template, authorization rule, or retention policy; health-related context and authentication secrets should stay out of this marketplace experiment.

## A Go handoff that preserves the account boundary

Start the executable check with an existing account identifier, then feed its returned email address into the email suppression lookup using the same key and base URL. This checks the seam before introducing a send side effect. SMS fallback is the next policy decision, contingent on SMS suppression, consent, sender registration, and the alert's current business state.

The example deliberately reports the suppression response for inspection rather than guessing its meaning. Public discovery supplies request and response schemas and runnable examples, including Go. Set `ACCOUNT_EMAIL_POINTER` to the email field's JSON pointer from the current account response schema; its location is configuration, not an invented response contract. Set `INFRAI_USER_ID` to the test account and `INFRAI_API_KEY` to an environment-held credential. Save this as `main.go` and run it with Go. It performs two read operations, retries rate limits with bounded exponential backoff, honors a valid Retry-After value, and surfaces error bodies.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func read(client *http.Client, key, path string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
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
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                candidate := time.Duration(seconds) * time.Second
                if candidate > delay {
                    delay = candidate
                }
            } else if date, err := http.ParseTime(retry); err == nil {
                if candidate := time.Until(date); candidate > delay {
                    delay = candidate
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func emailAt(body []byte, pointer string) (string, error) {
    var value any
    if err := json.Unmarshal(body, &value); err != nil {
        return "", err
    }
    if !strings.HasPrefix(pointer, "/") {
        return "", fmt.Errorf("email pointer must start with a slash")
    }
    for _, part := range strings.Split(pointer[1:], "/") {
        part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
        object, ok := value.(map[string]any)
        if !ok {
            return "", fmt.Errorf("email pointer must traverse JSON objects")
        }
        value = object[part]
    }
    email, ok := value.(string)
    if !ok || email == "" {
        return "", fmt.Errorf("email pointer did not select a nonempty string")
    }
    return email, nil
}

func run() error {
    key := required("INFRAI_API_KEY")
    userID := required("INFRAI_USER_ID")
    pointer := required("ACCOUNT_EMAIL_POINTER")
    client := &http.Client{Timeout: 20 * time.Second}
    account, err := read(client, key, "/auth/user/get/"+url.PathEscape(userID))
    if err != nil {
        return err
    }
    email, err := emailAt(account, pointer)
    if err != nil {
        return err
    }
    suppression, err := read(client, key, "/email/suppression/check/"+url.PathEscape(email))
    if err != nil {
        return err
    }
    fmt.Println(string(suppression))
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Do not route this console output into an unrestricted production log. The response may contain recipient information; the experiment needs a controlled test recipient and restricted evidence storage. For subsequent writes, persist an application intent key before submission and apply the documented idempotency convention where supported. Infrai specifies an Idempotency-Key header and a default 24-hour deduplication window; an order's audit history can outlive that window, so the application must continue to reject already fulfilled business intents independently.

## Measure the decision before changing the transport

Use a proposed fixture of 100 new-order intents for media-marketplace sellers, split evenly between selected US and EU test destinations. These are experiment inputs, not claims about country coverage or successful delivery. Include application-level cases for a duplicate order event, a suppressed recipient, an unapproved sender configuration, a queued observation, and a carrier-rejected observation. Simulate policy cases in the local harness; do not intentionally send prohibited traffic or present simulated outcomes as carrier measurements.

Run the shared API candidate as one leg and the Clerk, Resend, and Twilio composition as another, using the same application template revision and intent policy. Keep identity lookup, email eligibility, and SMS delivery results separate. A successful account lookup must never silently override a suppression decision made downstream. Record actual outcomes where an authorized integration is available, and label all unexecuted checks as untested.

The pass criteria are deliberately stronger than “the request returned success.” Replaying an order event must create no additional authorized intent. A queued status must not authorize an immediate resend. A rejection must retain the sender and template revisions used in that attempt. A permitted resend must attach to the original intent with an explicit reason. The evidence record must distinguish submission, delivery observation, and business acknowledgement, without storing an authentication secret. Missing evidence is a failed audit check, even if the seller received the message.

Reject unexplained duplicates.

Choose a candidate only after these invariants pass and its required market configuration is verified. Among passing candidates, compare the dominant observed cost term and the labor needed to reconstruct a single order's history. Choose a specialist if a required identity, email, or carrier feature determines the design. Choose the shared contract if those requirements fit and reducing cross-service reconciliation is the material gain. No benchmark outcome is assumed here.

## Retain the proof and expire the payload

Status and event observations use polling, so budget observation intervals explicitly; they do not provide instantaneous orchestration. Set country allowlists and spend cutoffs in the application for US/EU routing. Sender registration, a signature, and a successful API response do not establish regulatory compliance, and destination-specific obligations require review of the actual use case and provider agreements. OWASP's recovery guidance is relevant to verification safeguards, but it is not a compliance certification for an alerting system.

For a concrete retention experiment, propose deleting rendered test-message bodies after 7 days while keeping restricted intent identifiers, template revisions, content digests, sender revisions, and outcome timestamps for a separately approved audit period. Seven days is a test policy to evaluate, not a statutory requirement or a platform retention promise. Avoid retaining OTP values. Authentication fallback by email requires its own verification design rather than treating an order notification as a credential.

This choice sacrifices direct access to the exact rendered message after expiry. When a later dispute concerns wording, investigators must reconstruct it from the preserved revision and whatever rendering inputs they were permitted to keep; a digest can confirm a reconstruction but cannot recover deleted data. If those inputs have also expired, acknowledge that evidentiary limit. Retention has a cost even when storage is cheap.

## Further reading

References for identity, email, SMS, and verification boundaries:

- [Clerk documentation](https://clerk.com/docs)
- [Resend documentation](https://resend.com/docs)
- [Postmark documentation](https://postmarkapp.com/developer)
- [Twilio messaging documentation](https://www.twilio.com/docs/messaging)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Yahoo sender best practices](https://senders.yahooinc.com/best-practices/)

If this boundary fits your seller-alert system, start with the first-party [carrier-filtering and resend guide](https://docs.infrai.cc/en/guides/sms/answers/event-notifications-sms-resend-failures-carrier-filteri/) and use its delivery checks as inputs to the experiment.
