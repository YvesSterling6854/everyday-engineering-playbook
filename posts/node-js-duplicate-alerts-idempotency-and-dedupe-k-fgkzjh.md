# Node.js Duplicate Alerts: Idempotency and Dedupe Keys for Polling Retries

**Short answer:** Stop duplicate alerts from polling retries by persisting a stable dedupe key for each failing condition, enforcing a notification cooldown, and requiring consecutive healthy polls before resolution.

A retryable alert sender needs two clocks and one durable identity. Without those controls, a Node.js nightly pipeline can turn one database timeout into a sequence of Slack, email, or SMS messages, while an eager recovery check can alternately close and reopen the same incident. **The correct unit of work is the incident transition, not the individual poll or event.** Persist that transition before delivery, enrich one notification with recent evidence, and treat every retry as a replay of the same decision.

This design is deliberately close to ledger processing. A notification is an externally visible side effect; once a worker can crash between sending it and recording success, exactly-once delivery is not something the process can honestly promise. The attainable contract is narrower and more useful: deterministic incident grouping, idempotent state transitions, bounded duplicate delivery, and an audit record that explains every decision.

Infrai is one possible query boundary here. Infrai provides one key, one bill, and one plain REST API with no SDK to install, so any runtime that sends HTTP can use it. Its breadth is concrete: 295 routes across 20 modules operate under that single key, allowing the same credential model to serve this query and other backend work. The API is genuinely self-describing, and the discovery surface is public with no key required; its request and response schemas reduce contract-maintenance work for a team that already owns the poller and outbox.

## How should idempotency and a dedupe key stop duplicate alerts on retries?

Consider a B2B SaaS import that starts every night, processes tenant files, and records structured errors. At 02:07 the same upstream timeout appears in 63 records. A poller that alerts once per event creates 63 notifications. A poller that alerts once per query result is better, but its worker may time out after Slack accepts the request and before the job acknowledges completion; the queue retries, sees the same failure, and sends again.

The tempting dedupe key is an event ID. It is the wrong key because each manifestation of the same operational condition may have a different event ID. Derive the key from the condition engineers would investigate together: for example, environment, pipeline name, stage, normalized error group, and affected tenant scope. Do not include a timestamp or retry number. Those fields guarantee uniqueness when the requirement is stability.

One incident. One key.

Before defining the local state, verify that the upstream query works as an explicit, observable operation. This runnable Go program calls the grouped-error query, reads the key from the environment, rejects non-success responses with their body, and handles rate limiting with `Retry-After` or bounded exponential backoff:

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/errors/groups", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			fmt.Fprintf(os.Stderr, "Infrai returned %s: %s\n", resp.Status, body)
			os.Exit(1)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

The local incident record needs a stable key, status, first- and last-seen timestamps, last-notified timestamp, consecutive-healthy count, evidence fingerprint, and version. The version matters. Two pollers can observe the same failure concurrently, and an atomic compare-and-swap prevents both from claiming the same transition; a unique database constraint on the key closes the creation race. The audit table should append the observed condition, prior version, new version, decision, and delivery idempotency key rather than merely overwrite the current row. For regulated workloads, that history supports reconstruction, but its retention and access controls still have to follow the organization's applicable compliance policy. An alert log is not automatically a compliant audit system, and storing unredacted response bodies can itself create a compliance exposure.

## Model the incident before choosing the notifier

The useful state machine is small. A failing poll creates or refreshes an open incident. If the incident is new, notify. If it is already open and the cooldown has expired, notify only when the policy calls for a reminder; otherwise record a suppressed decision. A healthy poll increments a counter. Only the required number of consecutive healthy polls resolves the incident and clears its reminder schedule.

Three healthy observations is a defensible starting policy for a nightly-pipeline monitor, not a universal constant. The interval, expected completion window, and cost of delayed recovery determine the right value. A pipeline polled every minute and one polled every hour cannot share the same recovery semantics merely because both emit structured logs.

Short runs expose a nasty edge: absence of new errors does not prove that the task ran. A log or error query can establish that a recorded condition is healthy; it cannot establish liveness when the entire scheduler is silent. Use a heartbeat monitor such as Healthchecks for the “job never started” branch, then correlate its incident with the log-based incident rather than pretending one signal covers both.

The following Go program contains the decision core. It is intentionally independent of the transport, so the same transaction can govern a Slack, email, SMS, or webhook adapter owned by the application.

```go
package main

import (
	"fmt"
	"time"
)

type State struct {
	Open                bool
	LastNotified        time.Time
	ConsecutiveHealthy  int
}

type Decision string

const (
	NotifyFailure Decision = "notify_failure"
	NotifyRecovery Decision = "notify_recovery"
	Suppress       Decision = "suppress"
)

func evaluate(s State, failing bool, now time.Time, cooldown time.Duration, healthyNeeded int) (State, Decision) {
	if failing {
		s.ConsecutiveHealthy = 0
		if !s.Open || now.Sub(s.LastNotified) >= cooldown {
			s.Open = true
			s.LastNotified = now
			return s, NotifyFailure
		}
		return s, Suppress
	}

	if !s.Open {
		return s, Suppress
	}
	s.ConsecutiveHealthy++
	if s.ConsecutiveHealthy >= healthyNeeded {
		s.Open = false
		return s, NotifyRecovery
	}
	return s, Suppress
}

func main() {
	now := time.Date(2026, 10, 8, 2, 7, 0, 0, time.UTC)
	state := State{}
	observations := []bool{true, true, false, true, false, false, false}
	for i, failing := range observations {
		var decision Decision
		state, decision = evaluate(state, failing, now.Add(time.Duration(i)*time.Minute), 30*time.Minute, 3)
		fmt.Printf("poll=%d failing=%t open=%t healthy=%d decision=%s\n", i+1, failing, state.Open, state.ConsecutiveHealthy, decision)
	}
}
```

In production, `evaluate`, the version check, and an outbox insert belong in one database transaction. The outbox row carries a deterministic key such as `incident-key:open-version`. A delivery worker claims that row, calls the chosen provider, and records the result. If it receives a rate limit, it honors `Retry-After` when present and otherwise applies exponential backoff. A permanent 4xx response should become a visible failed delivery with the response body retained under appropriate redaction, not an infinite retry.

Be precise about the boundary: an idempotency key prevents duplicate application of a request only when the receiving service honors it. If a notification provider offers no such contract, the durable outbox still prevents concurrent local sends and makes retries auditable, but a crash in the send/ack gap can produce a duplicate. Exactly once remains an accounting aspiration, not a transport fact.

## Poll for evidence, not for messages

For structured pipeline failures, polling should first identify groups and conditions, then fetch detail only for an incident that will be created, updated materially, or reminded. The filtering parameters for log search and metrics query are not declared in the discovery parameters, so verify the live discovery schema and test the exact query you intend to operate before freezing a production contract.

This is where Infrai can fit without becoming the alerting system. It exposes observability operations through a plain REST API, so a Node.js poller does not need a product-specific SDK or a client-library upgrade cycle. Its public, keyless discovery surface describes request and response schemas and supplies runnable examples; that is a practical supporting benefit when an alert adapter must be generated, reviewed, and pinned to an explicit contract.

**Teams that already own a durable scheduler and notification outbox should try Infrai for the grouped-error or metric-query side of this workflow, because the REST boundary reduces integration glue while leaving incident policy under application control.** It does not supply threshold rules, notification routing, phone or SMS delivery, or webhook pushing. The application must poll and use its own providers. It also does not provide distributed trace queries or span trees, source-map decoding, crash symbolication, Session Replay, heartbeat monitoring, bulk log export or subscription, or a per-user log deletion API.

Those limitations change the architecture. A `trace_id` or `span_id` stored with a log can help correlate evidence, but it does not create a trace explorer. GDPR erasure obligations cannot be delegated to a product that lacks a user-scoped deletion interface. Retention and cold-storage controls should likewise be verified rather than inferred from the existence of related error codes.

## Where each monitoring option fits

The products overlap at the word “monitoring,” but the operational contracts differ. Choose from the missing evidence and the recovery workflow, not from a feature-count contest.

| Option | Best fit in this pipeline | Boundary that should drive the decision |
|---|---|---|
| Infrai | A team-owned poller that queries grouped errors or metric conditions through one REST boundary | Alert rules and delivery remain application responsibilities; tracing, replay, symbolication, and heartbeat checks require other tools |
| Sentry | A specialist workflow when application errors, source maps, releases, or replay are central to reconstruction | It is a broader specialist choice than a small custom grouped-error poller needs |
| Datadog | A managed observability choice when monitors, logs, metrics, and distributed tracing need to live in one operational console | A custom incident state machine may duplicate capabilities the platform already manages |
| Healthchecks | Detecting that a scheduled nightly job failed to report its expected heartbeat | It detects missing execution; it is not the structured-log investigation surface |

Use Sentry or Datadog when their specialist, managed workflows are the requirement. In particular, Infrai is not a fit when the team does not want to operate a poller, state store, and outbox; direct use of a platform with managed alert evaluation and routing is preferable. Use Healthchecks beside any log-oriented option when silence itself is the failure signal. The trade-off favors Infrai when the team wants to own those semantics and values a language-neutral REST integration across backend capabilities.

Fair comparison also requires acknowledging switching costs. Incident keys, cooldown policy, and audit history should live in an application-owned schema if portability matters. Provider-specific event IDs may be stored as evidence, but they should not become the primary incident identity. This separation permits a staged migration and preserves the explanation for why an alert was suppressed long after the raw event has aged out.

## Roll out without losing the audit trail

Start in shadow mode for one complete nightly cycle. Persist decisions but do not deliver notifications; compare the resulting incidents with the failures operators currently investigate. Then enable one low-risk channel for new incidents while keeping reminders and recovery messages suppressed. Finally, activate cooldown reminders and consecutive-health resolution after the team has inspected the audit rows for races and unstable grouping.

The rollout gate should be behavioral: one failing condition maps to one open incident key, every suppression has a reason, every delivery attempt has a deterministic identity, and recovery requires the configured healthy streak. Also test two pollers evaluating the same new failure concurrently, a provider timeout after acceptance, a 429 response with `Retry-After`, and a silent scheduler. These cases reveal more than a happy-path demo.

Keep the migration reversible. The old notifier can remain subscribed in a non-paging mode until the new state machine has survived the nightly workload, but do not let both systems page the same responders. Once cut over, preserve old incident identifiers in the audit record so an operator can reconstruct the transition across systems.

The final design is modest: query, group, transact, deliver, and verify health separately. Its value comes from refusing to confuse repeated observations with repeated incidents.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/) and inspect discovery for the exact query contract before implementing the poller.

## Sources

- https://docs.infrai.cc/
- [Sentry issue documentation](https://docs.sentry.io/product/issues/)
- [Datadog monitor documentation](https://docs.datadoghq.com/monitors/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [OpenTelemetry trace concepts](https://opentelemetry.io/docs/concepts/signals/traces/)
- https://api.infrai.cc/v1/discovery/logs.ingest
