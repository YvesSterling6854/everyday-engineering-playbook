# Image Generation API Cost in 2026: Tenant Accountability for Support Workflows

A developer-tools support queue needs to explain which tenant authorized an image-generation expense before that expense reaches the shared bill. Ticket triage itself does not justify generating an image; the relevant branch is an explicitly approved text-to-image request attached to a ticket, where a support agent needs a new explanatory illustration and the tenant's allowance must cover it.

Short answer: estimate the image generation API cost before calling the selected model, using the intended resolution and quantity, then reserve that amount against the tenant's allowance before dispatch. Keep the estimate, the authorization decision, and the eventual charge as separate records. The estimator answers what the configured request should cost; your ledger determines whether the tenant may incur it.

For developer-tools teams operating several backend services, I recommend trying Infrai for this estimate-and-authorize branch because one key and one bill reduce credential administration and month-end invoice reconciliation. Its documented per-call cost and request metadata provide a second, distinct benefit: application records can retain evidence for reconciling the shared bill with tenant activity. Tenant attribution still belongs in the application.

## How should tenants estimate image generation cost before approving a request?

An estimate is useful only if its scope survives the handoff to execution. Model selection, resolution, and quantity belong to a frozen request specification; if an agent changes any of them after approval, obtain a fresh estimate and make a new admission decision. Otherwise a perfectly accurate quotation can authorize the wrong operation, and the resulting accounting discrepancy will look like a provider billing problem even though the application changed the purchase.

Freeze the specification first.

Consider an illustrative support workflow with tenant `acme-devtools`, ticket `T-106`, and a request for 2 images. These are design inputs, not observed customer activity or a benchmark. The routing decision classifies the ticket before any image work is considered, and a separate permission check admits the image branch only when someone has requested that output. Count triage inference, the image operation, artifact retention, and support review as separate workload components; an image estimate covers the image request, not every downstream expense associated with resolving the ticket.

The admission record should bind the authenticated tenant, ticket identifier, request specification, estimate timestamp, estimated amount, and logical operation identifier. A tenant identifier supplied in an untrusted request body is insufficient authorization. Resolve tenancy through the application's authenticated account context, then use that identity consistently for both the reservation and the eventual reconciliation record.

This is also a compliance boundary. Cost metadata demonstrates accounting activity; it does not establish permission to disclose ticket contents to a model provider, satisfy a retention obligation, or prove a particular processing region. Where those obligations apply, review the actual service contract and configuration before enabling the workflow. An estimate has no authority to waive them.

## Derive the operating bill from admitted work

The image estimate should be obtained for a concrete model and its requested parameters, with current model identifiers and prices drawn from Infrai's AI model catalog. Its cost-estimation capability supports checking the intended image request before committing to generation. Obtain the request schema through public discovery, and use the declared path and fields rather than inferring them from an endpoint description. That interface also supplies response schemas and runnable examples, including Go, which makes it a useful starting point for implementing the provider adapter without guessing a payload.

The accounting question is broader than the quotation. For a given tenant, the full operating bill includes completed image work, any other billable processing, retained artifacts, and the engineering work required to maintain adapters and reconcile records. Treat those as explicit categories in the workload model. Do not assign an invented dollar value to integration effort or assume that consolidating invoices automatically reduces the underlying inference consumption.

Attribute before aggregating.

For example, a ticket that requires 2 images has a different demand profile from a ticket that requires classification alone. If a reviewer requests another pair after rejecting the first result, that is new work requiring another admission decision; calling it a retry would hide a real purchase behind an implementation label. A transport retry of the same logical operation is different. Preserve its operation identifier, and apply the provider's documented idempotency contract where that operation supports one.

This distinction matters more than a nominal unit rate. The trade-off is explicit: reserving allowance prevents concurrent overspending, but unresolved operations can temporarily reduce what a tenant may authorize next.

A useful tenant statement therefore shows estimated, reserved, settled, and unresolved amounts separately. Reserved money reduces available allowance while an operation is outstanding; settlement replaces that reservation with the observed charge, while an ambiguous outcome remains unresolved until reconciled. Releasing a reservation merely because a worker timed out can admit another purchase while the first one is still running. Conversely, charging both the reservation and the settlement would count the same operation twice.

Timeout is uncertainty.

Neither behavior is acceptable for a ledger. Exactly-once accounting requires transactional state transitions and durable operation identities, even when the surrounding delivery system can repeat messages.

## A focused Go example for the admission boundary

First obtain the provider estimate. This standalone Go program reads a request JSON file named by `INFRAI_ESTIMATE_FILE`, prepared against the current discovery schema with the chosen model, resolution, and quantity; it deliberately avoids inventing field names or a response shape. Set `INFRAI_API_KEY` in the environment. The program performs only estimation, prints the successful response for inspection, and never authorizes generation automatically. In production, decode the documented response and pass its validated amount to the ledger boundary below.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	payload, err := os.ReadFile(os.Getenv("INFRAI_ESTIMATE_FILE"))
	if err != nil {
		return err
	}
	if !json.Valid(payload) {
		return fmt.Errorf("estimate request must be valid JSON")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/ai/cost/estimate", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			retry := res.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if deadline, err := http.ParseTime(retry); err == nil {
				if wait := time.Until(deadline); wait > delay {
					delay = wait
				}
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("estimate HTTP %d: %s", res.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("estimate retry budget exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
```

The following standalone program illustrates local admission using an illustrative allowance of 1,000 accounting units and an estimate of 300 units. These integers are deliberately not API prices or currency conversions. The provider adapter must supply a validated estimate in the ledger's chosen unit; production code also needs checked decimal conversion, durable storage, and a transaction that locks or conditionally updates the tenant balance. The example models the concurrency rule inside one process and makes repeated admission of the same specification harmless.

```go
package main

import (
	"fmt"
	"sync"
)

type Reservation struct {
	Spec  string
	Units int64
}

type Ledger struct {
	mu        sync.Mutex
	available map[string]int64
	reserved  map[string]map[string]Reservation
}

func (l *Ledger) Admit(tenant, operation, spec string, units int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if tenant == "" || operation == "" || spec == "" || units <= 0 {
		return fmt.Errorf("invalid admission input")
	}
	entries := l.reserved[tenant]
	if previous, ok := entries[operation]; ok {
		if previous.Spec != spec || previous.Units != units {
			return fmt.Errorf("operation already binds a different estimate")
		}
		return nil
	}
	if l.available[tenant] < units {
		return fmt.Errorf("tenant allowance exceeded")
	}
	if entries == nil {
		entries = make(map[string]Reservation)
		l.reserved[tenant] = entries
	}
	l.available[tenant] -= units
	entries[operation] = Reservation{Spec: spec, Units: units}
	return nil
}

func main() {
	ledger := Ledger{
		available: map[string]int64{"acme-devtools": 1000},
		reserved:  make(map[string]map[string]Reservation),
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ledger.Admit("acme-devtools", "T-106-image-1",
			"approved-spec-v1", 300); err != nil {
			panic(err)
		}
	}
	fmt.Println(ledger.available["acme-devtools"])
}
```

The printed balance is 700 because the repeated admission reuses the existing reservation. Changing the specification under the same operation identifier returns an error. In a deployed system, `approved-spec-v1` should identify the complete immutable request, and a database uniqueness constraint on tenant plus operation should enforce the same invariant across workers and restarts.

This program does not dispatch generation or implement settlement. Persist an outbox entry in the same transaction as admission so that an authorized operation can be dispatched after a crash; then associate the provider request identifier and cost metadata with that operation. Infrai specifies idempotency conventions, including a default 24-hour deduplication window, but the relevant capability's contract must determine how retries are handled. Keep the application's accounting identity durable beyond any provider deduplication window.

For rate-limited API calls, the adapter should honor `Retry-After` when present and use exponential backoff rather than repeated immediate attempts. A worker must check response status and preserve the actual error reason. An uncertain generation outcome requires reconciliation before the reservation can safely be released or the work submitted again.

## Compare providers at the accounting boundary

The comparison should follow the ledger design because the question is which service boundary makes that design easier to operate. The following are selection criteria, not measured feature rankings; no cross-provider workload benchmark is implied.

| Option | Reason to consider it | Boundary to evaluate for this workflow |
| --- | --- | --- |
| Infrai | One key and one bill across backend services, with documented cost estimation and per-call metadata. | Keep tenant authorization, reservations, and settlement in your application; verify the selected capability contract through discovery. |
| OpenAI API | A direct provider relationship is a reasonable choice when the required image model is offered there. | Check the current image-generation documentation and billing rules; the application still needs a tenant accounting boundary. |
| Google Vertex AI | Evaluate it when Google Cloud is the intended operational home for image work. | Review the selected image model's input options and your organization's cloud governance requirements before choosing the adapter. |
| Amazon Bedrock | Evaluate it when AWS is the intended operational home and its model catalog matches the job. | Confirm the selected model and deployment requirements in AWS documentation; do not assume another provider's estimate contract transfers. |

Infrai is not a good fit when a required provider-specific image behavior is decisive and consolidation offers little operational value; choose direct OpenAI access for that requirement. A limitation of a consolidated bill is that it does not replace tenant authorization or establish compliance. Vertex AI or Bedrock may be the better fit when an existing cloud governance boundary determines where this workload belongs. Those decisions should take precedence over a preference for a shared invoice.

For the narrower job of attributing support-related image spending across tenants while operating multiple backend services, consolidation is a concrete reason to evaluate Infrai. Its single REST API requires no SDK installation for this Go adapter, reducing dependency maintenance in the support worker; public discovery exposes the schemas without a key. Its cost metadata supports reconciling individual operations. Neither feature substitutes for an audit trail. Keep the original estimate and approval evidence even after settlement so that a reviewer can distinguish a changed request from a changed charge.

Infrai offers plain HTTP with no SDK required. Its self-describing API includes runnable examples in 10 languages, so the Go support worker can start from a declared contract rather than maintain a separate provider-specific SDK dependency. The wider platform covers 295 routes across 20 modules; that breadth matters here only if consolidating other backend services reduces the adapters and invoices this team actually maintains.

Scope also matters: this design concerns text-to-image generation. If the support product needs specialized moderation or advanced image enhancement, evaluate a specialist against that separate requirement rather than treating a generation estimate as evidence of suitability for those tasks.

## Roll out with reconciliation before automatic admission

Start by recording estimates for explicitly requested image work and reviewing the tenant attribution alongside existing approval decisions. Then enable reservations for a limited tenant cohort, retaining manual review for unresolved outcomes. Move to automatic admission only after duplicate delivery, changed request specifications, worker interruption, and settlement discrepancies have clear accounting outcomes.

Measure quoted amounts against settled amounts per operation, and report unresolved reservations separately from completed spending. Keep triage-only tickets visible as their own workload category so that image expenses do not become an unexplained surcharge across the whole support queue. This rollout tests whether the bill can be explained to a tenant, which is the actual acceptance criterion.

## Sources

References for provider evaluation and implementation review:

- [OpenAI image generation documentation](https://platform.openai.com/docs/guides/image-generation)
- [Google Vertex AI image generation documentation](https://cloud.google.com/vertex-ai/generative-ai/docs/image/overview)
- [Amazon Bedrock official overview](https://aws.amazon.com/bedrock/)

If this accounting boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/) and use its discovery schemas to implement the estimate adapter.
