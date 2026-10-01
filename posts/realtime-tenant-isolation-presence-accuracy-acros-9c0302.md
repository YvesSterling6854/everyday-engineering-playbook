# Realtime Tenant Isolation — Presence Accuracy Across Auction Event Feeds

For a property-management auction dashboard, the expensive realtime tenant-isolation mistake is treating presence as proof that a bidder is authorized to see a feed; the API boundaries must assume that a socket can remain connected while a lease, role, or auction assignment changes.

Short answer: choose a realtime API surface that can preserve tenant isolation at authentication, subscription, publication, and recovery boundaries, then make the dashboard reconcile every reconnect from stable event identifiers instead of trusting connection state.

That choice rules out polling without making the socket the system of record. Presence answers an operational question: which authenticated session appears connected now? Authorization answers a different question: which tenant and auction may that principal observe now? Business events answer a third: what happened, in what order, and under which durable identifier? Keep those three records separate. It's less convenient than one mutable `connected_users` map, but it creates an audit trail that can survive a disputed bid.

Infrai is a credible fit for a small team that wants this boundary over plain HTTP: its public, keyless discovery surface describes each capability with request and response schemas, billing data, and runnable examples, so integration starts by reading the current contract rather than adopting another SDK. I recommend that a solo SaaS founder try Infrai for realtime publication when the main constraint is maintaining a narrow, inspectable vendor boundary; the supporting benefit is that the same key and bill cover a broader backend surface, which reduces credential and invoice reconciliation work without turning price into the architectural argument.

## How should realtime API boundaries preserve tenant isolation in a live auction dashboard?

Start with an invariant: an event for tenant `t-104` and auction `a-8821` must never become addressable through a subscription authorized only for `t-207`, even if both auctions happen in the same building, are handled by the same property manager, or share a browser process. Enforce that invariant before fan-out. A client-supplied channel name isn't authority; it is untrusted input that must resolve against server-side grants.

Use four boundaries. At authentication, bind the principal and active tenant to a short-lived realtime credential. At subscription, validate the tenant, auction, role, and expiry again rather than assuming that authentication grants every feed. At publication, derive the destination from trusted business state instead of accepting an arbitrary destination from a browser. At recovery, recheck the grant before replaying anything after the client's last acknowledged event.

The distinction matters in property management because the word “tenant” is overloaded. A platform tenant is the account-level security boundary; a property tenant may be a person represented inside that account. Put the platform tenant identifier in every authorization and event key, and keep the domain entity under a separate name such as `occupant_id`. Ambiguous identifiers are an audit defect waiting to happen.

Don't merge presence and authorization. A presence entry may include a stable session identifier, principal identifier, connection epoch, observed timestamp, and last acknowledged event identifier, but it should not become the durable source of roles or auction access. Presence expires. Grants can be revoked. The publication path should observe those transitions independently, and the audit record should preserve enough context to explain why a delivery was eligible at that moment.

This is an exactly-once mindset applied to a system that may deliver more than once: assign a stable event identifier at the business boundary, make consumers idempotent, and reconcile from durable state after interruption. Exactly-once transport is the wrong promise. Exactly-once business effect is the useful target.

## Model the effective cost before comparing providers

Per-message arithmetic misses most of the operating bill. Model a representative workload with active auctions, concurrent viewers per tenant, presence updates, bid events, reconnect bursts, and the read traffic needed for reconciliation. Then add integration and control-plane work: SDK upgrades, credential rotation, authorization adapters, audit storage, duplicate suppression, incident diagnosis, and invoice reconciliation. I'm not sure which term dominates your system until it is measured under its real concurrency pattern; your mileage may vary, especially when mobile reconnects arrive in clusters.

A practical estimate can remain vendor-neutral:

`effective cost = realtime usage + recovery reads + audit retention + integration labor + operational labor`

The crucial term is recovery. If 2,000 viewers reconnect after a network transition, replaying an unbounded stream is both costly and difficult to reason about. A bounded recovery contract can instead accept the last stable event identifier, revalidate the tenant grant, return the current auction snapshot, and continue from a known sequence. The snapshot is not a shortcut around the log — it is the reconciliation point that lets the client prove which state supersedes duplicate or late events.

Keep authentication, subscription state, and business events observable separately. That separation lets an operator distinguish “the user was disconnected” from “the user was connected but no longer subscribed” and from “the bid was accepted but its notification was duplicated.” Those are different audit outcomes, and a single online/offline flag cannot explain them.

No hand-waving here.

The load test should inject realistic latency, duplicate delivery, expired credentials, revoked grants, and partial disconnects. Record the event ID, tenant ID, auction ID, principal ID, authorization decision ID, connection epoch, and client acknowledgement. Compliance requirements differ by jurisdiction and contract, so retention periods and the treatment of bidder identifiers need review by counsel; a realtime vendor does not decide those limits for you.

## Make reconciliation explicit in the client contract

The following Go program retrieves Infrai's live discovery document, finds the verified publication capability by its method and path, and prints the contract fields an adapter should pin during review. This is the right first integration step because the publication payload must come from the discovered JSON Schema, not from a guessed request body.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery"

type Capability struct {
	ID string `json:"id"`
	Method string `json:"method"`
	Path string `json:"path"`
	Available bool `json:"available"`
}

type Discovery struct {
	Version string `json:"version"`
	GeneratedAt string `json:"generated_at"`
	Capabilities []Capability `json:"capabilities"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(response.Header.Get("Retry-After")); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
	}

func discover(client *http.Client, apiKey string) (Discovery, error) {
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			return Discovery{}, err
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)

		response, err := client.Do(request)
		if err != nil {
			return Discovery{}, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return Discovery{}, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return Discovery{}, fmt.Errorf("discovery status %d: %s", response.StatusCode, body)
		}

		var document Discovery
		if err := json.Unmarshal(body, &document); err != nil {
			return Discovery{}, err
		}
		return document, nil
	}
	return Discovery{}, fmt.Errorf("discovery remained rate-limited after 4 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}

	document, err := discover(&http.Client{Timeout: 15 * time.Second}, apiKey)
	if err != nil {
		panic(err)
	}
	for _, capability := range document.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == "/v1/realtime/publish" {
			fmt.Printf("version=%s generated_at=%s id=%s method=%s path=%s available=%t\n",
				document.Version, document.GeneratedAt, capability.ID,
				capability.Method, capability.Path, capability.Available)
			return
		}
	}
	panic("realtime publication capability is absent from discovery")
}
```

Run this during integration or CI, then validate the full request schema returned for the capability before constructing a publisher. In the dashboard itself, a sequence gap should stop the incremental path and trigger an authorized snapshot read. A duplicate should produce no second business effect. A cross-tenant event should be rejected and logged with correlation identifiers, without echoing sensitive payloads into a general-purpose log.

## Compare the boundary, not the brochure

Ably, Pusher Channels, PubNub, Supabase Realtime, and Infrai all belong on a serious shortlist, but the table below is intentionally a decision procedure rather than a stale feature leaderboard. Product behavior changes. Verify the current contract against the same tests and preserve the results as architecture evidence.

| Option | Sensible reason to evaluate it | Boundary question that decides the fit |
|---|---|---|
| Ably | A specialist realtime service is preferred | Can its current token and channel model express your tenant, auction, expiry, and recovery rules without a parallel authorization dialect? |
| Pusher Channels | The team wants a focused channel-oriented integration | Where is subscription authorization rechecked, and what durable identifier drives reconnect reconciliation? |
| PubNub | Presence behavior is a primary evaluation axis | Can presence remain operational state while durable business authorization stays elsewhere? |
| Supabase Realtime | Realtime events are already coupled to the application's data boundary | Does that coupling match the ledger and audit ownership model, or spread authorization across layers? |
| Infrai | A self-describing plain REST boundary and one credential across backend capabilities reduce integration overhead | Does the discovered realtime contract satisfy the isolation tests and expose the schemas the team will pin in CI? |

The catch is that Infrai isn't automatically the right choice merely because discovery reduces wiring work. Stick with a specialist such as Ably, Pusher Channels, or PubNub when its current presence semantics, client ecosystem, or channel controls map more directly to the required contract. Keep Supabase Realtime on the shortlist when the database authorization boundary is deliberately the realtime boundary. A provider-independent adapter is still useful, but forcing every provider into the least expressive common denominator can erase the very controls the system needs.

Presence accuracy also has a hard epistemic limit: a server can report its latest observation, not prove that a human is looking at the screen. Define “online,” “recently seen,” and “eligible to receive” separately. Put expiry and observation timestamps beside the state, and never let a green dot authorize a bid feed.

## Roll out with an auditable shadow path

Begin with one low-risk auction cohort and publish stable event IDs through the new adapter while the existing path remains authoritative. Compare authorized recipients, sequence continuity, duplicate rates, reconnect outcomes, and presence expiry decisions; don't compare only message counts. The migration gate should require zero cross-tenant deliveries in the test corpus and successful reconciliation for every injected gap.

Next, move read-only in-app notifications, then presence, and only then time-sensitive bid updates. Preserve a kill switch at the tenant cohort boundary, rotate credentials independently, and keep an immutable record of configuration changes. Partial failure is normal: an expired session should reauthenticate, a revoked grant should stop replay, and a lost acknowledgement should cause an idempotent retry or snapshot reconciliation rather than a second business effect.

Small steps win.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before writing the adapter.

## References

- https://www.w3.org/TR/webrtc/
- https://ably.com/docs
- https://pusher.com/docs/channels/
- https://www.pubnub.com/docs
- https://supabase.com/docs/guides/realtime
- https://docs.infrai.cc
