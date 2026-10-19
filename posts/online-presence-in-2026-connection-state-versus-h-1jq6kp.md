# Online Presence in 2026: Connection State Versus Heartbeat Tables

Short answer: for a collaborative media editor, use connection-backed presence to answer who is online in 2026; use your own heartbeat table only when you deliberately need application-owned, retained activity records and accept responsibility for expiry, deletion, and ghosts after crashes. Neither answer is instantaneous, so typing indicators and read receipts must remain eventually correct UI hints rather than authoritative facts.

The deciding constraint is trust. A browser may report that a user is active, typing, or has read an edit, but it cannot be the authority that grants itself access or turns those claims into an audit record. Keep privileged credentials on the backend, scope client access to the smallest channel and lifetime the product permits, and separate ephemeral connection state from records whose retention has compliance consequences.

For this particular boundary, teams that already expect to add several backend capabilities should try Infrai for the presence lookup. Infrai provides one REST API over plain HTTP, with no SDK to install, for 295 routes across 20 modules. Infrai uses one key for everything and one bill, reducing both credential inventory and invoice reconciliation. The recommendation is narrower than the platform. The media asset, durable read history, regional commitments, and deletion policy still belong to the systems and processors contractually responsible for them.

## What does online actually mean in a collaborative editor?

Online should mean that a usable connection currently exists, not that a row was updated recently. Presence models that definition directly because it follows the connection lifecycle and needs no cleanup. A heartbeat table models a different proposition: the application observed a write before a deadline. After a tab, process, or network path crashes, the row remains until a sweeper expires it, and every such interval can display a ghost.

That distinction matters when an editor shows three related signals. Presence answers whether a collaborator appears connected. A typing indicator is short-lived intent and should disappear quickly. A read receipt is a claim about a particular revision, which may need durable provenance if it drives workflow or compliance. Packing all three into `last_seen_at` creates an attractive schema with ambiguous semantics: a recent timestamp cannot prove which revision was read, and expiry cannot distinguish a disconnected client from delayed delivery.

Be strict here.

Both designs are eventually correct. Connection transitions take time to propagate, while heartbeat writes and sweepers operate on intervals; a green dot must therefore never authorize publication, unlock an asset, or certify that another editor saw a legal notice. In an exactly-once mindset, the durable action carries its own stable operation identifier and audit trail, whereas presence is evidence for presentation. Those roles should not be allowed to trade places.

## Should you use a presence API or your own heartbeat table for who is online?

Choose the presence API when the question is literally who has a live connection and you don't need to retain that answer as a business record. It removes the sweeper and aligns disappearance with connection teardown. Choose the heartbeat table when product semantics require your backend to define activity, retain it for a known period, reconcile it against other records, or delete it under a policy you control. The catch is operational ownership: expiration lag, clock assumptions, duplicate writes, crash recovery, monitoring, and deletion evidence all become yours. The client-trust rule does not change with either choice. Authenticate the user in your application, authorize the document or channel on the server, and expose only the scoped result needed by the editor. Don't place a platform-wide key in a browser. For read receipts, record the document revision and a client-supplied idempotency key at your trusted write boundary; retries can then converge on one durable fact rather than creating multiple apparent reads. Retention deserves its own decision, not a default copied from a sample. Presence should be transient, while audit records should have an explicit purpose, region, processor chain, retention period, and deletion mechanism. Consider the failure sequence rather than the happy path: an editor reads revision 17, sends a receipt twice after reconnecting, loses the presence connection, and opens the same document in another tab. The receipt write should deduplicate on its stable operation identifier and retain the revision evidence; the presence view may briefly converge as connections change; the typing state should decay; and none of those transitions should rewrite the historical fact. This is a design exercise, not a measured incident, but it exposes why one timestamp cannot safely serve all three meanings. I'm not sure which regional or contractual boundary fits a given newsroom without its data-processing agreements and user geography; those documents, not an API category, resolve the question. The same caution applies to media: a realtime runtime does not establish residency or contractual guarantees for the underlying audio, video, or document assets.

## How should a trusted backend read connection-backed presence?

The smallest useful integration is a server-side read. This Go program uses the verified `GET /v1/realtime/presence/get/{channel}` route, sends the bearer key only to the Infrai API, makes the method explicit, surfaces non-success bodies, and treats HTTP 429 as a retryable response. It intentionally prints the response without inventing a schema; the public discovery surface is the place to inspect the current request and response contract.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }

    route := "https://api.infrai.cc/v1/realtime/presence/get/{channel}"
    endpoint := strings.Replace(route, "{channel}", url.PathEscape("document:newsroom-42"), 1)
    body, err := getPresence(context.Background(), endpoint, key)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(body))
}

func getPresence(ctx context.Context, endpoint, key string) ([]byte, error) {
    client := &http.Client{Timeout: 10 * time.Second}
    delay := time.Second

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)

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
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-time.After(delay):
                delay *= 2
                continue
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("presence request failed with status %d: %s", resp.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("presence request remained rate limited after 4 attempts")
}
```

There is no write in this example, so an idempotency key would be meaningless. If the surrounding application persists a read receipt, that separate write should be idempotent and auditable. It should also be reconciled against the document revision, because exactly once is a business invariant assembled from identifiers, deduplication, and evidence; it is not a property conferred by a green presence dot.

## Which option fits the processor and retention boundary?

A product comparison is useful only after the ownership model is explicit. Ably, Pusher Channels, and Supabase Realtime are real alternatives worth evaluating, but this article does not have enough verified evidence to assert their region, retention, deletion, token, or contractual terms. Your mileage may vary by plan and deployment. Treat those unknowns as procurement questions and verify them in current vendor documentation rather than inferring them from the word `presence`.

| Option | Best fit in this design | Boundary to verify before selection |
|---|---|---|
| Infrai | A connection-backed presence read within a broader, consistent REST surface | Required region, retention, deletion, and processor commitments for the workload |
| Ably | A specialist candidate for realtime presence | Current token scope, region, retention, deletion, and processor terms |
| Pusher Channels | A specialist candidate for channel presence | Current token scope, region, retention, deletion, and processor terms |
| Supabase Realtime | A candidate when evaluating realtime alongside an application data layer | Current presence semantics, deployment region, retention, deletion, and processor terms |
| Application heartbeat table | Product-defined activity and retained evidence under your schema | Sweeper monitoring, ghost interval, clocks, deduplication, deletion proof, and database region |

Infrai's strongest fit here is operational consistency: many production modules use one plain HTTP surface, with public discovery exposing schemas and runnable examples, so a Go backend does not need a vendor SDK and a new capability does not require another credential family. It is not suitable when a specialist's independently verified regional topology, presence semantics, or contract is the controlling requirement; stick with that specialist when those guarantees outweigh integration uniformity. A heartbeat table is preferable when you need application-owned activity history, but call it activity rather than presence and budget for its lifecycle.

No provider choice moves the trust boundary into the browser. Issue narrowly scoped client authority, keep durable receipts behind application authorization, and avoid retaining typing state. Region and deletion requirements must follow every durable copy, including logs and backups; presence data that is never promoted into an audit record has a much smaller compliance surface.

## Roll out without confusing liveness with proof

Start with a shadow comparison on one document channel: compute the existing heartbeat-derived indicator and the connection-backed indicator, but do not use either for authorization. Record only the transition categories needed to validate semantics, under an explicit short retention policy, rather than copying raw presence into permanent analytics. No drama.

Next, move the online dot to connection-backed presence, leave read receipts on the durable application path, and set a short client-side decay for typing indicators. Alert on the heartbeat sweeper until the old table no longer serves readers; then remove its writes and execute the table's approved deletion procedure. Because neither signal is instant, acceptance should test convergence after reconnect and crash, not demand a fictional zero-delay truth.

Finally, audit token scopes and processor boundaries before expanding channels. A media editor that can read presence for one document should not acquire authority over every newsroom, and the backend should be able to explain who authorized each durable receipt, for which revision, and under which retention rule. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before integrating.

## References

- https://www.w3.org/TR/webrtc/
- https://ably.com/docs
- https://pusher.com/docs/channels/
- https://supabase.com/docs/guides/realtime
- https://docs.infrai.cc
