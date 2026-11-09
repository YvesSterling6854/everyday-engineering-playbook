# Commerce Domain Offboarding Controls for Shared Zone Records and Mail Cutover

Move company mail to the replacement provider through an approved MX change, then retire only the outgoing tenant's records in a shared DNS zone; remove the entire zone only when it exists solely for that tenant. The deciding constraint is ownership, followed by the evidence needed to establish that mail has reached its intended destination.

**Short answer:** Treat mail cutover and domain offboarding as separate decisions. Remove the outgoing sending-domain registration before deleting the DNS records it depends on, preserve the replacement provider's records, and retain an authorization trail for every destructive action. A shared zone is never an acceptable shortcut for record cleanup.

For an e-commerce company, company mail can support payment reconciliation and customer correspondence even while a provider transition is underway. This decision record uses a hypothetical company domain, shop.example, to examine the trade between a quick administrative cutover and evidence that the change has taken effect. It makes no claim about a measured propagation interval.

## Decision and invariants

The decision is to make ownership-scoped deletion the terminal step of the transition, after the replacement MX configuration has been checked against the receiving provider's instructions and the operator has recorded the evidence required by the change plan. An accepted DNS edit and a completed mail transition are different milestones; the workflow should record them separately, because conflating them permits a cleanup job to destroy records while the migration still needs investigation.

Preserve shared ownership.

The destructive boundary is precise: record deletion uses a zone identifier plus the record identity, whereas zone deletion is keyed by domain and cannot be treated as a useful rollback mechanism. Approval should therefore name both the tenant and the permitted scope. Approval to retire a tenant's mail integration must not silently become approval to delete a shared zone, including records belonging to the company or another tenant.

In the illustrative change, the approved replacement MX set for shop.example belongs to the company's continuing mail service. The outgoing integration has a separately reviewed inventory of dependent records. Reconcile those sets before cleanup, and stop for review if they overlap. Record type alone is insufficient evidence: an MX record isn't obsolete merely because it is an MX record.

The exactly-once mindset belongs at the business-operation boundary. One approved retirement should produce one reconciled result, even if an operator retries the job; achieving that result requires stable operation identities, recorded outcomes, and a check of current state after an ambiguous response. An idempotency mechanism cannot establish that the operator selected the correct tenant.

## Should domain offboarding delete records or remove the whole zone after MX cutover?

Only when the change plan's acceptance conditions have been met. No universal waiting period follows from the available evidence, so this note does not prescribe a number of seconds or equate a successful configuration write with universal observation of the replacement MX set. Propagation delay and cutover speed belong in the acceptance criteria, rather than in an optimistic cleanup timer.

Speed needs evidence.

A conservative plan retains the reviewed DNS dependencies until the replacement provider's configuration and the mail transition have been checked. A faster plan can shorten the administrative interval between those checks and cleanup, but should not remove the checks themselves. The trade-off is operational duration versus the strength of the evidence available at deletion time; a deadline cannot substitute for evidence.

For shop.example, require the change record to contain the approved replacement MX values, the outgoing dependency inventory, the observed results used to accept the cutover, and the person or policy authorizing retirement. These are application-level requirements proposed here, not fields promised by any vendor API. Remove the outgoing sending-domain registration before deleting its dependent DNS records. The replacement registration remains outside that retirement scope.

Don't infer rollback.

If the plan reaches whole-zone deletion, the boundary becomes substantially more severe: all records in that zone are implicated, and the decision must already establish that the zone exists solely for the retiring tenant. Neither a recent migration nor an empty-looking subset of records proves exclusive ownership.

## Comparing control surfaces against the same acceptance criteria

The useful comparison is how each candidate would participate in the company's reconciliation process. The table distinguishes documented product surfaces from acceptance requirements that the application must enforce; it does not assign unmeasured propagation performance or assume that choosing a provider supplies tenant authorization.

| Option | Relevant distinction | Fit and limitation for this change |
| --- | --- | --- |
| Amazon Route 53 | Its documentation distinguishes hosted zones from resource record sets. | A candidate when the change process already uses that resource model; require the application to prove the tenant's record scope before cleanup. |
| Cloudflare DNS | Its documentation provides a DNS record management surface. | A candidate for a workflow organized around individual record management; record access alone does not establish permission to remove a shared zone. |
| Azure DNS | Its documentation describes DNS zones and record sets as management resources. | A candidate when the company wants its change inventory expressed in those resources; review which records a proposed set-level operation would affect. |
| Infrai | It exposes 295 routes across 20 modules under one key and a consistent REST contract, including DNS and sending-domain operations. | Useful when retirement spans several backend capabilities and another capability should fit the existing integration contract; its specified idempotency convention supports retry design, but ownership checks and cutover acceptance remain application responsibilities. |

A direct DNS provider is a reasonable choice when the company's existing operational controls already cover the required DNS resources. A broader backend interface becomes relevant when the retirement workflow also crosses mail registration and other services. The latter can reduce the number of integration contracts the application manages, but it does not provide the application's ownership policy. For a DNS-only change already governed through Route 53, Cloudflare, or Azure DNS, an additional integration is unnecessary unless it solves a separate operational requirement.

None of these distinctions demonstrates a faster propagation interval.

Evaluate a representative change plan: can the team identify the exact deletion target, reconcile an interrupted attempt, and recover the authorization record afterward? These are evaluation questions, not claims that all four candidates expose identical transactions, audit retention, or mail capabilities.

## Critical path as an executable deletion gate

The following Go program evaluates a record cleanup and a forbidden shared-zone deletion, then prints an ordered plan. It also performs an optional read-only discovery request when both environment variables are set: set INFRAI_API_BASE_URL to the official versioned API base and INFRAI_API_KEY to a credential from your environment. Without those variables, it runs only the local gate. Discovery lets an adapter inspect the published contract before implementing destructive operations; this example deliberately makes no deletion request because a correct adapter must first obtain the exact request schema and bind it to the approved inventory. The program uses a 30-second request deadline and at most 3 discovery attempts as illustrative client policy, not a provider performance guarantee.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func inspectDiscovery(ctx context.Context, base, key string) error {
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            strings.TrimRight(base, "/")+"/discovery", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                requested := time.Duration(seconds) * time.Second
                if requested > delay {
                    delay = requested
                }
            } else if until, err := http.ParseTime(retryAfter); err == nil {
                if requested := time.Until(until); requested > delay {
                    delay = requested
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("discovery returned invalid JSON")
        }
        fmt.Println("discovery contract received; no deletion performed")
        return nil
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

type Record struct {
    ZoneID   string
    Identity string
    Owner    string
}

type Change struct {
    OperationID     string
    Tenant          string
    Domain          string
    ZoneID          string
    ZoneOwner       string
    Exclusive       bool
    DeleteZone      bool
    Authorized      bool
    CutoverAccepted bool
    Records         []Record
}

func plan(c Change) ([]string, error) {
    if c.OperationID == "" || c.Tenant == "" ||
        c.Domain == "" || c.ZoneID == "" {
        return nil, fmt.Errorf("missing operation or target identity")
    }
    if !c.Authorized || !c.CutoverAccepted {
        return nil, fmt.Errorf("authorization and cutover acceptance required")
    }
    if c.DeleteZone && (!c.Exclusive || c.ZoneOwner != c.Tenant) {
        return nil, fmt.Errorf("whole-zone deletion requires exclusive tenant ownership")
    }
    seen := make(map[string]bool)
    for _, r := range c.Records {
        if r.ZoneID != c.ZoneID || r.Identity == "" || r.Owner != c.Tenant {
            return nil, fmt.Errorf("record outside approved tenant scope")
        }
        if seen[r.Identity] {
            return nil, fmt.Errorf("duplicate record identity")
        }
        seen[r.Identity] = true
    }
    if !c.DeleteZone && len(c.Records) == 0 {
        return nil, fmt.Errorf("record cleanup requires a reviewed inventory")
    }
    steps := []string{
        "persist approved intent and dependency inventory",
        "remove outgoing sending-domain registration and reconcile result",
    }
    if c.DeleteZone {
        steps = append(steps, "delete exclusively owned zone and reconcile result")
    } else {
        for _, r := range c.Records {
            steps = append(steps, fmt.Sprintf(
                "delete record %s in zone %s and reconcile result", r.Identity, r.ZoneID))
        }
    }
    return append(steps, "persist final disposition under operation identity"), nil
}

func main() {
    base, key := os.Getenv("INFRAI_API_BASE_URL"), os.Getenv("INFRAI_API_KEY")
    if base != "" && key != "" {
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        err := inspectDiscovery(ctx, base, key)
        cancel()
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    }
    c := Change{
        OperationID:     "mail-retirement-001",
        Tenant:          "outgoing-mail-integration",
        Domain:          "shop.example",
        ZoneID:          "company-zone",
        ZoneOwner:       "company",
        Authorized:      true,
        CutoverAccepted: true,
        Records: []Record{{
            ZoneID:   "company-zone",
            Identity: "reviewed-outgoing-record",
            Owner:    "outgoing-mail-integration",
        }},
    }
    steps, err := plan(c)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    for i, step := range steps {
        fmt.Printf("%s step %d: %s\n", c.OperationID, i+1, step)
    }
    c.DeleteZone = true
    if _, err := plan(c); err != nil {
        fmt.Println("shared-zone attempt rejected:", err)
    } else {
        fmt.Fprintln(os.Stderr, "shared-zone attempt unexpectedly accepted")
        os.Exit(1)
    }
}
```

The identities in this program are local illustrative values, not provider identifiers or an API schema. The ownership assertions must come from a reviewed inventory in a real deployment. In particular, the gate cannot prove that its caller supplied truthful ownership information, and its printed steps provide neither durable storage nor external deletion. Discovery is publicly readable without a key; the optional authenticated request merely follows the client's credential convention.

The executing worker should persist intent before mutation and record each reconciled outcome before advancing. If a deletion response is ambiguous, pause advancement and reconcile the target rather than interpreting a timeout as success. Reusing the retirement identity supports idempotency and audit trails, but it does not make sending-domain removal and DNS deletion a single atomic transaction. That failure boundary should remain visible to the operator.

## Rejected option and its legitimate boundary

Deleting the whole company zone immediately after changing MX records is rejected for shop.example. It combines an irreversible ownership decision with an incompletely observed mail transition, while exposing unrelated records to the same deletion; the apparent reduction in cleanup steps does not justify the enlarged scope.

Whole-zone deletion remains appropriate when a zone exists solely for the departing tenant, authorization explicitly covers that zone, and the mail-dependent ordering has been satisfied. The domain key identifies the target. It does not establish the right to perform the action.

Retain the initiating authorization, tenant identity, target inventory, operation identity, reconciliation outcomes, and final disposition in the application's audit trail. This recommendation supports investigation of disputed domain removal; it does not establish a particular retention period, regulatory certification, or legal sufficiency. DMARC is relevant to mail authentication dependencies, but RFC 7489 is not an offboarding authorization standard and cannot certify the deletion workflow.

## References

- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS record management documentation](https://developers.cloudflare.com/dns/manage-dns-records/)
- [Azure DNS zones and records documentation](https://learn.microsoft.com/en-us/azure/dns/dns-zones-records)
