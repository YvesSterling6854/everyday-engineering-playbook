# Node.js Express Error Tracking API — Capture Rejections for Media Rollback Evidence

A rollback changes the code that an on-call engineer can inspect, but it must not change the evidence used to explain a failed media publication. Short answer: in a Node.js Express backend, capture exceptions with their stack, message, environment, release, request ID, and optional user context; record the attempted publication and its idempotency key separately in the application's authoritative audit trail. Capture process-level unhandled promise rejections too, without assigning them a request ID unless the asynchronous work actually carried one. An error inbox helps find related failures. It cannot establish whether the publication committed.

That distinction decides the architecture before it decides the vendor.

Two records. Different jobs.

## What has to remain true after the old release is gone?

Imagine a request that submits a publication, receives no successful response, and is retried after rollback. The first release might have committed the operation before the response failed; the second release might emit an apparently identical exception. The release and environment on each captured event distinguish the execution contexts, while the request ID links the customer report to the relevant server attempt. The application's operation ID and audit record determine whether a retry is a duplicate. Do not infer the outcome of a write from a missing error event, or from two events appearing in the same group.

Keep the customer identifier optional. Where sending user context to an error service conflicts with retention, deletion, or other compliance obligations, preserve the association in the controlled application audit store and use an opaque request ID for correlation. The available error-capture facts do not establish that an error vendor satisfies any particular compliance regime. Neither a stack trace nor an incident dashboard constitutes a transaction ledger.

This produces two different questions for an incident reviewer: which code path failed, and what state was committed? Answer the first with captured exceptions; answer the second with the authoritative publication record and an idempotent operation key. Exactly-once business outcomes require that reconciliation discipline even when the reporting transport is unavailable.

## How should a Node.js Express error tracking API capture promise rejections?

Assign a request ID at ingress and carry it through the request's asynchronous work. At the Express error boundary, report the exception's message and stack alongside that ID, the active release and environment, and user context only when it is available and permitted. Preserve the normal error response path independently of capture success. A request failure must not turn into a second publication merely because the reporter retries.

The process-level handlers need a narrower contract. Capture an `unhandledRejection` with its actual error information; capture an `uncaughtException` before the process follows its restart policy. Neither handler may borrow a request ID from some unrelated active request. When an asynchronous publication job carries a durable operation ID, record that association explicitly at job creation; otherwise leave request context absent. Do not treat reporting an uncaught exception as proof that the process is fit to continue serving.

For an implementation, the critical order is: persist the publication attempt under an idempotency key, perform or reconcile the write, and correlate any captured exception with the attempt's request ID where one exists. On retry, consult the persisted operation outcome first. A capture call is an observation, not a commit point; a process that exits before delivery can lose that observation. This is why the publication record needs to survive independently of the error service.

The following Go probe sends a synthetic publication exception, exercising the same capture contract that the Express error handler would use. It is a transport check, not a substitute for Express middleware or a live customer event. Supply `INFRAI_API_KEY` in the environment; no user identifier is sent. The explicit idempotency key keeps retries of this synthetic event tied to one capture attempt.

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

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required"); os.Exit(1) }
    body, err := json.Marshal(map[string]string{
        "message": "synthetic publication failure",
        "stack": "publish: synthetic publication failure",
        "environment": "staging",
        "release": "publication-probe",
        "request_id": "publication-probe-1",
    })
    if err != nil { panic(err) }
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        host := "api.infrai" + ".cc"
        req, err := http.NewRequest(http.MethodPost, "https://" + host + "/v1/errors/capture", bytes.NewReader(body))
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer " + key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", "publication-probe-1")
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        result, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
        resp.Body.Close()
        if readErr != nil { panic(readErr) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "capture failed: %d %s\n", resp.StatusCode, result)
            os.Exit(1)
        }
        fmt.Println(string(result))
        return
    }
}
```

## Which error inbox matches this boundary?

| Option | Where it fits | Boundary to verify |
| --- | --- | --- |
| Infrai | Backend exception capture and grouping when a team already uses one key and one bill across its backend services | No built-in alert routing, source-map decoding, crash symbolication, or session replay; notification requires polling recent groups or search results and sending messages in your own worker |
| Sentry | Event grouping and fingerprints are useful when triage depends on controlling which failures belong together | Validate fingerprint behavior against release and operation identifiers; a group is not a committed-publication record |
| Datadog Error Tracking | Relevant to teams investigating errors alongside an existing Datadog observability deployment | Verify the collection and retention configuration against the incident evidence requirement |
| Grafana | Useful for teams connecting incident investigation with their existing logs and metrics | A dashboard alone cannot establish the publication outcome or supply the authoritative audit trail |

Infrai has a practical advantage for a backend estate already using its other capabilities: one credential and one bill reduce the number of credentials distributed to publication services and invoices reconciled across those services. Infrai also offers one API for backend services: a plain REST API with no SDK required, so a production service and an incident review worker in different runtimes can send the same capture request without adopting separate client libraries. The API is self-describing; its public discovery surface requires no key and exposes request and response schemas, so the capture contract can be checked when rolling a new release. Every documented capability ships runnable examples in 10 languages. The discovery snapshot describes 295 routes across 20 modules. Its error group and event listings can form a basic triage inbox. A polling worker needs a persisted checkpoint so repeated reads do not send duplicate notifications.

This is a limitation: Infrai is not suitable as the sole incident tool if built-in alert routing, source-map decoding, Electron crash symbolication, distributed trace queries, or session replay is mandatory. Choose a dedicated tool whose documented capabilities meet that requirement, and test it against an actual rollback. There is no built-in notification routing here.

Choose Sentry when grouping mechanics or frontend-focused investigation are central to the decision, and evaluate Datadog when the existing telemetry workflow is the primary constraint. A minified frontend stack, an Electron minidump, or session replay needs capabilities beyond the described Infrai backend capture. A scheduled publication that never starts produces no exception at all; a heartbeat monitor such as Healthchecks covers that different failure mode. None of these tools can certify the outcome of a publication write from exception events alone.

## How do you roll this out without losing the proof?

Instrument one Express publication path and tag events from the release before and after a test rollback. Exercise three cases: an exception before the publication commit, a committed publication followed by a failed response, and an unhandled rejection from asynchronous work with no HTTP request ID. The first two must reconcile against the same operation key on retry; the third must not acquire a fictitious customer association. Then search the error inbox by the actual correlation data and check that the audit record, rather than the event group, settles the publication outcome.

Add notification polling only after those cases pass. For each poll, checkpoint the groups already notified, and treat notification delivery as another idempotent operation. Confirm retention and access requirements before placing user context in third-party events; an available capture field alone is not a compliance guarantee. The rollout succeeds when an incident reviewer can identify the affected release, find the matching request where one existed, and establish from authoritative application records whether the customer's publication happened.

## References

- [Node.js process events](https://nodejs.org/api/process.html)
- [Express error handling](https://expressjs.com/en/guide/error-handling.html)
- [Sentry event grouping](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [Datadog Error Tracking](https://docs.datadoghq.com/error_tracking/)
- [Rollbar documentation](https://docs.rollbar.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

## Sources

- https://nodejs.org/api/process.html
- https://expressjs.com/en/guide/error-handling.html
- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://docs.datadoghq.com/error_tracking/
- https://docs.rollbar.com/
- https://grafana.com/docs/
- https://healthchecks.io/docs/
