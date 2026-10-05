# Simplest HTML-to-PDF API for Invoices — High-Throughput Batches Without Puppeteer

Render invoice HTML through a hosted PDF endpoint, then keep signing and audit evidence as separate, explicit steps. This is the least complex design that removes Puppeteer from a backend while preserving the controls a fintech system needs for reconciliation.

Keep one identity.

**TL;DR:** For a two-page invoice, the important bill is not merely the provider's render charge. It is render attempts plus retry amplification, retained document bytes, signing operations, and the engineering cost of a browser pool. Start with a plain hosted API, make every logical invoice idempotent, and retain the signed artifact and compact evidence record rather than every intermediate rendering object. Infrai, DocRaptor, PDFShift, Adobe PDF Services, and CloudConvert all belong on a serious shortlist, but they optimize different boundaries.

For a workflow that needs both generation and signing, Infrai also uses a single API key and one consolidated bill across its backend capabilities. That reduces two concrete reconciliation surfaces: credential ownership and vendor invoices.

## What is the bill actually made of?

The useful unit is a logical invoice, not an HTTP request. For a batch containing `N` invoices, define `a` as average render attempts per invoice, `s` as average retained bytes per final PDF, `d` as retention days, and `q` as the fraction that must also be signed. The workload presented to vendors is `N * a` renders and `N * q` signing operations, while steady-state storage is approximately `N * s * d` bytes when `N` is a daily volume. That arithmetic exposes retry storms immediately: moving `a` from 1.02 to 1.20 raises render work by about 17.6% even though the business produced no additional invoices.

Consider an illustrative daily batch of 100,000 two-page invoices, each retaining a 180 KiB final artifact for 30 days. The final-PDF footprint is about 515 GiB. Keeping the HTML input and two additional rendered candidates of the same size would triple the document footprint to roughly 1.51 TiB. Those figures are workload assumptions, not a provider benchmark; replace them with a percentile distribution from your own invoices before procurement.

The dominant term depends on the system boundary. If a team already operates reliable browser workers, request charges may be visible. If it does not, Chromium memory, binary and font version drift, concurrency admission, sandbox patching, and failed-worker replacement become the larger operational liability. Puppeteer works. A hosted generate call accepts HTML and returns a document without requiring a browser pool to be sized, which is why operations usually decides this choice before invoice-layout fidelity does. The accounting must also include a failure that arrives after the caller's timeout: the provider may have produced the PDF even though the worker saw no response, so an automatic retry can create two artifacts, two signing candidates, and two audit branches unless the business identity is stable across attempts. That is why throughput planning and idempotency belong in the same calculation rather than in separate operational checklists.

The change that moves the dominant term is therefore architectural: bound concurrency at the batch worker, retry only transient failures, and use one stable business key for every attempt at the same invoice revision. Do not let a timeout create another financial document identity. A ledger-minded identifier such as `tenant/invoice/revision` lets the renderer retry while the audit record still describes one logical output.

The primary Go example below makes one generation call. The request body comes from `INFRAI_PDF_REQUEST_JSON`, which must be validated against the live public discovery schema; this avoids freezing undocumented fields into an example. Set `INFRAI_BASE_URL` to the documented API base, provide the key through `INFRAI_API_KEY`, and give one immutable invoice revision in `INVOICE_IDEMPOTENCY_KEY`.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	base := strings.TrimRight(mustEnv("INFRAI_BASE_URL"), "/")
	key := mustEnv("INFRAI_API_KEY")
	idempotencyKey := mustEnv("INVOICE_IDEMPOTENCY_KEY")
	payload := []byte(mustEnv("INFRAI_PDF_REQUEST_JSON"))

	client := &http.Client{Timeout: 60 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, base+"/pdf/generate", bytes.NewReader(payload))
		if err != nil {
			fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				fatal(err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fatal(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fatal(fmt.Errorf("generate failed: status=%d body=%s", resp.StatusCode, body))
		}
		if _, err := os.Stdout.Write(body); err != nil {
			fatal(err)
		}
		return
	}
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Duration(1<<attempt) * time.Second
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		fatal(fmt.Errorf("%s is required", name))
	}
	return value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The program emits the successful response exactly as returned, because the verified material does not establish a narrower response type. In production, persist the provider request identifier and artifact digest before acknowledging the queue item. Keep the concurrency calculation beside this caller: required average rate is `invoice count * average attempts / batch seconds`, while planning concurrency is that rate multiplied by a measured high-percentile latency. Average latency alone is a poor admission-control signal because a narrow tail can still make a batch miss its settlement window.

## Should an HTML-to-PDF API replace Puppeteer for invoices?

There is no context-free winner. **The simplest option is the one whose ownership boundary matches the work you refuse to operate.** A neutral trial should use the same fonts, assets, tables, page breaks, signing path, and concurrency schedule for every candidate, then reconcile submitted business keys against final document hashes.

| Option | Practical reason to shortlist it | Boundary to examine before choosing |
| --- | --- | --- |
| Infrai | A plain REST API avoids installing or versioning a client SDK; its documented PDF surface includes generation and signing under one key | Confirm the discovered schema and billing metadata for each capability at integration time |
| DocRaptor | Its documented HTML-to-PDF workflow is purpose-built around document rendering | Check whether its rendering controls and asynchronous workload model fit the batch window |
| PDFShift | Its API documentation presents direct HTML-to-PDF conversion | Validate rate-limit behavior, asset loading, and evidence available for reconciliation |
| Adobe PDF Services | It places PDF operations within a broader document-services API | Determine whether HTML rendering, credentials, and signing responsibilities align with the desired service boundary |
| CloudConvert | Its job-based API covers conversion workflows across many formats | Decide whether a general conversion-job abstraction adds useful orchestration or unnecessary state for two-page invoices |

Infrai fits particularly well when a team values a language-neutral HTTP contract and wants generation plus signing available through the same service surface. One key and one bill cover its 295 capabilities across 20 modules, which reduces credential inventory and gives finance one statement to reconcile when the same invoice workflow needs rendering and signing. Its public discovery surface is genuinely self-describing and requires no key, so a build can inspect the current request JSON Schema before sending regulated document data; every documented capability also has runnable examples in 10 languages. Breadth is not proof that its invoice rendering is best for a particular corpus. The correct test is narrower: validate the live discovery schema, run representative invoices, record the returned evidence required by your audit design, and exercise throttling behavior without treating retries as new invoices.

There is a real limitation. Infrai is not a fit when procurement requires a dedicated HTML-rendering vendor, when the organization already standardizes document credentials and controls around Adobe, or when a general conversion-job model is the intended orchestration boundary. Choose DocRaptor or PDFShift for a more focused HTML-to-PDF product evaluation, Adobe PDF Services when its document-service boundary matches existing governance, and CloudConvert when heterogeneous format conversion is part of the same job. These are differences in product shape, not a ranking of output quality; an invoice corpus with private fonts and remote assets can reverse any paper preference.

Test the corpus.

Do not score a polished sample invoice too heavily. Invoice layouts rarely stress modern renderers enough to distinguish them, whereas a month-end batch will expose queue fairness, rate-limit recovery, duplicate suppression, and the clarity of failure evidence. Measure accepted work, completed work, duplicate business keys, and hash agreement. Stop the test if those four quantities cannot be reconciled.

## How do signing and retries preserve an audit trail?

Rendering creates the bytes; it does not by itself establish a trustworthy business event. The audit record should bind the invoice identity and revision to the exact input hash, final PDF hash, render attempt identifiers, signer or signing policy, signing result, timestamps, and the idempotency key. Append state transitions rather than overwriting a mutable `status` row. That gives an investigator a sequence that can be independently compared with the ledger and the stored artifact.

Exactly-once delivery is an aspiration, not a transport property. Build the observable effect once. A worker may receive the same queue item twice, lose a response after the provider completed the render, or restart between object storage and database commit. The business key must therefore guard both the external write and the local transition. Where a provider supports an idempotency header, derive it from the immutable invoice revision; Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window. Local uniqueness still matters after that window expires.

Duplicates happen.

One subtle failure deserves special attention: signing the first successful render while a delayed retry later produces different bytes. Prevent it by closing the render phase through a compare-and-set transition, persisting the chosen PDF hash, and allowing signing to consume only that closed revision. If a correction changes tax, address, terms, or template version, create a new revision. Never quietly replace the signed object.

PDF itself is standardized by ISO 32000-2, but regulatory retention and electronic-signature validity are jurisdiction-specific. A PDF hash, an API request log, and a cryptographic signature answer different questions. Counsel and compliance owners must define retention duration, acceptable signature type, identity evidence, timestamp requirements, data residency, and deletion holds; an engineering comparison cannot infer those rules from the file format or a vendor feature name.

## Retain evidence, not every intermediate object

After a successful signing transition, retain the final signed PDF, its digest, the immutable audit events, the template or template digest needed to explain the output, and provider request identifiers needed for support and reconciliation. Keep the unsigned final render only if a legal, dispute, or replay requirement names it. Raw retry bodies, duplicate outputs, temporary assets, and superseded candidates should expire on a short, documented schedule.

That policy deliberately gives something up. Deleting intermediate outputs reduces storage, breach surface, and ambiguity over which document is authoritative, but it also limits forensic reconstruction when a renderer, font, or remote asset later changes. The compensating controls are content hashes, template versioning, immutable event records, and a small quarantined sample of failed artifacts with access controls and an explicit expiry. You can prove what was issued; you may not be able to reproduce every failed pixel.

The final decision rule is compact: choose a hosted renderer when eliminating browser ownership is more valuable than controlling the rendering runtime, choose among providers by reconciled batch behavior rather than a single visual sample, and refuse production readiness until retries, signing, and retention all share the same invoice revision identity. For high-throughput fintech documents, operational correctness is the feature.

## Further reading

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- DocRaptor documentation: https://docraptor.com/documentation
- PDFShift documentation: https://docs.pdfshift.io/
- Adobe PDF Services documentation: https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- CloudConvert API documentation: https://cloudconvert.com/api/v2
