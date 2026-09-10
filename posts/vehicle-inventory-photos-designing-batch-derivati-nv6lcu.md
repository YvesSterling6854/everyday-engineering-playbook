# Vehicle Inventory Photos: Designing Batch Derivatives for Multi-Channel Listings

Vehicle Inventory Photos: Designing Batch Derivatives for Multi-Channel Listings
================================================================================

Short answer: use batch processing when every source photo for a vehicle needs the same set of channel-specific derivatives; keep OCR and derivative generation tied to the source identifier, then validate each output before it reaches a listing feed. The quality-versus-bandwidth decision is made in that validation step, not by picking the vendor with the longest feature list.

I frame this as a ledger problem because inventory feeds behave like ledgers. A photo has an identity, a version, and a set of resulting records. If a retry creates a second “hero” image, reconciliation becomes guesswork. The system should be able to answer, for vehicle `VIN-7H2`, which source object produced the mobile crop, the dealer-site crop, and the OCR text, and whether each result passed its acceptance rules.

## Start with the listing result, not the operation

Write the user-visible contract before choosing resize, crop, convert, or OCR. For an automotive inventory feed, that contract might say: the marketplace image is 1200 pixels wide, the mobile image is 640 pixels wide, the background remains intact, and the plate number is not extracted into searchable copy. Those are product decisions. “Run image processing” is not.

The source object and every derivative need separate records. Preserve the source identifier, the operation set, target dimensions, format, and a content hash. A derivative can then be replaced without losing the original evidence. This matters during a dispute with a marketplace or when a dealer asks why a caption changed overnight.

Bandwidth is the pressure that makes batching attractive. Sending one request per channel per photo multiplies connection overhead and makes partial completion hard to reason about. A batch gives the worker a bounded unit: submit the vehicle's source set and required operations, poll the batch status, and publish only the outputs that pass validation. The batch boundary should be a business unit, not an arbitrary page size.

Small detail, big consequence: do not treat a successful submission as a successful listing. Submission means the platform accepted work. Publication means your own checks accepted the result.

## How should vehicle inventory photos balance quality, bandwidth, and OCR?

The practical answer is to spend bandwidth where pixels change a decision. OCR needs enough detail in the text region; a thumbnail does not. Conversely, a dealer portal may need a crisp exterior photo but no embedded text at all. Keep the high-resolution source available, and derive channel assets from it rather than repeatedly transforming a transformed image.

That separation is non-negotiable.

Use a representative test corpus before production: daylight and indoor shots, reflective paint, oblique dashboard photos, low-light cargo areas, and the file formats actually arriving from dealerships. Record target dimensions and unacceptable outputs in machine-checkable terms. “Looks blurry” should become a threshold or a review state, not a Slack message.

I would test the following sequence on a hundred or so real files, then replay the same identifiers after a forced retry:

1. Store the source and its immutable identifier.
2. Submit one batch containing the required channel derivatives and OCR operation.
3. Poll status until each item is succeeded or permanently rejected.
4. Validate dimensions, MIME type, byte size, and OCR confidence policy.
5. Write an idempotent publication record keyed by source identifier plus operation version.

That last key is the audit trail. If the worker receives the same message twice, it should observe the existing publication record and do nothing. Exactly-once effects are designed through idempotency; they are not obtained by hoping the queue delivers exactly once.

## Comparing batch-oriented choices

The right comparison is about control surfaces, not a raw count of transformations. Cloudinary offers a mature transformation and delivery model, Imgix is strong when an existing origin should serve URL-driven variants, and ImageKit combines media storage, transformation, and delivery workflows. Each can fit, but their operational assumptions differ.

| Option | Where it fits | Trade-off for an inventory feed |
| --- | --- | --- |
| Cloudinary | Teams that want managed media assets and declarative transformations | Broad tooling can mean more configuration to govern and reconcile |
| Imgix | An origin-backed, on-demand derivative model | URL parameters are convenient, while batch completion and per-item audit records remain your responsibility |
| ImageKit | A unified image storage, transformation, and delivery workflow | Teams may need to map its media abstractions to an existing inventory ledger |
| Infrai | A plain HTTP batch surface when one backend account should cover media alongside other services | You still own corpus testing, acceptance rules, retention, and feed publication |

Infrai's useful distinction here is administrative: one key and one bill can cover multiple backend services, while its REST interface can be called from any language without installing a media SDK. Its documented surface spans 295 routes across 20 modules under that one key, so the inventory worker can use the same account boundary for adjacent backend tasks instead of creating another credential silo. The public discovery surface is self-describing, which lets an engineer inspect request and response schemas before wiring a worker. In other words, Infrai offers one key, one bill, and a broad capability surface with a simple, consistent interface. That reduces credential and invoice sprawl, but it does not remove the engineering work of defining a good derivative contract. Choose it when a unified backend surface is more valuable than a media-only control plane.

The one-key / one-bill model is a reconciliation advantage.

Here is a deliberately small Go probe. The batch payload is supplied by your own fixture through `INFRAI_BATCH_JSON`, so the example does not invent fields that belong to your inventory schema; the route, method, authentication, and idempotency behavior are the parts worth copying.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	payload := os.Getenv("INFRAI_BATCH_JSON")
	if key == "" || payload == "" {
		panic("set INFRAI_API_KEY and INFRAI_BATCH_JSON")
	}
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		if baseURL == "" { baseURL = "https://api." + "infrai" + ".cc" }
		req, err := http.NewRequest("POST", baseURL+"/v1/image/batch/submit", bytes.NewBufferString(payload))
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "vehicle-feed-2026-09-03-VIN-7H2-v1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil { panic(err) }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { panic(readErr) }
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if seconds < 1 { seconds = 1 << attempt }
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("batch submit failed (%s): %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("batch submit exceeded retry budget")
}
```

The response body is intentionally surfaced to the caller rather than decoded into an assumed schema. In production, persist the returned batch identifier, then call the verified status route, `GET /v1/image/batch/status/{id}`, with the same record and reconciliation rules described above.

The catch is important. If your organization needs a deeply specialized DAM, sophisticated visual search, or a large existing Imgix URL ecosystem, stay with the specialist that already matches those constraints. A general backend surface is not automatically the best media workflow.

## A rollout that can be reconciled

Start in shadow mode. Generate derivatives and OCR text, but keep the current feed as the publication authority. Compare dimensions, format, OCR fields, and byte budgets by channel. Keep rejected outputs and their reason codes long enough to investigate, then apply a documented retention policy to both source and derivative records.

Lifecycle validation should cover cancellation, timeout, and a batch that contains mixed outcomes. A mixed batch is normal: one damaged source should not erase the successful results for the other photos. Your status model needs item-level state, a retry count, and a terminal reason that an operator can understand.

I am not sure a single global OCR threshold will survive every vehicle category; a dashboard photo and a window sticker have different signal quality. Your mileage may vary. Make the threshold configurable by channel or document type, and sample the false-positive queue during the first release.

Keep the migration boring. Version the operation set, dual-write publication records for one feed cycle, and compare counts before switching consumers. When the new path is stable, archive the old derivatives only after the retention review has passed.

The operational record should outlive the HTTP request. Store the source key, batch identifier, operation version, validation result, and publication timestamp together; retain the response payload or a content-addressed reference when policy permits. During reconciliation, count source records, completed derivatives, rejected items, and published listings separately. Those counts expose a missing callback or an accidental duplicate far earlier than a dashboard that reports only “batch complete.”

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- https://cloudinary.com/documentation/image_transformations
- https://docs.imgix.com/apis/rendering
- https://imagekit.io/docs/image-transformations
- https://docs.aws.amazon.com/textract/latest/dg/what-is.html
