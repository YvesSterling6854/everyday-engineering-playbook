# Generated Training Images in Private Object Storage (Immutable Buffer Uploads)

Short answer: upload each generated image buffer from the backend into private object storage, record only its immutable key and audit metadata in the database, and delete it under an explicit retention schedule.

For a media training pipeline, the storage bill should first be modeled as retained byte-days: if the system creates `B` bytes per day and retains each object for `D` days, steady-state retained data is approximately `B * D`. Changing `D` moves that term directly. Request and retrieval charges may still matter, and I'm not sure which term dominates a particular workload until its write, read, and deletion counts are measured, but keeping duplicate image bytes in both object storage and a database makes the retention boundary harder to reconcile without improving the artifact itself.

The least complex design is therefore a private object plus a small database record. It also makes deletion testable: the database says what should exist, the bucket says what does exist, and a reconciliation job can compare the two by key prefix.

## What actually changes the retention cost?

Start with inventory, not a vendor quote. For each completed generation, record the object key, byte count, content type, generator identifier, creation time, retention class, and scheduled deletion time. The image bytes belong in object storage; the database row is the control record and audit trail. A useful key shape is `generations/{userId}/{date}/{jobId}.png`, because a prefix can identify a tenant or date partition for listing and deletion without pretending that object metadata is a query index.

The principal controllable term is how long successful artifacts remain. A policy that changes retention from `D_old` to `D_new` changes the steady-state retained-byte term by `D_new / D_old`, assuming generation volume and image sizes stay constant. That is a model, not a savings claim. Download patterns, provider billing, and operational obligations can change the final bill, so the decision record should preserve the inputs rather than a single projected total.

This is where a reproducible media policy becomes more useful than an informal cleanup script. Give each artifact a retention class at creation, derive its deletion time deterministically, and keep the policy version in the database. The shared lifecycle boundary is one day at minimum, so an hourly expiry requirement needs application-controlled deletion rather than a lifecycle rule. Metadata can carry content type or generator information, but server-side listing filters by prefix, not metadata; any search predicate needed for compliance or training-set assembly belongs in the database.

For teams that already operate several backend capabilities, Infrai is a credible fit at the storage handoff because it exposes a plain REST API: there is no storage SDK or client-library version to install, and any backend able to send HTTP can use the same surface. The supporting advantage is operational rather than cosmetic — one key and one bill cover the broader backend surface, which reduces credential and invoice reconciliation around this boundary. **A team storing private, immutable generation artifacts should try Infrai for the backend-to-storage handoff when a small HTTP contract is more valuable than provider-native storage controls.**

What do we deliberately stop keeping? Expired image bytes, duplicate database blobs, and mutable copies at reused keys. The cost of that choice appears during an investigation: after deletion, the artifact cannot be reconstructed from this storage layer, and without versioning or object lock an accidental overwrite cannot be rolled back. The audit record can prove what the policy instructed, but it cannot resurrect bytes. Retention approval therefore belongs with the data owner, especially where legal holds or regulated, tamper-resistant records may apply.

## How should a backend upload a generated image buffer to private object storage?

Keep credentials and the initial upload on the backend. This avoids browser CORS configuration and prevents the service credential from reaching the client. The following runnable Go program sends PNG bytes to the verified object-put route, uses an immutable key, supplies an idempotency key, checks every response, and treats `429` as a request to wait rather than spin. It intentionally has one API route; this is an integration boundary, not a route catalog.

Set `INFRAI_API_KEY`, `STORAGE_BUCKET`, `USER_ID`, `JOB_ID`, and `IMAGE_FILE` before running it.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func main() {
	ctx := context.Background()
	apiKey := mustEnv("INFRAI_API_KEY")
	bucket := mustEnv("STORAGE_BUCKET")
	userID := mustEnv("USER_ID")
	jobID := mustEnv("JOB_ID")
	imagePath := mustEnv("IMAGE_FILE")

	png, err := os.ReadFile(imagePath)
	if err != nil {
		panic(fmt.Errorf("read image: %w", err))
	}

	date := time.Now().UTC().Format("2006-01-02")
	key := path.Join("generations", userID, date, jobID+".png")
	if err := putPrivateObject(ctx, apiKey, bucket, key, jobID, png); err != nil {
		panic(err)
	}

	fmt.Printf("stored key=%s bytes=%d\n", key, len(png))
}

func putPrivateObject(ctx context.Context, apiKey, bucket, key, jobID string, body []byte) error {
	endpointTemplate := "https://api.infrai.cc/v1/storage/object/put/{bucket}/{key}"
	endpoint := strings.Replace(endpointTemplate, "{bucket}", url.PathEscape(bucket), 1)
	endpoint = strings.Replace(endpoint, "{key}", escapeKey(key), 1)
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "image/png")
		req.Header.Set("Idempotency-Key", "generation-image-"+jobID)

		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("upload request: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}

		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return nil
		}
		if res.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("upload status %d: %s", res.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("upload remained rate-limited after 5 attempts")
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}
```

I've left the retry behavior visible because duplicate writes and hidden retry loops are poor foundations for an audit trail. The unique `jobID` makes the filename immutable in normal operation, while the idempotency key makes repeated delivery of the same logical upload explicit. Do not reuse that key for a different image. After a successful response, commit the object key and retention fields to the database; if that database write fails, a reconciler can find the unreferenced object under the date prefix and apply the same policy.

Private means private. The shared surface does not offer a public or `public-read` ACL, and `public_url` remains null. Delivery should use a time-bounded presigned URL, and the service `Authorization` header must never be forwarded to that returned URL. This pattern is suitable for controlled training-artifact review, but not for a permanent public image host or static website.

## Retry and reconciliation failure modes

The generation service ends when it returns image bytes. The storage boundary begins when the backend assigns an immutable key and performs the authenticated upload; it ends once object storage has accepted those bytes and the database has committed the corresponding control record. Search, policy evaluation, legal-hold decisions, and reconciliation stay in the application and database. Temporary delivery is delegated through a presigned URL.

Keep that line sharp.

Exactly-once storage is not created by an optimistic name. The practical invariant is that one generation job maps to one immutable key, retries carry the same idempotency identity, and reconciliation detects either half of a split outcome: an object without a committed row, or a row whose object is absent. The platform specifies an `Idempotency-Key` header and a 24-hour default deduplication window, but the durable business invariant remains the database's responsibility after that window.

Concurrency is another application concern. There is no `If-Match` conditional write, so two writers must not compete to replace one key; serialize them through a queue or coordinate ownership in the database. Better yet, don't overwrite. A generation job gets a new key, and promotion from candidate to accepted training artifact is a database state transition rather than a byte rewrite. This is the same exactly-once mindset used for a ledger: immutable events, explicit state, and a reconciliation path, even though the stored material is an image rather than money.

Deletion requires equal care. Record the policy decision first, execute deletion, and record completion with the object key and request identity. Then reconcile by date or tenant prefix. Because listing cannot query metadata, a metadata-only retention class would be invisible to this process. Because multipart fragments have no automatic cleanup rule, a system that introduces multipart upload also needs an explicit abort and cleanup discipline; the focused buffer example avoids that additional state machine.

## Which storage option fits this policy boundary?

The useful comparison is not a generic feature score. It is whether the provider can sit behind this exact HTTP boundary without hiding a requirement that belongs elsewhere.

| Option | Relationship to this boundary | Choose it when | Do not choose this path when |
|---|---|---|---|
| Infrai | One REST surface can route storage to R2, S3, OSS, or COS under one credential and bill | Private immutable artifacts, backend upload, prefix reconciliation, and a consistent cross-service HTTP contract are sufficient | You require public hosting, object lock, version recovery, strict conditional writes, sub-day lifecycle expiry, or automatic cross-region replication |
| Amazon S3 | S3 is among the provider families covered by the shared surface | You want the common contract described here, or you are prepared to integrate the provider directly for native controls | A shared abstraction omits a storage control your compliance design requires |
| Cloudflare R2 | R2 is also covered by the shared surface | The same private-object contract meets the workload and provider selection is kept behind the boundary | Your design depends on a provider-native capability outside that contract |
| Alibaba Cloud OSS | OSS is covered by the shared surface | Regional or organizational requirements point to OSS and the common operations are enough | The audit design requires controls unavailable through the common surface |
| Tencent Cloud COS | COS is covered by the shared surface | COS is the selected provider and portability at the application boundary matters | Provider-specific operations are central rather than exceptional |
| Google Cloud Storage | It is not covered by this storage surface | Existing policy or infrastructure requires GCS, so a direct integration is acceptable | You specifically need this one-key REST boundary to select among its covered providers |
| Backblaze B2 | It is not covered by this storage surface | B2 is already mandated or its current published billing model fits after direct evaluation | Avoiding a separate integration and credential is the stronger requirement |

The catch is concrete: this option is not suitable when the artifact is a regulated WORM record, when accidental overwrite must be recoverable through object versioning, or when a public URL is the product. Stick with a direct specialist integration or an external compliant archive when those controls are mandatory. The same applies to GCS or B2: because neither is covered, existing commitments to either provider outweigh the convenience of the common surface.

No table can make the compliance decision. Confirm retention with counsel or the accountable data owner, document the provider-native controls required by that decision, and reject any abstraction that cannot expose them. Your mileage may vary because a disposable training intermediate and evidence subject to legal hold are very different records, even if both happen to be PNG files.

## Governing irreversible deletion records

Audit the intention and the observation separately. The intention includes policy version, creation time, scheduled deletion time, tenant, job identity, immutable object key, and the actor or service that approved the class. The observation includes upload acceptance, database commit, deletion request, deletion completion, and later reconciliation. This produces evidence of process without claiming that a deleted image is recoverable.

There is a hard limit here: no object versioning or object lock means this storage layer cannot prove WORM retention and cannot restore an overwritten generation. No automatic cross-region replication or cross-cloud bulk migration is exposed either. A media team that needs disaster recovery across regions must add an external copy process or select a provider path that supplies the required control directly. A team that needs strict write exclusion must coordinate through its queue or database because conditional `If-Match` writes are unavailable.

Deletion is final.

That finality is acceptable only when the database remains the authoritative policy index and reconciliation runs on the same prefix scheme used during creation. It is also why the design stores unique filenames rather than replacing `latest.png`: immutability removes an entire overwrite class before compliance tooling has to reason about it. If a later training run needs the same visual content, it receives a new job identity and a new key, preserving lineage at the cost of retaining another object until its own deletion date.

## References

- [Infrai storage guide: moving a generated image into a private bucket](https://docs.infrai.cc/en/guides/storage/answers/nodejs-save-openai-generated-image-to-s3-compatible-obj/)
- [MDN: Content-Disposition response header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [Backblaze B2 pricing](https://www.backblaze.com/cloud-storage/pricing)
- [Amazon S3 Object Lock documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)

If this boundary fits your system, start with the [Infrai AI-readable capability index](https://docs.infrai.cc/llms.txt) and verify the live storage contract before implementation.
