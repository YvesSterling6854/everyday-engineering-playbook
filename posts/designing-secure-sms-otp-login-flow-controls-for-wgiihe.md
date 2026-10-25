# Designing Secure SMS OTP Login Flow Controls for Commerce Report Delivery

Design a secure SMS OTP login flow around the commerce report's authorization boundary, then send the generated report through a durable job, bind every delivery attempt to an immutable report version, and treat the attachment as a security boundary rather than a convenient byte array. That minimizes integration effort without sacrificing reconciliation: a worker renders the report once, records its digest, asks a standards-based mail adapter to deliver it, and makes retries safe by keying them to `(report_id, version, recipient)`. The login proves who may request or retrieve the report; it does not prove that an email was delivered.

The detail that changes the design is the report's sensitivity. An order-reconciliation export can contain names, addresses, transaction references, or operational totals, so an email attachment is a copy of business data with its own retention and audit obligations. If the recipient must authenticate before opening or retrieving the report, use a short-lived, single-use access mechanism and record the authentication event separately from delivery. SMS OTP can be one part of that boundary, but it should never be confused with proof that an email was delivered.

The attachment is a second copy of the data.

## How should a secure SMS OTP login flow protect a commerce report?

This is an architecture decision record, not a mail API recipe. The invariants come first.

* A report version is immutable after its digest is recorded. Re-running a query may produce a new version; it must not silently mutate the attachment associated with an old attempt.
* Each accepted delivery request has one durable identity. A timeout after submission is an unknown outcome, not permission to create a second logical delivery.
* The audit trail records intent, preparation, submission, provider response metadata, and final observation as different facts. One `sent` boolean is not reconciliation.
* Secrets, OTP values, attachment contents, and full recipient addresses stay out of ordinary logs.
* Retention, regional processing, and access rules are policy inputs. They are not properties that can be inferred from an SMTP response.

The practical boundary is between preparation and submission. Preparation can be retried freely if it is content-addressed. Submission needs an idempotency record and a clear interpretation of the adapter's acknowledgement. An SMTP `250` response indicates acceptance at that protocol hop; it does not establish that the recipient opened the message. RFC 5321 describes the response semantics, while RFC 6376 describes DKIM signing, which can help protect message-domain integrity but does not turn delivery into a read receipt.

The broadest design is also the wrong one here: an attachment cannot be revoked after a recipient downloads or forwards it. That limitation makes authenticated retrieval the better choice when post-dispatch control is a requirement.

| Option | Integration effort | Failure boundary | Audit consequence | Appropriate use |
| --- | --- | --- | --- | --- |
| Synchronous request sends the attachment | Low at first | Web request and mail submission share a timeout | Ambiguous retries and weak operator visibility | Small, low-sensitivity notices with no retry requirement |
| Queue plus worker and durable delivery record | Moderate | Preparation, submission, and observation are distinct | Each state transition can be reconciled | Scheduled or user-requested commerce reports |
| Secure retrieval link instead of attachment | Higher product work | Delivery carries a pointer; authorization happens at retrieval | Access can be logged, revoked, and expired | Large or sensitive reports with repeat access |

I would choose the queue and worker for the stated job, then use retrieval for files that exceed an agreed attachment limit or require revocation after dispatch. The choice is about integration effort in the whole system, not the number of lines needed to call a mail endpoint. A synchronous shortcut moves complexity into duplicate messages, request timeouts, and support investigations.

## Where should authentication sit in a report email flow?

A recipient who requests a report is not automatically an authenticated reader of its contents. Those are separate claims. A common flow is: create the report request under an authenticated principal, render and encrypt or attach the immutable output, submit the message, then require authentication before a protected retrieval endpoint releases a copy. If the business requirement truly demands an attachment that opens without a network call, authorization must be decided before dispatch and the organization must accept that the attachment can be copied after delivery.

For SMS OTP login, the server should create a random, high-entropy challenge, store only a verifier or protected representation, bind the challenge to the account and intended operation, and enforce an expiry. Verification consumes the challenge atomically. A successful verification cannot be replayed against another report or another session. NIST SP 800-63B treats OTP authenticators as replay-resistant only when the verifier rejects a code that has already been accepted; an SMS channel also carries risks outside the application, so it should not be treated as equivalent to a phishing-resistant authenticator.

Rate limits need more than one counter. Use an account or destination limit to resist guessing, an IP or network signal to resist concentrated abuse, and a request-generation limit to control message cost and harassment. The counters should be time-bounded, observable, and designed so that a failed attempt cannot distinguish too much account state. Lockout is a policy decision with a recovery path; an indefinite lock is an availability attack.

The retry record should contain the challenge identifier, attempt count, expiry, and outcome class, not the OTP itself. This data model keeps the business event and security event legible:

```go
type DeliveryAttempt struct {
	ID             string
	ReportID       string
	ReportVersion  int64
	RecipientKey   string
	ContentDigest  string
	IdempotencyKey string
	State          string
	CreatedAt      time.Time
	SubmittedAt    *time.Time
	ObservedAt     *time.Time
}

func deliveryKey(reportID string, version int64, recipientKey string) string {
	return reportID + ":" + strconv.FormatInt(version, 10) + ":" + recipientKey
}
```

The difficult part is the transaction around it: insert the report version, insert the delivery attempt with a uniqueness constraint on the delivery key, and enqueue work only after the durable state is committed. An outbox table is a reasonable implementation of that handoff.

## The critical path: render once, submit with evidence

The worker should load the immutable version, validate that the recipient and policy still permit dispatch, and build the message from bounded inputs. Do not let a retry re-run a live order query and generate a different attachment under the same logical delivery. If source data has changed, create a new report version and make that change visible.

```go
type MailAdapter interface {
	Submit(ctx context.Context, msg Message) (Submission, error)
}

func processDelivery(ctx context.Context, store Store, mail MailAdapter, attemptID string) error {
	attempt, err := store.LockAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	if attempt.State == "submitted" || attempt.State == "observed" {
		return nil
	}
	version, err := store.LoadReportVersion(ctx, attempt.ReportID, attempt.ReportVersion)
	if err != nil {
		return err
	}
	msg, err := BuildMessage(version, attempt.RecipientKey)
	if err != nil {
		return err
	}
	submission, err := mail.Submit(ctx, msg)
	if err != nil {
		return classifySubmissionFailure(ctx, store, attempt.ID, err)
	}
	return store.MarkSubmitted(ctx, attempt.ID, submission.Reference, submission.AcceptedAt)
}
```

The adapter contract must state what happens when the process loses its connection after the remote system has accepted the message. The worker cannot infer that outcome from a local timeout. A durable submission reference, an idempotent remote operation where available, or an operator reconciliation procedure is required. Blindly retrying is how a monthly report becomes two monthly reports.

In design reviews, I put the unknown outcome on the whiteboard before discussing throughput. It forces the team to answer the question that a happy-path demo avoids: can we prove that the second attempt is a retry of the same logical operation? If not, the system needs a reconciliation state, not a more aggressive retry loop.

## What belongs in the audit trail?

A useful event sequence might be `requested`, `rendered`, `queued`, `submission_accepted`, `submission_rejected`, `delivery_observed`, `access_granted`, and `expired`. These are domain events, not a list of API routes. Each event should include a stable event ID, the report version, a policy version, an actor or service identity, and a timestamp from a trusted server clock. The attachment digest lets an investigator establish which bytes were intended without placing the bytes in the log.

One short event is enough to change an investigation.

Keep security and delivery evidence correlated but separable. An OTP challenge accepted at 14:03:12 UTC can authorize a retrieval request; it does not prove that the report email was accepted at 14:03:13 UTC. Likewise, an email provider's acceptance reference does not prove that the OTP was generated for the same operation unless the application binds both records to the same authorization context.

For US and EU SaaS deployments, document the processing region, subprocessors, retention period, and deletion behavior for both the report artifact and message metadata. Legal requirements vary by data category and jurisdiction, so the engineering record should identify the applicable counsel-approved policy rather than make a universal compliance claim. Encryption in transit and at rest is necessary hygiene; it is not a substitute for access control, retention limits, or auditability.

Observability should expose rates and states, not secrets. Track render duration, queue age, submission outcomes by normalized class, retry count, attachment size buckets, and the age of unresolved unknown outcomes. Alert on a growing reconciliation queue and on repeated OTP failures by policy dimensions. Avoid logging message bodies, OTPs, authorization headers, or raw phone numbers.

## The rejected option and when it is valid

The rejected option is a direct HTTP request that renders the report and submits the attachment before returning success. It has attractive integration economics: no worker deployment, no queue, and a short call path. It is valid for an internal, low-sensitivity notification where duplicate delivery is harmless, the report can be regenerated, and the caller can tolerate an ambiguous response.

It is a poor default for customer-facing commerce reports. A browser disconnect can leave the operator unable to distinguish “not submitted” from “accepted remotely.” A second click can create a duplicate. A renderer that reads mutable orders can produce two different files with the same business label. Those are correctness defects, not merely operational inconveniences.

A secure retrieval link is the better boundary when reports are large, sensitive, or likely to be opened repeatedly. It increases product work: authorization, expiry, revocation, download streaming, and regional storage all need decisions. It also gives the team a cleaner way to record access and respond to a permission change. The integration cost is real, but so is the cost of explaining an attachment that cannot be revoked.

The decision rule is narrow: use queue-backed attachment delivery for bounded, immutable reports; use authenticated retrieval when control after dispatch matters; and use synchronous delivery only when duplicate and ambiguous outcomes have been explicitly accepted. Preserve the version digest and delivery identity in every case. That is the smallest durable design that lets finance, support, security, and engineering tell the same story about what happened.

## References

- RFC 5321, Simple Mail Transfer Protocol: https://datatracker.ietf.org/doc/html/rfc5321
- RFC 6376, DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
