# Powerfarm Heartime

Heartime keeps **temporal obligations and temporal evidence**. It establishes that something is due and guarantees that time cannot silently abandon a Powerfarm responsibility:

> Every active durable obligation is terminal, currently owed a return with a durable review deadline, or has a durable future temporal evaluation.

Heartime is not a task manager, planner, agent runtime, workflow engine, world observer, Registry copy, event bus, reconciliation engine or notification system. It never decides what work to do and never executes a graph. Each occurrence names the executable relationship that receives it; that relationship resolves authority, claims execution and reports back.

Specification: [`specs/HEARTIME_CONTRACT_v0.md`](https://github.com/powerfarm/powerfarm-specs/blob/main/specs/HEARTIME_CONTRACT_v0.md) in `powerfarm-specs`, schema `schemas/heartime-contract-v0.schema.json`, conformance cases `HEART-001`–`HEART-009`.

## Model

A `HeartimeContract` generation declares one obligation of an existing responsibility contract: an anchored recurrence, grace, catch-up and overlap policies, a return review interval, the handoff relationship, planning coverage with its planning relationship, and a fallback relationship with its contractual mode.

Heartime materializes **occurrences**. An occurrence's identity is

```text
sha256(RFC8785([contractId, generation, obligationId, kind, nominalUTC]))
```

so the same nominal instant keeps the same identity across restarts and receivers can deduplicate by it.

| Kind | Emitted when |
| --- | --- |
| `work` | a recurrence instant is due inside planning coverage |
| `planning-review` | the next period must be prepared before coverage ends |
| `fallback` | coverage ended without renewal; carries the contractual fallback mode |
| `return-review/<parent>` | an unresolved occurrence reached its return deadline, or reported failure or uncertainty |

| Disposition | Meaning |
| --- | --- |
| `pending` | due; delivery not yet acknowledged |
| `acknowledged` | the receiver acknowledged delivery; the effect is still unknown |
| `verified` / `failed` / `uncertain` | execution feedback bound to an immutable evidence digest |
| `contained` | failure effects were independently bounded; never success |
| `skipped` | due outside grace, coverage or expiry; recorded, not delivered |
| `coalesced` / `superseded` / `lapsed` | undelivered and replaced by newer work, a newer generation, or the end of coverage, expiry or retirement |

## Guarantees

- **Persistence before delivery.** One `BEGIN IMMEDIATE` transaction (WAL, `synchronous=FULL`) stores the occurrence, its temporal evidence, its return deadline, its delivery intent and every advanced deadline. A delivery attempt is recorded before any byte is sent, so after a crash the occurrence counts as possibly delivered.
- **Acknowledgement is not verification.** A 2xx response records delivery only. Only `report` with an explicit outcome and a `sha256:` evidence digest changes execution certainty; free text is refused.
- **Missed time follows the contract.** `all` keeps every instant in bounded batches, `latest` coalesces them into one occurrence covering the interval, `skip` records the missed interval and emits only within grace.
- **Overlap follows the contract.** `defer` and `coalesce` hold new work while delivered work of the same contract (any generation) is unresolved; `coalesce` delivers only the newest undelivered occurrence; `allow` leaves concurrency to the executable relationship.
- **Planning continuity.** A planning review is always armed before coverage ends. A renewal must reference an immutable plan, extend coverage and place the next review strictly inside it. A missed renewal lapses undelivered work and emits the fallback, dated at the end of coverage, and keeps re-emitting it. There is no implicit fallback to a human.
- **Returns stay owed.** Pause, supersession, expiry and retirement never cancel a return review. While a return review waits for its first delivery attempt, later deadlines of the same parent add nothing, so an unreachable receiver cannot grow the ledger without bound.
- **Clock regression is explicit.** Evaluating earlier than persisted evidence fails with `ErrClock`; the cursor never moves backwards. Evidence records the clock source (`system-utc` or `operator-supplied-utc`).
- **Restart answers.** `account` reports what was due, what was emitted, what may have executed, what is unresolved, what happens next, the planning coverage, and any uncovered obligation (which must be empty).

## Usage

```bash
go build -o heartime ./cmd/heartime
```

```bash
heartime -db heartime.db import contract.json
```

```bash
heartime -db heartime.db evaluate
```

```bash
heartime -db heartime.db serve https://receiver.example/heartime /path/to/receiver-token
```

Every command opens the ledger, performs one durable operation and exits, so each invocation is also a restart. `-at` supplies an explicit UTC instant for tests and accelerated demonstrations; it is recorded as `operator-supplied-utc` and `serve` refuses it. Run `heartime` without arguments for the full command list.

`serve` posts each due occurrence's evidence as JSON with `Authorization: Bearer <token>` and `Idempotency-Key: <occurrence id>`, retrying refused deliveries with backoff from 5 s to 300 s. A receiver must deduplicate by occurrence id before claiming execution and must report outcomes with evidence.

## Conformance

| Case | Tests |
| --- | --- |
| HEART-001 planning rollover | `TestPlanningRolloverArmsTheNextPeriodBeforeExpiry`, `TestPlanningDeadlineCannotMovePastCoverage` |
| HEART-002 failed planning | `TestMissedRenewalFallsBackAndLapsesUndeliveredWork`, `TestFailedPlanningReturnInvokesFallback` |
| HEART-003 kill after commit | `TestKilledProcessLeavesOneOccurrenceAndAnAccount/committed`, `TestConcurrentEvaluatorsDoNotDuplicate` |
| HEART-004 catch-up | `TestCatchUpPolicies` |
| HEART-005 overlap | `TestOverlapPolicies`, `TestCoalesceDeliversOnlyTheNewestUndeliveredWork`, `TestContainmentReleasesOverlapWithoutInventingSuccess` |
| HEART-006 acknowledgement is not verification | `TestAcknowledgementIsNotVerification`, `TestRelayAcknowledgesWithoutVerifying`, `TestReportRequiresDeliveryAndImmutableEvidence` |
| HEART-007 pause and supersession | `TestPauseDefersWorkButNotReturnsOrPlanning`, `TestSupersessionPreservesHistoryAndOwedReturns` |
| HEART-008 restart account | `TestKilledProcessLeavesOneOccurrenceAndAnAccount/attempted`, `TestLivenessInvariantAcrossLifecycle` |
| HEART-009 bounded, lapsing obligations | `TestReturnReviewsStayBoundedWhileUndelivered`, `TestExpiryAndRetirementLapseUndeliveredWork`, `TestRelayKeepsRefusedDeliveryPending` |

The kill tests start a real child process on a real SQLite file and terminate it with SIGKILL.

## Verification

The [Go implementation profile](https://github.com/powerfarm/powerfarm-specs/blob/main/profiles/GO.md) applies. CI runs exactly these checks:

```bash
test -z "$(gofmt -l .)"
```

```bash
go vet ./...
```

```bash
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
```

```bash
go test -race -count=1 ./...
```

The SQLite driver uses cgo, so a C compiler is required.

## Limits of this implementation

- `import` is a trusted operator cache of exact terms. Heartime does not yet resolve contracts from an authenticated Registry and must not be presented as autonomous institutional admission.
- Recurrence is anchored fixed-second recurrence. Civil calendars, time zones and restart-spanning hold predicates need an explicit later profile.
- Execution feedback and planning renewal are accepted through the local CLI or library. There is no authenticated network API for reports yet.
- A single local SQLite ledger per deployment. Heartime cannot certify its own availability: detecting the complete loss of Heartime needs an independent watchdog.
