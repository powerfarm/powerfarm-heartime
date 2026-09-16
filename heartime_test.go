package heartime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// base is a fixed logical instant; tests control time explicitly.
const base int64 = 1800000000

var (
	workRef     = Ref{ID: "pf.contract.exec.build", Generation: 1}
	recoveryRef = Ref{ID: "pf.contract.exec.recover", Generation: 1}
	planningRef = Ref{ID: "pf.contract.exec.plan", Generation: 1}
)

// fixture: work every 10 s from base, return review after 20 s, fallback review
// every 10 s, coverage until base+100 with its planning review at base+70.
func fixture() Contract {
	return Contract{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{ID: "pf.contract.heartime.test", Generation: 1, Subject: "pf.powerfarm", Owner: "pf.danvoulez"},
		Spec: Terms{
			ObligationID:       "build",
			Responsibility:     workRef,
			Recurrence:         Recurrence{Anchor: formatUTC(base), EverySeconds: 10},
			GraceSeconds:       2,
			CatchUp:            CatchUpAll,
			MaxBatch:           100,
			Overlap:            OverlapDefer,
			ReviewAfterSeconds: 20,
			Handoff:            workRef,
			Fallback:           Fallback{Mode: "safe_mode", ReviewAfterSeconds: 10, Handoff: recoveryRef},
			Planning:           Planning{PlanValidThrough: formatUTC(base + 100), NextPlanningReviewAt: formatUTC(base + 70), ReviewEverySeconds: 20, Handoff: planningRef},
		},
	}
}

func openLedger(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	must(t, err)
	t.Cleanup(func() { store.Close() })
	store.ClockSource = "test-logical-time"
	return store
}

func setup(t *testing.T, contract Contract) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "heartime.db")
	store := openLedger(t, path)
	must(t, store.Install(contract, base))
	return store, path
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, store *Store, where string, args ...any) int {
	t.Helper()
	var n int
	must(t, store.db.QueryRow(`SELECT COUNT(*) FROM occurrences WHERE `+where, args...).Scan(&n))
	return n
}

func stateOf(t *testing.T, store *Store, id string) string {
	t.Helper()
	var state string
	must(t, store.db.QueryRow(`SELECT state FROM occurrences WHERE id = ?`, id).Scan(&state))
	return state
}

func occurrence(t *testing.T, contract Contract, kind string, nominal int64) string {
	t.Helper()
	id, err := OccurrenceID(contract.Ref(), contract.Spec.ObligationID, kind, nominal)
	must(t, err)
	return id
}

func evidenceOf(t *testing.T, store *Store, id string) OccurrenceEvidence {
	t.Helper()
	var body []byte
	must(t, store.db.QueryRow(`SELECT body FROM occurrences WHERE id = ?`, id).Scan(&body))
	var evidence OccurrenceEvidence
	must(t, json.Unmarshal(body, &evidence))
	return evidence
}

func deadline(account Account, subject string) (Deadline, bool) {
	for _, next := range account.Next {
		if next.Subject == subject {
			return next, true
		}
	}
	return Deadline{}, false
}

func requireCovered(t *testing.T, store *Store, now int64) Account {
	t.Helper()
	account, err := store.Account(now)
	must(t, err)
	if len(account.Uncovered) != 0 {
		t.Fatalf("liveness invariant violated: %v", account.Uncovered)
	}
	return account
}

func TestOccurrenceSurvivesRestartWithoutDuplication(t *testing.T) {
	contract := fixture()
	store, path := setup(t, contract)
	must(t, store.Evaluate(base))
	must(t, store.Close())

	restarted := openLedger(t, path)
	must(t, restarted.Evaluate(base))
	if count(t, restarted, "kind = 'work'") != 1 {
		t.Fatal("restart manufactured a second semantic occurrence")
	}
	pending, err := restarted.Pending()
	must(t, err)
	if len(pending) != 1 || pending[0].ID != occurrence(t, contract, KindWork, base) || pending[0].Attempts != 0 {
		t.Fatalf("delivery intent lost or changed: %+v", pending)
	}
	next, err := restarted.NextEvaluation(base)
	must(t, err)
	if next != base+10 {
		t.Fatalf("next evaluation %d, want %d", next, base+10)
	}
}

// HEART-003 and HEART-008: a process killed after durable commit, before or
// after recording a delivery attempt, leaves one semantic occurrence and a
// ledger that answers the five restart questions.
func TestKilledProcessLeavesOneOccurrenceAndAnAccount(t *testing.T) {
	if path := os.Getenv("HEARTIME_KILL_DB"); path != "" {
		store, err := Open(path)
		if err != nil {
			os.Exit(2)
		}
		if store.Evaluate(base) != nil {
			os.Exit(3)
		}
		if os.Getenv("HEARTIME_KILL_STAGE") == "attempted" {
			if store.Attempt(occurrence(t, fixture(), KindWork, base), base) != nil {
				os.Exit(4)
			}
		}
		if os.WriteFile(path+".ready", []byte("durable"), 0o600) != nil {
			os.Exit(5)
		}
		time.Sleep(time.Hour) // Killed here, as if the network send were in flight.
	}
	for _, stage := range []string{"committed", "attempted"} {
		t.Run(stage, func(t *testing.T) {
			contract := fixture()
			store, path := setup(t, contract)
			child := exec.Command(os.Args[0], "-test.run=^TestKilledProcessLeavesOneOccurrenceAndAnAccount$")
			child.Env = append(os.Environ(), "HEARTIME_KILL_DB="+path, "HEARTIME_KILL_STAGE="+stage)
			must(t, child.Start())
			t.Cleanup(func() { child.Process.Kill() })
			deadlineAt := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(path + ".ready"); err == nil {
					break
				}
				if time.Now().After(deadlineAt) {
					t.Fatal("child never reached its durable point")
				}
				time.Sleep(10 * time.Millisecond)
			}
			must(t, child.Process.Signal(os.Kill))
			if child.Wait() == nil {
				t.Fatal("child was expected to die from SIGKILL")
			}

			must(t, store.Evaluate(base))
			if count(t, store, "kind = 'work'") != 1 {
				t.Fatal("SIGKILL and restart duplicated the semantic occurrence")
			}
			work := occurrence(t, contract, KindWork, base)
			account := requireCovered(t, store, base+1)
			if len(account.Due) != 1 || account.Due[0].ID != work || !slices.Equal(account.Unresolved, []string{work}) {
				t.Fatalf("what was due / unresolved: %+v", account)
			}
			emitted := stage == "attempted"
			if slices.Contains(account.Emitted, work) != emitted || slices.Contains(account.MayHaveExecuted, work) != emitted {
				t.Fatalf("emitted %v and may-have-executed %v must be %t", account.Emitted, account.MayHaveExecuted, emitted)
			}
			for subject, at := range map[string]int64{"work": base + 10, "planning-review": base + 70, "return:" + work: base + 20} {
				next, found := deadline(account, subject)
				if !found || next.At != formatUTC(at) || next.Overdue {
					t.Fatalf("next %s: %+v found=%t", subject, next, found)
				}
			}
		})
	}
}

// HEART-004.
func TestCatchUpPolicies(t *testing.T) {
	t.Run("all retains every instant in bounded batches", func(t *testing.T) {
		contract := fixture()
		contract.Spec.MaxBatch = 2
		store, _ := setup(t, contract)
		must(t, store.Evaluate(base+35))
		if count(t, store, "kind = 'work' AND state = 'pending'") != 2 {
			t.Fatal("first batch must hold exactly maxBatch occurrences")
		}
		next, err := store.NextEvaluation(base + 35)
		must(t, err)
		if next != base+20 {
			t.Fatalf("remaining backlog must be immediately due, got %d", next)
		}
		must(t, store.Evaluate(base+35))
		for _, nominal := range []int64{base, base + 10, base + 20, base + 30} {
			if stateOf(t, store, occurrence(t, contract, KindWork, nominal)) != StatePending {
				t.Fatalf("backlog instant %d lost", nominal)
			}
		}
	})
	t.Run("latest coalesces missed instants into one", func(t *testing.T) {
		contract := fixture()
		contract.Spec.CatchUp = CatchUpLatest
		store, _ := setup(t, contract)
		must(t, store.Evaluate(base+35))
		latest := occurrence(t, contract, KindWork, base+30)
		evidence := evidenceOf(t, store, latest)
		if count(t, store, "kind = 'work'") != 1 || evidence.CoveredFrom != formatUTC(base) || evidence.CoveredCount != 4 || stateOf(t, store, latest) != StatePending {
			t.Fatalf("latest must cover the whole missed interval: %+v", evidence)
		}
	})
	t.Run("skip records omissions and emits only within grace", func(t *testing.T) {
		contract := fixture()
		contract.Spec.CatchUp = CatchUpSkip
		store, _ := setup(t, contract)
		must(t, store.Evaluate(base+31))
		missed := evidenceOf(t, store, occurrence(t, contract, KindWork, base))
		if missed.Disposition != StateSkipped || missed.CoveredCount != 3 {
			t.Fatalf("missed interval must be recorded as skipped: %+v", missed)
		}
		if stateOf(t, store, occurrence(t, contract, KindWork, base+30)) != StatePending {
			t.Fatal("latest instant within grace must be emitted")
		}
		must(t, store.Evaluate(base+45))
		if stateOf(t, store, occurrence(t, contract, KindWork, base+40)) != StateSkipped {
			t.Fatal("instant beyond grace must be skipped, not emitted")
		}
	})
}

// HEART-005.
func TestOverlapPolicies(t *testing.T) {
	for _, policy := range []string{OverlapAllow, OverlapDefer, OverlapCoalesce} {
		t.Run(policy, func(t *testing.T) {
			contract := fixture()
			contract.Spec.Overlap = policy
			store, _ := setup(t, contract)
			must(t, store.Evaluate(base))
			first := occurrence(t, contract, KindWork, base)
			must(t, store.Attempt(first, base))
			must(t, store.Acknowledge(first, base))
			must(t, store.Evaluate(base+10))
			second := occurrence(t, contract, KindWork, base+10)
			err := store.Attempt(second, base+10)
			if policy == OverlapAllow {
				must(t, err)
				return
			}
			if !errors.Is(err, ErrOverlapDeferred) {
				t.Fatalf("acknowledged but unreported work must defer %s delivery, got %v", policy, err)
			}
			must(t, store.Report(first, OutcomeVerified, Digest([]byte("independent verification")), base+10))
			must(t, store.Attempt(second, base+10))
		})
	}
}

func TestCoalesceDeliversOnlyTheNewestUndeliveredWork(t *testing.T) {
	contract := fixture()
	contract.Spec.Overlap = OverlapCoalesce
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base+20))
	if err := store.Attempt(occurrence(t, contract, KindWork, base), base+20); !errors.Is(err, ErrNewerPending) {
		t.Fatalf("older undelivered work must yield to the newest, got %v", err)
	}
	must(t, store.Attempt(occurrence(t, contract, KindWork, base+20), base+20))
	if count(t, store, "state = 'coalesced'") != 2 {
		t.Fatal("older undelivered occurrences must be recorded as coalesced")
	}
}

// HEART-001.
func TestPlanningRolloverArmsTheNextPeriodBeforeExpiry(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base+70))
	review := occurrence(t, contract, KindPlanningReview, base+70)
	if stateOf(t, store, review) != StatePending || evidenceOf(t, store, review).Handoff != planningRef {
		t.Fatal("planning review must be due before coverage ends")
	}
	account := requireCovered(t, store, base+70)
	if next, found := deadline(account, KindPlanningReview); !found || next.At != formatUTC(base+90) {
		t.Fatalf("the next planning evaluation must be armed inside coverage: %+v", account.Next)
	}
	if len(account.Coverage) != 1 || account.Coverage[0].ValidUntil != formatUTC(base+100) || account.Coverage[0].Ended {
		t.Fatalf("current coverage must be exposed: %+v", account.Coverage)
	}
	plan := Digest([]byte("period N+1 plan"))
	must(t, store.Attempt(review, base+71))
	must(t, store.Renew(contract.Ref(), base+200, base+150, plan, base+71))
	must(t, store.Report(review, OutcomeVerified, plan, base+71))

	must(t, store.Evaluate(base+101))
	if count(t, store, "kind = 'fallback'") != 0 {
		t.Fatal("a renewed plan must not fall back")
	}
	if stateOf(t, store, occurrence(t, contract, KindWork, base+100)) != StatePending {
		t.Fatal("work of period N+1 must be deliverable")
	}
	account = requireCovered(t, store, base+101)
	if next, found := deadline(account, KindPlanningReview); !found || next.At != formatUTC(base+150) {
		t.Fatalf("period N+1 must arm its own planning review: %+v", account.Next)
	}
	if err := store.Renew(contract.Ref(), base+150, base+140, plan, base+120); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("a renewal that does not extend coverage must be refused, got %v", err)
	}
}

// HEART-002.
func TestMissedRenewalFallsBackAndLapsesUndeliveredWork(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	delivered := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(delivered, base))
	must(t, store.Evaluate(base+10))
	undelivered := occurrence(t, contract, KindWork, base+10)

	must(t, store.Evaluate(base+101))
	fallback := occurrence(t, contract, KindFallback, base+100)
	if stateOf(t, store, fallback) != StatePending || evidenceOf(t, store, fallback).Handoff != recoveryRef {
		t.Fatal("missed renewal must emit the contractual fallback, not silence")
	}
	if stateOf(t, store, undelivered) != StateLapsed {
		t.Fatal("undelivered work must lapse when coverage ends")
	}
	if !oneOf(stateOf(t, store, delivered), StatePending, StateAcknowledged) {
		t.Fatal("delivered work still owes a return and must stay unresolved")
	}
	if count(t, store, "kind = 'work' AND state = 'pending' AND nominal > ?", base) != 0 {
		t.Fatal("no ordinary work may become deliverable outside coverage")
	}
	if err := store.Attempt(occurrence(t, contract, KindWork, base+100), base+101); !errors.Is(err, ErrNotPending) {
		t.Fatalf("work due outside coverage must not be deliverable, got %v", err)
	}
	account := requireCovered(t, store, base+101)
	if next, found := deadline(account, KindFallback); !found || next.At != formatUTC(base+111) {
		t.Fatalf("fallback must stay armed: %+v", account.Next)
	}
	must(t, store.Evaluate(base+111))
	if count(t, store, "kind = 'fallback'") != 2 {
		t.Fatal("fallback silently disappeared")
	}
}

func TestFailedPlanningReturnInvokesFallback(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base+70))
	review := occurrence(t, contract, KindPlanningReview, base+70)
	must(t, store.Attempt(review, base+70))
	must(t, store.Report(review, OutcomeFailed, Digest([]byte("planner could not produce a plan")), base+72))
	must(t, store.Evaluate(base+72))
	returned := occurrence(t, contract, returnReviewPrefix+review, base+72)
	evidence := evidenceOf(t, store, returned)
	if evidence.Handoff != recoveryRef || evidence.Parent != review || evidence.FallbackMode != "safe_mode" {
		t.Fatalf("failed planning must route to the fallback relationship at once: %+v", evidence)
	}
}

// HEART-006.
func TestAcknowledgementIsNotVerification(t *testing.T) {
	contract := fixture()
	store, path := setup(t, contract)
	must(t, store.Evaluate(base))
	work := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(work, base))
	must(t, store.Acknowledge(work, base))
	must(t, store.Close())

	restarted := openLedger(t, path)
	account := requireCovered(t, restarted, base+1)
	if stateOf(t, restarted, work) != StateAcknowledged || !slices.Contains(account.MayHaveExecuted, work) || !slices.Contains(account.Unresolved, work) {
		t.Fatalf("acknowledged work must remain possibly executed and unresolved: %+v", account)
	}
	must(t, restarted.Evaluate(base+21))
	if evidenceOf(t, restarted, occurrence(t, contract, returnReviewPrefix+work, base+20)).Handoff != recoveryRef {
		t.Fatal("silence after acknowledgement must produce a return review")
	}
	if count(t, restarted, "state = 'verified'") != 0 {
		t.Fatal("transport acknowledgement became verification")
	}
}

func TestReturnReviewsStayBoundedWhileUndelivered(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	work := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(work, base))
	for now := base + 21; now <= base+91; now += 10 {
		must(t, store.Evaluate(now))
	}
	if count(t, store, "kind = ?", returnReviewPrefix+work) != 1 {
		t.Fatal("an undelivered return review must absorb later deadlines of the same parent")
	}
	requireCovered(t, store, base+91)
	pending, err := store.Pending()
	must(t, err)
	for _, delivery := range pending {
		if evidenceOf(t, store, delivery.ID).Parent == work {
			must(t, store.Attempt(delivery.ID, base+92))
		}
	}
	must(t, store.Evaluate(base+101))
	if count(t, store, "kind = ?", returnReviewPrefix+work) != 2 {
		t.Fatal("once delivered, the next deadline must produce the next review")
	}
}

func TestResolvedParentMakesUndeliveredReviewsLapse(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	work := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(work, base))
	must(t, store.Evaluate(base+21))
	undelivered := occurrence(t, contract, returnReviewPrefix+work, base+20)
	must(t, store.Report(work, OutcomeVerified, Digest([]byte("verified late")), base+22))
	must(t, store.Evaluate(base+23))
	if stateOf(t, store, undelivered) != StateLapsed {
		t.Fatal("a review of a resolved occurrence must not still be delivered")
	}
	pending, err := store.Pending()
	must(t, err)
	for _, delivery := range pending {
		if delivery.ID == undelivered {
			t.Fatal("a moot review is still pending delivery")
		}
	}
}

// HEART-007 (pause).
func TestPauseDefersWorkButNotReturnsOrPlanning(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	delivered := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(delivered, base))
	must(t, store.Pause(contract.Ref(), base+50, base+1, "maintenance window"))
	must(t, store.Evaluate(base+30))
	if count(t, store, "kind = 'work'") != 1 {
		t.Fatal("pause must stop work materialization")
	}
	if count(t, store, "kind = ?", returnReviewPrefix+delivered) != 1 {
		t.Fatal("pause must not suppress return reviews")
	}
	account := requireCovered(t, store, base+30)
	if next, found := deadline(account, KindWork); !found || next.At != formatUTC(base+50) {
		t.Fatalf("work must resume at the pause end: %+v", account.Next)
	}
	must(t, store.Evaluate(base+50))
	if stateOf(t, store, occurrence(t, contract, KindWork, base+50)) != StatePending {
		t.Fatal("work must resume on the original anchor after the pause")
	}
	if err := store.Pause(contract.Ref(), base+40, base+45, ""); !errors.Is(err, ErrInvalidControl) {
		t.Fatalf("a pause without reason or future end must be refused, got %v", err)
	}
}

// HEART-007 (supersession).
func TestSupersessionPreservesHistoryAndOwedReturns(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	delivered := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(delivered, base))
	must(t, store.Evaluate(base+21))
	undelivered := occurrence(t, contract, KindWork, base+20)
	review := occurrence(t, contract, returnReviewPrefix+delivered, base+20)
	before := evidenceOf(t, store, delivered)

	successor := fixture()
	successor.Metadata.Generation = 2
	successor.Spec.Recurrence.Anchor = formatUTC(base + 25)
	must(t, store.Install(successor, base+22))
	if stateOf(t, store, undelivered) != StateSuperseded {
		t.Fatal("undelivered old work must be superseded with provenance")
	}
	if stateOf(t, store, review) != StatePending {
		t.Fatal("supersession must not cancel a review of an effect of the old generation")
	}
	if after := evidenceOf(t, store, delivered); after != before || after.Contract.Generation != 1 {
		t.Fatal("old occurrences must keep their original terms")
	}
	must(t, store.Evaluate(base+41))
	if count(t, store, "generation = 1 AND kind = ?", returnReviewPrefix+delivered) != 1 {
		t.Fatal("the undelivered review of the old generation must still absorb later deadlines")
	}
	if stateOf(t, store, occurrence(t, successor, KindWork, base+25)) != StatePending {
		t.Fatal("the new generation must arm its own recurrence")
	}
	requireCovered(t, store, base+41)
}

func TestGenerationsAreImmutableAndClockCannotRegress(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	changed := contract
	changed.Spec.Overlap = OverlapAllow
	if err := store.Install(changed, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("rewriting generation terms must conflict, got %v", err)
	}
	must(t, store.Install(contract, base))
	must(t, store.Evaluate(base+1))
	if err := store.Evaluate(base); !errors.Is(err, ErrClock) {
		t.Fatalf("clock regression must be explicit, got %v", err)
	}
}

func TestReportRequiresDeliveryAndImmutableEvidence(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	work := occurrence(t, contract, KindWork, base)
	receipt := Digest([]byte("receipt"))
	if err := store.Report(work, OutcomeVerified, receipt, base); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("undelivered work cannot be verified, got %v", err)
	}
	must(t, store.Attempt(work, base))
	if err := store.Report(work, OutcomeVerified, "looks good", base); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("free text is not evidence, got %v", err)
	}
	if err := store.Report(work, Outcome("acknowledged"), receipt, base); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("acknowledgement is not an execution outcome, got %v", err)
	}
	must(t, store.Report(work, OutcomeVerified, receipt, base))
	must(t, store.Report(work, OutcomeVerified, receipt, base))
	if err := store.Report(work, OutcomeFailed, receipt, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("a final report must not be reinterpreted, got %v", err)
	}
}

func TestContainmentReleasesOverlapWithoutInventingSuccess(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base))
	first := occurrence(t, contract, KindWork, base)
	must(t, store.Attempt(first, base))
	must(t, store.Report(first, OutcomeFailed, Digest([]byte("failure")), base))
	must(t, store.Evaluate(base+10))
	second := occurrence(t, contract, KindWork, base+10)
	if err := store.Attempt(second, base+10); !errors.Is(err, ErrOverlapDeferred) {
		t.Fatalf("an unresolved failure must still defer work, got %v", err)
	}
	must(t, store.Report(first, OutcomeContained, Digest([]byte("effects independently bounded")), base+10))
	must(t, store.Attempt(second, base+10))
	if count(t, store, "state = 'verified'") != 0 {
		t.Fatal("containment invented success")
	}
}

func TestExpiryAndRetirementLapseUndeliveredWork(t *testing.T) {
	t.Run("expiry", func(t *testing.T) {
		contract := fixture()
		contract.Spec.ExpiresAt = formatUTC(base + 15)
		store, _ := setup(t, contract)
		must(t, store.Evaluate(base+10))
		work := occurrence(t, contract, KindWork, base+10)
		if err := store.Attempt(work, base+16); !errors.Is(err, ErrOutsideCoverage) {
			t.Fatalf("expired work must not be delivered, got %v", err)
		}
		must(t, store.Evaluate(base+16))
		if stateOf(t, store, work) != StateLapsed {
			t.Fatal("expired undelivered work must lapse")
		}
		requireCovered(t, store, base+16)
	})
	t.Run("retirement", func(t *testing.T) {
		contract := fixture()
		store, _ := setup(t, contract)
		must(t, store.Evaluate(base+10))
		delivered, undelivered := occurrence(t, contract, KindWork, base), occurrence(t, contract, KindWork, base+10)
		must(t, store.Attempt(delivered, base+10))
		must(t, store.Retire(contract.Ref(), base+11, "responsibility ended by Direction"))
		if stateOf(t, store, undelivered) != StateLapsed {
			t.Fatal("retirement must lapse undelivered work")
		}
		account := requireCovered(t, store, base+11)
		if _, found := deadline(account, KindPlanningReview); found {
			t.Fatal("a retired obligation must not keep planning evaluations")
		}
		if _, found := deadline(account, "return:"+delivered); !found {
			t.Fatal("retirement must keep the return owed by delivered work")
		}
	})
}

func TestConcurrentEvaluatorsDoNotDuplicate(t *testing.T) {
	store, path := setup(t, fixture())
	other := openLedger(t, path)
	var group sync.WaitGroup
	errs := make(chan error, 2)
	for _, ledger := range []*Store{store, other} {
		group.Add(1)
		go func(ledger *Store) {
			defer group.Done()
			errs <- ledger.Evaluate(base + 10)
		}(ledger)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	if count(t, store, "kind = 'work'") != 2 {
		t.Fatal("concurrent evaluation duplicated occurrences")
	}
}

func TestStrictContractParsing(t *testing.T) {
	contract := fixture()
	raw, err := json.Marshal(contract)
	must(t, err)
	_, err = Parse(raw)
	must(t, err)
	if _, err := Parse(append(raw, []byte("{}")...)); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("trailing documents must be refused, got %v", err)
	}
	var fields map[string]any
	must(t, json.Unmarshal(raw, &fields))
	delete(fields["spec"].(map[string]any), "graceSeconds")
	implicit, err := json.Marshal(fields)
	must(t, err)
	if _, err := Parse(implicit); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("implicit zero grace must be refused, got %v", err)
	}
	contract.Spec.Planning.NextPlanningReviewAt = contract.Spec.Planning.PlanValidThrough
	if !errors.Is(contract.Validate(), ErrInvalidContract) {
		t.Fatal("a planning review at the end of coverage must be refused")
	}
	contract = fixture()
	contract.Metadata.Subject = "Powerfarm"
	if !errors.Is(contract.Validate(), ErrInvalidContract) {
		t.Fatal("identities must follow the common schema patterns")
	}
}

func TestPlanningDeadlineCannotMovePastCoverage(t *testing.T) {
	contract := fixture()
	contract.Spec.Planning.ReviewEverySeconds = maxSeconds
	store, _ := setup(t, contract)
	must(t, store.Evaluate(base+70))
	must(t, store.Evaluate(base+100))
	if count(t, store, "kind = 'fallback'") != 1 {
		t.Fatal("a long review interval must not skip the end of coverage")
	}
}

func TestLivenessInvariantAcrossLifecycle(t *testing.T) {
	contract := fixture()
	store, _ := setup(t, contract)
	steps := []struct {
		name  string
		now   int64
		apply func(now int64) error
	}{
		{"evaluate", base, store.Evaluate},
		{"deliver", base, func(now int64) error { return store.Attempt(occurrence(t, contract, KindWork, base), now) }},
		{"report uncertain", base + 5, func(now int64) error {
			return store.Report(occurrence(t, contract, KindWork, base), OutcomeUncertain, Digest([]byte("timeout")), now)
		}},
		{"evaluate returns", base + 25, store.Evaluate},
		{"pause", base + 26, func(now int64) error { return store.Pause(contract.Ref(), base+60, now, "incident") }},
		{"coverage ends", base + 101, store.Evaluate},
		{"retire", base + 102, func(now int64) error { return store.Retire(contract.Ref(), now, "ended") }},
		{"evaluate after retirement", base + 130, store.Evaluate},
	}
	for _, step := range steps {
		must(t, step.apply(step.now))
		account := requireCovered(t, store, step.now)
		for _, id := range account.Unresolved {
			if evidenceOf(t, store, id).Parent != "" {
				continue
			}
			if _, found := deadline(account, "return:"+id); !found {
				t.Fatalf("after %s: unresolved %s has no future evaluation", step.name, id)
			}
		}
	}
}

func TestRelayAcknowledgesWithoutVerifying(t *testing.T) {
	now := time.Now().UTC().Unix()
	contract := fixture()
	contract.Spec.Recurrence.Anchor = formatUTC(now)
	contract.Spec.Planning.PlanValidThrough = formatUTC(now + 100)
	contract.Spec.Planning.NextPlanningReviewAt = formatUTC(now + 70)
	store := openLedger(t, filepath.Join(t.TempDir(), "relay.db"))
	must(t, store.Install(contract, now))
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		select {
		case received <- r.Header.Get("Idempotency-Key"):
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.Run(ctx, server.URL, "test-token") }()
	var id string
	select {
	case id = <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("relay delivered nothing")
	}
	deadlineAt := time.Now().Add(5 * time.Second)
	for stateOf(t, store, id) != StateAcknowledged {
		if time.Now().After(deadlineAt) {
			t.Fatalf("delivery never acknowledged: %s", stateOf(t, store, id))
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if count(t, store, "state = 'verified'") != 0 {
		t.Fatal("HTTP acknowledgement became verification")
	}
}

func TestRelayKeepsRefusedDeliveryPending(t *testing.T) {
	now := time.Now().UTC().Unix()
	contract := fixture()
	contract.Spec.Recurrence.Anchor = formatUTC(now)
	contract.Spec.Planning.PlanValidThrough = formatUTC(now + 100)
	contract.Spec.Planning.NextPlanningReviewAt = formatUTC(now + 70)
	store := openLedger(t, filepath.Join(t.TempDir(), "relay.db"))
	must(t, store.Install(contract, now))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.Run(ctx, server.URL, "test-token") }()
	deadlineAt := time.Now().Add(10 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadlineAt) {
			t.Fatal("relay never attempted delivery")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	pending, err := store.Pending()
	must(t, err)
	if len(pending) != 1 || pending[0].Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("a refused delivery must stay pending and back off: calls=%d pending=%+v", calls.Load(), pending)
	}
}

func TestRetryDelayIsBoundedAndIncreasing(t *testing.T) {
	previous := int64(0)
	for attempts := 1; attempts <= 12; attempts++ {
		delay := RetryDelay(attempts)
		if delay < previous || delay > maxRetryDelay {
			t.Fatalf("attempt %d: delay %d after %d", attempts, delay, previous)
		}
		previous = delay
	}
	if RetryDelay(1) != 5 || RetryDelay(12) != maxRetryDelay {
		t.Fatal("retry delay must start at 5 s and cap at 300 s")
	}
}
