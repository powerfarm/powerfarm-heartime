package heartime

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3" // Registers the SQLite driver for database/sql.
)

// Occurrence kinds. A return review's kind embeds its parent occurrence so that
// simultaneous deadlines of different parents never share an identity.
const (
	KindWork           = "work"
	KindPlanningReview = "planning-review"
	KindFallback       = "fallback"
	returnReviewPrefix = "return-review/"
)

// Occurrence dispositions.
const (
	StatePending      = "pending"      // due; delivery not yet acknowledged
	StateAcknowledged = "acknowledged" // delivery acknowledged; effect unknown
	StateVerified     = "verified"     // downstream reported independently verified success
	StateFailed       = "failed"       // downstream reported failure; effects may remain
	StateUncertain    = "uncertain"    // downstream cannot establish what happened
	StateContained    = "contained"    // failure effects reconciled or bounded; not success
	StateSkipped      = "skipped"      // materialized outside grace, coverage or expiry
	StateCoalesced    = "coalesced"    // undelivered and folded into a newer occurrence
	StateSuperseded   = "superseded"   // undelivered when a newer generation was installed
	StateLapsed       = "lapsed"       // undelivered when coverage ended, the contract expired or retired
)

// unresolvedStates still owe a return. Every occurrence in one of these states
// keeps a durable review deadline.
const unresolvedStates = `('pending','acknowledged','uncertain','failed')`

// Clock sources recorded in temporal evidence.
const (
	ClockSystemUTC   = "system-utc"
	ClockOperatorUTC = "operator-supplied-utc"
)

const schema = `
CREATE TABLE IF NOT EXISTS contracts(
  id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  digest TEXT NOT NULL,
  body BLOB NOT NULL,
  current INTEGER NOT NULL,
  cursor INTEGER NOT NULL,
  coverage INTEGER NOT NULL,
  planning_review INTEGER NOT NULL,
  pause_until INTEGER NOT NULL DEFAULT 0,
  retired INTEGER NOT NULL DEFAULT 0,
  last_clock INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(id, generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_current ON contracts(id) WHERE current = 1;
CREATE TABLE IF NOT EXISTS occurrences(
  id TEXT PRIMARY KEY,
  contract_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  kind TEXT NOT NULL,
  nominal INTEGER NOT NULL,
  evaluated INTEGER NOT NULL,
  covered_from INTEGER NOT NULL,
  covered_count INTEGER NOT NULL,
  state TEXT NOT NULL,
  review_at INTEGER NOT NULL,
  body BLOB NOT NULL,
  FOREIGN KEY(contract_id, generation) REFERENCES contracts(id, generation)
);
CREATE INDEX IF NOT EXISTS occurrences_by_contract ON occurrences(contract_id, generation, state);
CREATE INDEX IF NOT EXISTS occurrences_by_kind ON occurrences(kind, state);
CREATE TABLE IF NOT EXISTS outbox(
  id TEXT PRIMARY KEY REFERENCES occurrences(id),
  attempts INTEGER NOT NULL DEFAULT 0,
  acknowledged INTEGER NOT NULL DEFAULT 0,
  last_attempt INTEGER
);
CREATE TABLE IF NOT EXISTS evidence(
  seq INTEGER PRIMARY KEY,
  occurrence_id TEXT,
  kind TEXT NOT NULL,
  at INTEGER NOT NULL,
  body BLOB NOT NULL
);`

// Store is Heartime's application-owned SQLite ledger of contracts,
// occurrences, delivery intents and temporal evidence.
type Store struct {
	db *sql.DB
	// ClockSource names where evaluation instants come from. It is written into
	// every occurrence's temporal evidence.
	ClockSource string
}

// OccurrenceEvidence is the temporal evidence persisted and delivered for one
// occurrence. It asserts due-ness only; it grants no authority.
type OccurrenceEvidence struct {
	OccurrenceID    string `json:"occurrenceId"`
	Contract        Ref    `json:"contract"`
	ContractDigest  string `json:"contractDigest"`
	Responsibility  Ref    `json:"responsibility"`
	ObligationID    string `json:"obligationId"`
	Kind            string `json:"kind"`
	Nominal         string `json:"nominal"`
	CoveredFrom     string `json:"coveredFrom"`
	CoveredCount    int64  `json:"coveredCount"`
	Predicate       string `json:"predicate"`
	PredicateResult bool   `json:"predicateResult"`
	Disposition     string `json:"disposition"`
	EvaluatedAt     string `json:"evaluatedAt"`
	ClockSource     string `json:"clockSource"`
	Handoff         Ref    `json:"handoff"`
	Parent          string `json:"parent,omitempty"`
	Overlap         string `json:"overlap"`
	FallbackMode    string `json:"fallbackMode"`
}

// Open opens or creates a ledger. Every write is one IMMEDIATE transaction in
// WAL mode with synchronous=FULL, so a committed occurrence, its evidence, its
// delivery intent and the next deadline survive process or power loss before
// any delivery is attempted.
func Open(path string) (*Store, error) {
	dsn := url.URL{Scheme: "file", Path: path}
	query := dsn.Query()
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")
	query.Set("_synchronous", "FULL")
	query.Set("_txlock", "immediate")
	dsn.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, ClockSource: ClockSystemUTC}, nil
}

// Close releases the ledger.
func (s *Store) Close() error { return s.db.Close() }

// transact runs apply inside one IMMEDIATE transaction and commits only when
// apply succeeds.
func (s *Store) transact(apply func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := apply(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func recordEvidence(tx *sql.Tx, occurrenceID, kind string, at int64, body any) error {
	canonical, err := Canonical(body)
	if err != nil {
		return err
	}
	var subject any
	if occurrenceID != "" {
		subject = occurrenceID
	}
	_, err = tx.Exec(`INSERT INTO evidence(occurrence_id, kind, at, body) VALUES (?, ?, ?, ?)`, subject, kind, at, canonical)
	return err
}

// Install caches exact contract terms admitted by a trusted operator. It is not
// Registry admission. Installing a newer generation disables the older
// recurrence and establishes the new future evaluations in the same transaction.
func (s *Store) Install(contract Contract, now int64) error {
	if err := contract.Validate(); err != nil {
		return err
	}
	body, err := Canonical(contract)
	if err != nil {
		return err
	}
	digest := Digest(body)
	ref := contract.Ref()
	return s.transact(func(tx *sql.Tx) error {
		var existing string
		err := tx.QueryRow(`SELECT digest FROM contracts WHERE id = ? AND generation = ?`, ref.ID, ref.Generation).Scan(&existing)
		switch {
		case err == nil && existing == digest:
			return nil
		case err == nil:
			return fmt.Errorf("%w: generation %d of %s already has different terms", ErrConflict, ref.Generation, ref.ID)
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		var newest int
		if err := tx.QueryRow(`SELECT COALESCE(MAX(generation), 0) FROM contracts WHERE id = ?`, ref.ID).Scan(&newest); err != nil {
			return err
		}
		if newest >= ref.Generation {
			return fmt.Errorf("%w: generation %d of %s does not follow generation %d", ErrConflict, ref.Generation, ref.ID, newest)
		}
		if _, err := tx.Exec(`UPDATE contracts SET current = 0 WHERE id = ?`, ref.ID); err != nil {
			return err
		}
		if err := supersedeUndelivered(tx, ref, now); err != nil {
			return err
		}
		anchor, _ := parseUTC(contract.Spec.Recurrence.Anchor)
		coverage, _ := parseUTC(contract.Spec.Planning.PlanValidThrough)
		review, _ := parseUTC(contract.Spec.Planning.NextPlanningReviewAt)
		if _, err := tx.Exec(`INSERT INTO contracts(id, generation, digest, body, current, cursor, coverage, planning_review, last_clock)
			VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)`, ref.ID, ref.Generation, digest, body, anchor, coverage, review, now); err != nil {
			return err
		}
		return recordEvidence(tx, "", "contract-installed", now, map[string]any{
			"contract":  contract.Metadata,
			"digest":    digest,
			"admission": "trusted-local-operator-cache; not Registry admission",
		})
	})
}

// supersedeUndelivered marks undelivered work and planning evaluations of older
// generations as superseded. Return reviews stay pending: they concern effects of
// occurrences that the new terms must not reinterpret.
func supersedeUndelivered(tx *sql.Tx, successor Ref, now int64) error {
	ids, err := queryIDs(tx, `SELECT o.id FROM occurrences o JOIN outbox b ON b.id = o.id
		WHERE o.contract_id = ? AND o.state = 'pending' AND b.attempts = 0
		AND o.kind IN ('work', 'planning-review', 'fallback') ORDER BY o.nominal, o.id`, successor.ID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := tx.Exec(`UPDATE occurrences SET state = ? WHERE id = ?`, StateSuperseded, id); err != nil {
			return err
		}
		if err := recordEvidence(tx, id, "disposition-superseded", now, map[string]any{"supersededBy": successor}); err != nil {
			return err
		}
	}
	return nil
}

type contractRow struct {
	terms          Contract
	digest         string
	cursor         int64
	coverage       int64
	planningReview int64
	pauseUntil     int64
	lastClock      int64
	current        bool
	retired        bool
}

func (r contractRow) ref() Ref     { return r.terms.Ref() }
func (r contractRow) active() bool { return r.current && !r.retired }

// expiresAt returns the contract expiry, or never when the contract has none.
func (r contractRow) expiresAt() int64 {
	if r.terms.Spec.ExpiresAt == "" {
		return never
	}
	expires, _ := parseUTC(r.terms.Spec.ExpiresAt)
	return expires
}

// ordinaryKinds are the occurrence kinds an obligation stops owing once it ends.
// Return reviews are never among them: an ended obligation still owes returns.
var ordinaryKinds = []string{KindWork, KindPlanningReview, KindFallback}

// expired reports whether the contract's own terms ended the obligation.
func (r contractRow) expired(now int64) bool { return now >= r.expiresAt() }

// lapse explains why undelivered occurrences of this generation can never be
// delivered and which kinds are affected, or returns an empty reason. The end
// of planning coverage lapses only work: the fallback is then exactly what must
// be delivered. A pause is not a reason; it only defers delivery.
func (r contractRow) lapse(now int64) (string, []string) {
	switch {
	case r.retired:
		return "obligation retired", ordinaryKinds
	case r.expired(now):
		return "contract expired at " + r.terms.Spec.ExpiresAt, ordinaryKinds
	case now >= r.coverage:
		return "planning coverage ended at " + formatUTC(r.coverage), []string{KindWork}
	}
	return "", nil
}

// workBlocked explains why ordinary work cannot be delivered at now, or returns
// the empty string when delivery is admissible.
func (r contractRow) workBlocked(now int64) string {
	if !r.current {
		return "contract generation was superseded"
	}
	if reason, _ := r.lapse(now); reason != "" {
		return reason
	}
	if now < r.pauseUntil {
		return "obligation paused until " + formatUTC(r.pauseUntil)
	}
	return ""
}

const contractColumns = `body, digest, cursor, coverage, planning_review, pause_until, last_clock, current, retired`

func scanContract(scan func(...any) error) (contractRow, error) {
	var row contractRow
	var body []byte
	if err := scan(&body, &row.digest, &row.cursor, &row.coverage, &row.planningReview, &row.pauseUntil, &row.lastClock, &row.current, &row.retired); err != nil {
		return row, err
	}
	if err := json.Unmarshal(body, &row.terms); err != nil {
		return row, err
	}
	return row, nil
}

func loadContracts(tx *sql.Tx) ([]contractRow, error) {
	rows, err := tx.Query(`SELECT ` + contractColumns + ` FROM contracts ORDER BY id, generation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	contracts := []contractRow{}
	for rows.Next() {
		row, err := scanContract(rows.Scan)
		if err != nil {
			return nil, err
		}
		contracts = append(contracts, row)
	}
	return contracts, rows.Err()
}

// loadActiveContract returns the current generation named by ref when it is
// neither retired nor expired at now.
func loadActiveContract(tx *sql.Tx, ref Ref, now int64) (contractRow, error) {
	row, err := scanContract(tx.QueryRow(`SELECT `+contractColumns+` FROM contracts WHERE id = ? AND generation = ?`, ref.ID, ref.Generation).Scan)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (!row.active() || row.expired(now))) {
		return row, fmt.Errorf("%w: %s generation %d", ErrUnknownContract, ref.ID, ref.Generation)
	}
	return row, err
}

// Evaluate materializes everything due at now in one transaction: planning
// reviews or fallbacks, work occurrences under the catch-up policy, lapses of
// undeliverable work, and return reviews of unresolved occurrences. Every
// occurrence, its evidence, its delivery intent and every advanced deadline are
// committed together before anything can be delivered.
func (s *Store) Evaluate(now int64) error {
	return s.transact(func(tx *sql.Tx) error {
		contracts, err := loadContracts(tx)
		if err != nil {
			return err
		}
		for _, row := range contracts {
			if now < row.lastClock {
				return fmt.Errorf("%w: evaluation at %s precedes evidence already persisted at %s for %s", ErrClock, formatUTC(now), formatUTC(row.lastClock), row.ref().ID)
			}
			if row.active() && row.expired(now) && row.lastClock < row.expiresAt() {
				if err := recordEvidence(tx, "", "obligation-expired", now, map[string]any{"contract": row.ref(), "expiresAt": row.terms.Spec.ExpiresAt}); err != nil {
					return err
				}
			}
			if row.active() && !row.expired(now) {
				if err := s.evaluatePlanning(tx, &row, now); err != nil {
					return err
				}
				if err := s.evaluateWork(tx, &row, now); err != nil {
					return err
				}
			}
			if row.current {
				if reason, kinds := row.lapse(now); reason != "" {
					if err := lapseUndelivered(tx, row.ref(), kinds, reason, now); err != nil {
						return err
					}
				}
			}
			if _, err := tx.Exec(`UPDATE contracts SET cursor = ?, planning_review = ?, last_clock = ? WHERE id = ? AND generation = ?`,
				row.cursor, row.planningReview, now, row.ref().ID, row.ref().Generation); err != nil {
				return err
			}
			if err := s.evaluateReturnReviews(tx, row, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// evaluatePlanning emits a planning review while coverage lasts and arms the
// next one, never later than the end of coverage, so expiry is always observed.
// Once coverage has ended it emits the fallback, dated at the end of coverage,
// and records a planning review missed while Heartime was not evaluating as
// skipped: the fallback now carries that obligation.
func (s *Store) evaluatePlanning(tx *sql.Tx, row *contractRow, now int64) error {
	if now < row.planningReview {
		return nil
	}
	planning, fallback := row.terms.Spec.Planning, row.terms.Spec.Fallback
	if now < row.coverage {
		review := occurrenceSpec{kind: KindPlanningReview, nominal: row.planningReview, coveredFrom: row.planningReview, coveredCount: 1, state: StatePending, handoff: planning.Handoff}
		if err := s.materialize(tx, *row, now, review); err != nil {
			return err
		}
		row.planningReview = min(now+planning.ReviewEverySeconds, row.coverage)
		return nil
	}
	if row.planningReview < row.coverage {
		missed := occurrenceSpec{kind: KindPlanningReview, nominal: row.planningReview, coveredFrom: row.planningReview, coveredCount: 1, state: StateSkipped, handoff: planning.Handoff}
		if err := s.materialize(tx, *row, now, missed); err != nil {
			return err
		}
	}
	nominal := max(row.planningReview, row.coverage)
	emitted := occurrenceSpec{kind: KindFallback, nominal: nominal, coveredFrom: nominal, coveredCount: 1, state: StatePending, handoff: fallback.Handoff}
	if err := s.materialize(tx, *row, now, emitted); err != nil {
		return err
	}
	row.planningReview = now + fallback.ReviewAfterSeconds
	return nil
}

// evaluateWork materializes due work occurrences under the catch-up policy and
// advances the recurrence cursor. Work due outside coverage or after expiry is
// recorded as skipped rather than delivered.
func (s *Store) evaluateWork(tx *sql.Tx, row *contractRow, now int64) error {
	terms := row.terms.Spec
	expires := row.expiresAt()
	if now < row.pauseUntil || row.cursor > now || row.cursor >= expires {
		return nil
	}
	last := now
	if last >= expires {
		last = expires - 1
	}
	step := terms.Recurrence.EverySeconds
	due := int64(1)
	if step > 0 {
		due = (last-row.cursor)/step + 1
	}
	deliverable := now < row.coverage && now < expires
	advanced := due
	switch terms.CatchUp {
	case CatchUpAll:
		if due > int64(terms.MaxBatch) {
			advanced = int64(terms.MaxBatch)
		}
		for index := int64(0); index < advanced; index++ {
			nominal := row.cursor + index*step
			state := StateSkipped
			if deliverable {
				state = StatePending
			}
			occurrence := occurrenceSpec{kind: KindWork, nominal: nominal, coveredFrom: nominal, coveredCount: 1, state: state, handoff: terms.Handoff}
			if err := s.materialize(tx, *row, now, occurrence); err != nil {
				return err
			}
		}
	case CatchUpLatest:
		latest := row.cursor + (due-1)*step
		state := StateSkipped
		if deliverable {
			state = StatePending
		}
		occurrence := occurrenceSpec{kind: KindWork, nominal: latest, coveredFrom: row.cursor, coveredCount: due, state: state, handoff: terms.Handoff}
		if err := s.materialize(tx, *row, now, occurrence); err != nil {
			return err
		}
	case CatchUpSkip:
		latest := row.cursor + (due-1)*step
		if due > 1 {
			missed := occurrenceSpec{kind: KindWork, nominal: row.cursor, coveredFrom: row.cursor, coveredCount: due - 1, state: StateSkipped, handoff: terms.Handoff}
			if err := s.materialize(tx, *row, now, missed); err != nil {
				return err
			}
		}
		state := StateSkipped
		if deliverable && now-latest <= terms.GraceSeconds {
			state = StatePending
		}
		occurrence := occurrenceSpec{kind: KindWork, nominal: latest, coveredFrom: latest, coveredCount: 1, state: state, handoff: terms.Handoff}
		if err := s.materialize(tx, *row, now, occurrence); err != nil {
			return err
		}
	}
	if step == 0 {
		row.cursor = never
	} else {
		row.cursor += advanced * step
	}
	return nil
}

// lapseUndelivered records that occurrences of the given kinds that were never
// delivered can no longer be delivered. Delivered occurrences are untouched:
// they still owe a return.
func lapseUndelivered(tx *sql.Tx, ref Ref, kinds []string, reason string, now int64) error {
	for _, kind := range kinds {
		ids, err := queryIDs(tx, `SELECT o.id FROM occurrences o JOIN outbox b ON b.id = o.id
			WHERE o.contract_id = ? AND o.generation = ? AND o.kind = ? AND o.state = 'pending' AND b.attempts = 0
			ORDER BY o.nominal, o.id`, ref.ID, ref.Generation, kind)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := tx.Exec(`UPDATE occurrences SET state = ? WHERE id = ?`, StateLapsed, id); err != nil {
				return err
			}
			if err := recordEvidence(tx, id, "disposition-lapsed", now, map[string]string{"reason": reason}); err != nil {
				return err
			}
		}
	}
	return nil
}

// evaluateReturnReviews emits a return review for every unresolved occurrence
// whose review deadline arrived and arms the next deadline. While one review of
// a parent still waits for its first delivery attempt, a later deadline adds no
// new review, so an unreachable receiver cannot grow the ledger without bound.
// Return checks continue for superseded generations and retired obligations.
func (s *Store) evaluateReturnReviews(tx *sql.Tx, row contractRow, now int64) error {
	type dueReturn struct {
		id       string
		reviewAt int64
	}
	rows, err := tx.Query(`SELECT id, review_at FROM occurrences
		WHERE contract_id = ? AND generation = ? AND state IN `+unresolvedStates+`
		AND review_at <= ? AND kind NOT LIKE 'return-review/%' ORDER BY review_at, id`, row.ref().ID, row.ref().Generation, now)
	if err != nil {
		return err
	}
	due := []dueReturn{}
	for rows.Next() {
		var item dueReturn
		if err := rows.Scan(&item.id, &item.reviewAt); err != nil {
			rows.Close()
			return err
		}
		due = append(due, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A review that was never delivered is moot once its parent is resolved.
	moot, err := queryIDs(tx, `SELECT o.id FROM occurrences o JOIN outbox b ON b.id = o.id
		WHERE o.contract_id = ? AND o.generation = ? AND o.kind LIKE 'return-review/%' AND o.state = 'pending' AND b.attempts = 0
		AND (SELECT p.state FROM occurrences p WHERE p.id = substr(o.kind, length('return-review/') + 1)) NOT IN `+unresolvedStates+`
		ORDER BY o.nominal, o.id`, row.ref().ID, row.ref().Generation)
	if err != nil {
		return err
	}
	for _, id := range moot {
		if _, err := tx.Exec(`UPDATE occurrences SET state = ? WHERE id = ?`, StateLapsed, id); err != nil {
			return err
		}
		if err := recordEvidence(tx, id, "disposition-lapsed", now, map[string]string{"reason": "the reviewed occurrence was resolved before this review was delivered"}); err != nil {
			return err
		}
	}
	fallback := row.terms.Spec.Fallback
	for _, parent := range due {
		var waiting int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM occurrences o JOIN outbox b ON b.id = o.id
			WHERE o.kind = ? AND o.state = 'pending' AND b.attempts = 0`, returnReviewPrefix+parent.id).Scan(&waiting); err != nil {
			return err
		}
		if waiting == 0 {
			review := occurrenceSpec{kind: returnReviewPrefix + parent.id, nominal: parent.reviewAt, coveredFrom: parent.reviewAt, coveredCount: 1, state: StatePending, handoff: fallback.Handoff, parent: parent.id}
			if err := s.materialize(tx, row, now, review); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE occurrences SET review_at = ? WHERE id = ?`, now+fallback.ReviewAfterSeconds, parent.id); err != nil {
			return err
		}
	}
	return nil
}

type occurrenceSpec struct {
	kind         string
	nominal      int64
	coveredFrom  int64
	coveredCount int64
	state        string
	handoff      Ref
	parent       string
}

// materialize persists one occurrence with its temporal evidence, return
// deadline and, when deliverable, its delivery intent. Re-materializing the same
// nominal occurrence is a no-op, which keeps its identity stable across restarts.
func (s *Store) materialize(tx *sql.Tx, row contractRow, now int64, spec occurrenceSpec) error {
	terms := row.terms.Spec
	id, err := OccurrenceID(row.ref(), terms.ObligationID, spec.kind, spec.nominal)
	if err != nil {
		return err
	}
	body, err := Canonical(OccurrenceEvidence{
		OccurrenceID:    id,
		Contract:        row.ref(),
		ContractDigest:  row.digest,
		Responsibility:  terms.Responsibility,
		ObligationID:    terms.ObligationID,
		Kind:            spec.kind,
		Nominal:         formatUTC(spec.nominal),
		CoveredFrom:     formatUTC(spec.coveredFrom),
		CoveredCount:    spec.coveredCount,
		Predicate:       "due",
		PredicateResult: true,
		Disposition:     spec.state,
		EvaluatedAt:     formatUTC(now),
		ClockSource:     s.ClockSource,
		Handoff:         spec.handoff,
		Parent:          spec.parent,
		Overlap:         terms.Overlap,
		FallbackMode:    terms.Fallback.Mode,
	})
	if err != nil {
		return err
	}
	result, err := tx.Exec(`INSERT OR IGNORE INTO occurrences(id, contract_id, generation, kind, nominal, evaluated, covered_from, covered_count, state, review_at, body)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, row.ref().ID, row.ref().Generation, spec.kind, spec.nominal, now, spec.coveredFrom, spec.coveredCount, spec.state, now+terms.ReviewAfterSeconds, body)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 0 {
		return err
	}
	if err := recordEvidence(tx, id, "temporal-"+spec.state, now, json.RawMessage(body)); err != nil {
		return err
	}
	if spec.state == StatePending {
		_, err = tx.Exec(`INSERT INTO outbox(id) VALUES (?)`, id)
	}
	return err
}

func queryIDs(tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
