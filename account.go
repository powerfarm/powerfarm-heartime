package heartime

import (
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
)

// OccurrenceAccount is the durable disposition of one occurrence.
type OccurrenceAccount struct {
	ID           string `json:"id"`
	Contract     Ref    `json:"contract"`
	Kind         string `json:"kind"`
	Nominal      string `json:"nominal"`
	CoveredCount int64  `json:"coveredCount"`
	State        string `json:"state"`
	Attempts     int    `json:"attempts"`
	Acknowledged bool   `json:"acknowledged"`
	ReviewAt     string `json:"reviewAt,omitempty"`
}

// Deadline is one durable future temporal evaluation. Subject is "work",
// "planning-review", "fallback" or "return:<occurrence id>".
type Deadline struct {
	Contract Ref    `json:"contract"`
	Subject  string `json:"subject"`
	At       string `json:"at"`
	Overdue  bool   `json:"overdue"`
	unix     int64
}

// PlanningCoverage is the planning coverage of the current generation of an
// obligation.
type PlanningCoverage struct {
	Contract    Ref    `json:"contract"`
	ValidUntil  string `json:"validUntil"`
	Ended       bool   `json:"ended"`
	PausedUntil string `json:"pausedUntil,omitempty"`
	Expired     bool   `json:"expired,omitempty"`
	Retired     bool   `json:"retired,omitempty"`
}

// Account answers from durable state alone, typically after a restart: what was
// due, what was emitted, what may have executed, what remains unresolved and
// what must happen next. Uncovered lists active obligations or unresolved
// occurrences without a future evaluation; the liveness invariant requires it
// to be empty.
type Account struct {
	At              string              `json:"at"`
	Due             []OccurrenceAccount `json:"due"`
	Emitted         []string            `json:"emitted"`
	MayHaveExecuted []string            `json:"mayHaveExecuted"`
	Unresolved      []string            `json:"unresolved"`
	Next            []Deadline          `json:"next"`
	Coverage        []PlanningCoverage  `json:"coverage"`
	Uncovered       []string            `json:"uncovered"`
}

// Account reads a consistent snapshot of the ledger at now.
func (s *Store) Account(now int64) (Account, error) {
	account := Account{At: formatUTC(now), Due: []OccurrenceAccount{}, Emitted: []string{}, MayHaveExecuted: []string{}, Unresolved: []string{}, Next: []Deadline{}, Coverage: []PlanningCoverage{}, Uncovered: []string{}}
	err := s.transact(func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT o.id, o.contract_id, o.generation, o.kind, o.nominal, o.covered_count, o.state, o.review_at,
			COALESCE(b.attempts, 0), COALESCE(b.acknowledged, 0)
			FROM occurrences o LEFT JOIN outbox b ON b.id = o.id ORDER BY o.nominal, o.id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item OccurrenceAccount
			var nominal, reviewAt int64
			if err := rows.Scan(&item.ID, &item.Contract.ID, &item.Contract.Generation, &item.Kind, &nominal, &item.CoveredCount, &item.State, &reviewAt, &item.Attempts, &item.Acknowledged); err != nil {
				return err
			}
			item.Nominal = formatUTC(nominal)
			unresolved := oneOf(item.State, StatePending, StateAcknowledged, StateUncertain, StateFailed)
			if unresolved {
				item.ReviewAt = formatUTC(reviewAt)
				account.Unresolved = append(account.Unresolved, item.ID)
				if item.Attempts > 0 {
					account.MayHaveExecuted = append(account.MayHaveExecuted, item.ID)
				}
				if !strings.HasPrefix(item.Kind, returnReviewPrefix) && reviewAt <= 0 {
					account.Uncovered = append(account.Uncovered, "occurrence:"+item.ID)
				}
			}
			if item.Attempts > 0 {
				account.Emitted = append(account.Emitted, item.ID)
			}
			account.Due = append(account.Due, item)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		deadlines, uncovered, err := futureEvaluations(tx, now)
		if err != nil {
			return err
		}
		account.Next = deadlines
		account.Uncovered = append(account.Uncovered, uncovered...)
		contracts, err := loadContracts(tx)
		if err != nil {
			return err
		}
		for _, row := range contracts {
			if !row.current {
				continue
			}
			coverage := PlanningCoverage{Contract: row.ref(), ValidUntil: formatUTC(row.coverage), Ended: now >= row.coverage, Expired: row.expired(now), Retired: row.retired}
			if now < row.pauseUntil {
				coverage.PausedUntil = formatUTC(row.pauseUntil)
			}
			account.Coverage = append(account.Coverage, coverage)
		}
		return nil
	})
	return account, err
}

// NextEvaluation returns the earliest durable future evaluation in Unix
// seconds, or zero when nothing remains to evaluate.
func (s *Store) NextEvaluation(now int64) (int64, error) {
	var next int64
	err := s.transact(func(tx *sql.Tx) error {
		deadlines, _, err := futureEvaluations(tx, now)
		if err == nil && len(deadlines) > 0 {
			next = deadlines[0].unix
		}
		return err
	})
	return next, err
}

// futureEvaluations lists every durable future evaluation in time order: the
// next work instant and planning evaluation of each active, unexpired obligation
// and the review deadline of each unresolved occurrence. Return reviews are
// covered by their parent's deadline.
func futureEvaluations(tx *sql.Tx, now int64) ([]Deadline, []string, error) {
	deadlines := []Deadline{}
	uncovered := []string{}
	add := func(contract Ref, subject string, at int64) {
		deadlines = append(deadlines, Deadline{Contract: contract, Subject: subject, At: formatUTC(at), Overdue: at <= now, unix: at})
	}
	contracts, err := loadContracts(tx)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range contracts {
		if !row.active() || row.expired(now) {
			continue
		}
		if row.planningReview <= 0 {
			uncovered = append(uncovered, "contract:"+row.ref().ID)
		} else if row.planningReview < row.coverage {
			add(row.ref(), KindPlanningReview, row.planningReview)
		} else {
			add(row.ref(), KindFallback, row.planningReview)
		}
		nextWork := row.cursor
		if nextWork < row.pauseUntil {
			nextWork = row.pauseUntil
		}
		if row.cursor < never && nextWork < row.coverage && nextWork < row.expiresAt() {
			add(row.ref(), KindWork, nextWork)
		}
	}
	rows, err := tx.Query(`SELECT id, contract_id, generation, review_at FROM occurrences
		WHERE state IN ` + unresolvedStates + ` AND kind NOT LIKE 'return-review/%' AND review_at > 0`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var contract Ref
		var reviewAt int64
		if err := rows.Scan(&id, &contract.ID, &contract.Generation, &reviewAt); err != nil {
			return nil, nil, err
		}
		add(contract, "return:"+id, reviewAt)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	sort.SliceStable(deadlines, func(i, j int) bool {
		if deadlines[i].unix != deadlines[j].unix {
			return deadlines[i].unix < deadlines[j].unix
		}
		return deadlines[i].Subject < deadlines[j].Subject
	})
	return deadlines, uncovered, nil
}

// Status returns every ledger table for audit, including contract bytes and the
// complete evidence history.
func (s *Store) Status() (map[string][]map[string]any, error) {
	status := map[string][]map[string]any{}
	for _, table := range []string{"contracts", "occurrences", "outbox", "evidence"} {
		records, err := s.dumpTable(table)
		if err != nil {
			return nil, err
		}
		status[table] = records
	}
	return status, nil
}

func (s *Store) dumpTable(table string) ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT * FROM ` + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	records := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		record := map[string]any{}
		for index, value := range values {
			if raw, ok := value.([]byte); ok && json.Valid(raw) {
				value = json.RawMessage(raw)
			} else if ok {
				value = string(raw)
			}
			record[columns[index]] = value
		}
		records = append(records, record)
	}
	return records, rows.Err()
}
