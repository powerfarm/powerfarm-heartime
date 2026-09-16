package heartime

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Delivery is one occurrence waiting for an acknowledged delivery.
type Delivery struct {
	ID          string          `json:"id"`
	Attempts    int             `json:"attempts"`
	LastAttempt int64           `json:"lastAttempt,omitempty"`
	Body        json.RawMessage `json:"body"`
}

// Outcome is execution feedback reported by the receiving relationship.
type Outcome string

// Reportable outcomes. Transport acknowledgement is never an outcome.
const (
	OutcomeVerified  Outcome = StateVerified
	OutcomeFailed    Outcome = StateFailed
	OutcomeUncertain Outcome = StateUncertain
	// OutcomeContained means failure effects were independently reconciled or
	// bounded and the occurrence no longer owns active work. It never means the
	// intended work succeeded.
	OutcomeContained Outcome = StateContained
)

// Pending lists occurrences whose delivery is not yet acknowledged, oldest
// first. It is a read model: Attempt rechecks every admission rule.
func (s *Store) Pending() ([]Delivery, error) {
	rows, err := s.db.Query(`SELECT o.id, b.attempts, COALESCE(b.last_attempt, 0), o.body
		FROM outbox b JOIN occurrences o ON o.id = b.id
		WHERE b.acknowledged = 0 AND o.state = 'pending' ORDER BY o.nominal, o.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := []Delivery{}
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(&delivery.ID, &delivery.Attempts, &delivery.LastAttempt, &delivery.Body); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

// Attempt durably records a delivery attempt before any byte is sent. After a
// crash the occurrence therefore counts as possibly delivered. Ordinary work is
// admitted only inside active coverage and under the contract's overlap policy.
func (s *Store) Attempt(id string, now int64) error {
	return s.transact(func(tx *sql.Tx) error {
		var (
			contract     Ref
			kind, state  string
			nominal      int64
			attempts     int
			acknowledged bool
		)
		err := tx.QueryRow(`SELECT o.contract_id, o.generation, o.kind, o.state, o.nominal, b.attempts, b.acknowledged
			FROM occurrences o JOIN outbox b ON b.id = o.id WHERE o.id = ?`, id).Scan(&contract.ID, &contract.Generation, &kind, &state, &nominal, &attempts, &acknowledged)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s has no delivery intent", ErrNotPending, id)
		}
		if err != nil {
			return err
		}
		if state != StatePending || acknowledged {
			return fmt.Errorf("%w: %s is %s", ErrNotPending, id, state)
		}
		if kind == KindWork {
			row, err := scanContract(tx.QueryRow(`SELECT `+contractColumns+` FROM contracts WHERE id = ? AND generation = ?`, contract.ID, contract.Generation).Scan)
			if err != nil {
				return err
			}
			if reason := row.workBlocked(now); reason != "" {
				return fmt.Errorf("%w: %s", ErrOutsideCoverage, reason)
			}
			// Overlap admits an occurrence once, at its first attempt. A retry of
			// possibly delivered work must stay possible, or it could deadlock
			// against newer work that its own unresolved state defers.
			if attempts == 0 {
				if err := admitOverlap(tx, row, id, nominal, now); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(`UPDATE outbox SET attempts = attempts + 1, last_attempt = ? WHERE id = ?`, now, id); err != nil {
			return err
		}
		return recordEvidence(tx, id, "publication-attempt", now, map[string]any{"attempt": attempts + 1, "effectCertainty": "unknown"})
	})
}

// admitOverlap applies the overlap policy against delivered work of the same
// contract, across generations, that has not been resolved.
func admitOverlap(tx *sql.Tx, row contractRow, id string, nominal, now int64) error {
	overlap := row.terms.Spec.Overlap
	var unresolved int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM occurrences o JOIN outbox b ON b.id = o.id
		WHERE o.contract_id = ? AND o.id != ? AND o.kind = 'work' AND b.attempts > 0 AND o.state IN `+unresolvedStates,
		row.ref().ID, id).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved > 0 && overlap != OverlapAllow {
		return fmt.Errorf("%w: %d delivered occurrence(s) of %s unresolved", ErrOverlapDeferred, unresolved, row.ref().ID)
	}
	if overlap != OverlapCoalesce {
		return nil
	}
	var newest string
	if err := tx.QueryRow(`SELECT o.id FROM occurrences o JOIN outbox b ON b.id = o.id
		WHERE o.contract_id = ? AND o.generation = ? AND o.kind = 'work' AND o.state = 'pending' AND b.attempts = 0
		ORDER BY o.nominal DESC, o.id DESC LIMIT 1`, row.ref().ID, row.ref().Generation).Scan(&newest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if newest != "" && newest != id {
		return fmt.Errorf("%w: %s", ErrNewerPending, newest)
	}
	older, err := queryIDs(tx, `SELECT o.id FROM occurrences o JOIN outbox b ON b.id = o.id
		WHERE o.contract_id = ? AND o.generation = ? AND o.kind = 'work' AND o.state = 'pending' AND b.attempts = 0 AND o.nominal < ?
		ORDER BY o.nominal, o.id`, row.ref().ID, row.ref().Generation, nominal)
	if err != nil {
		return err
	}
	for _, olderID := range older {
		if _, err := tx.Exec(`UPDATE occurrences SET state = ? WHERE id = ?`, StateCoalesced, olderID); err != nil {
			return err
		}
		if err := recordEvidence(tx, olderID, "disposition-coalesced", now, map[string]string{"into": id}); err != nil {
			return err
		}
	}
	return nil
}

// Acknowledge records that the receiver acknowledged delivery. It proves
// delivery only; the effect remains unknown until reported.
func (s *Store) Acknowledge(id string, now int64) error {
	return s.transact(func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE outbox SET acknowledged = 1 WHERE id = ? AND attempts > 0`, id)
		if err != nil {
			return err
		}
		if changed, err := result.RowsAffected(); err != nil {
			return err
		} else if changed != 1 {
			return fmt.Errorf("%w: %s", ErrNotPublished, id)
		}
		if _, err := tx.Exec(`UPDATE occurrences SET state = ? WHERE id = ? AND state = ?`, StateAcknowledged, id, StatePending); err != nil {
			return err
		}
		return recordEvidence(tx, id, "transport-acknowledgement", now, map[string]bool{"verified": false})
	})
}

// Report records authenticated execution feedback bound to an immutable
// evidence digest. Failure or uncertainty keeps the occurrence unresolved and
// makes its fallback review due at once. Verified and contained reports are
// final: an identical replay is accepted, any other report conflicts.
func (s *Store) Report(id string, outcome Outcome, evidence string, now int64) error {
	if !oneOf(string(outcome), StateVerified, StateFailed, StateUncertain, StateContained) || !validDigest(evidence) {
		return fmt.Errorf("%w: got outcome %q and evidence %q", ErrInvalidReport, outcome, evidence)
	}
	return s.transact(func(tx *sql.Tx) error {
		var state string
		var attempts int
		err := tx.QueryRow(`SELECT o.state, COALESCE(b.attempts, 0) FROM occurrences o LEFT JOIN outbox b ON b.id = o.id WHERE o.id = ?`, id).Scan(&state, &attempts)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s is not a known occurrence", ErrNotPublished, id)
		}
		if err != nil {
			return err
		}
		if attempts == 0 {
			return fmt.Errorf("%w: %s was never delivered, so no execution can be reported", ErrNotPublished, id)
		}
		if state == StateVerified || state == StateContained {
			var body []byte
			if err := tx.QueryRow(`SELECT body FROM evidence WHERE occurrence_id = ? AND kind = 'execution-report' ORDER BY seq DESC LIMIT 1`, id).Scan(&body); err != nil {
				return err
			}
			var previous struct {
				Outcome  string `json:"outcome"`
				Evidence string `json:"evidence"`
			}
			if err := json.Unmarshal(body, &previous); err != nil {
				return err
			}
			if previous.Outcome == string(outcome) && strings.EqualFold(previous.Evidence, evidence) {
				return nil
			}
			return fmt.Errorf("%w: %s is already %s with evidence %s", ErrConflict, id, state, previous.Evidence)
		}
		if _, err := tx.Exec(`UPDATE occurrences SET state = ?, review_at = ? WHERE id = ?`, string(outcome), now, id); err != nil {
			return err
		}
		return recordEvidence(tx, id, "execution-report", now, map[string]string{"outcome": string(outcome), "evidence": evidence})
	})
}
