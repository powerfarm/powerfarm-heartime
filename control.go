package heartime

import (
	"database/sql"
	"fmt"
)

// Renew accepts a planning return that extends coverage. The plan must be an
// immutable reference; coverage must strictly grow; the next review must lie
// strictly between acceptance and the new end of coverage. The new coverage
// and the next planning evaluation commit together. Nothing else renews a plan:
// not a run succeeding, not a model exiting, not attention expiring.
func (s *Store) Renew(contract Ref, through, review int64, plan string, now int64) error {
	if !validDigest(plan) {
		return fmt.Errorf("%w: plan must be an immutable sha256 reference", ErrInvalidPlan)
	}
	if review <= now || review >= through {
		return fmt.Errorf("%w: next review %s must lie after acceptance at %s and before coverage ends at %s", ErrInvalidPlan, formatUTC(review), formatUTC(now), formatUTC(through))
	}
	return s.transact(func(tx *sql.Tx) error {
		row, err := loadActiveContract(tx, contract, now)
		if err != nil {
			return err
		}
		if now < row.lastClock {
			return fmt.Errorf("%w: renewal at %s precedes evidence at %s", ErrClock, formatUTC(now), formatUTC(row.lastClock))
		}
		if through <= row.coverage {
			return fmt.Errorf("%w: renewal must extend coverage beyond %s", ErrInvalidPlan, formatUTC(row.coverage))
		}
		if _, err := tx.Exec(`UPDATE contracts SET coverage = ?, planning_review = ? WHERE id = ? AND generation = ?`, through, review, contract.ID, contract.Generation); err != nil {
			return err
		}
		return recordEvidence(tx, "", "planning-renewed", now, map[string]any{
			"contract":      contract,
			"plan":          plan,
			"coverageUntil": formatUTC(through),
			"nextReview":    formatUTC(review),
			"previousUntil": formatUTC(row.coverage),
		})
	})
}

// Pause suppresses delivery of ordinary work until resumeAt. It never
// suppresses planning evaluation, fallback or return reviews.
func (s *Store) Pause(contract Ref, resumeAt, now int64, reason string) error {
	if resumeAt <= now || reason == "" {
		return fmt.Errorf("%w: a pause needs a future resume time and a reason", ErrInvalidControl)
	}
	return s.transact(func(tx *sql.Tx) error {
		row, err := loadActiveContract(tx, contract, now)
		if err != nil {
			return err
		}
		if now < row.lastClock {
			return fmt.Errorf("%w: pause at %s precedes evidence at %s", ErrClock, formatUTC(now), formatUTC(row.lastClock))
		}
		if _, err := tx.Exec(`UPDATE contracts SET pause_until = ? WHERE id = ? AND generation = ?`, resumeAt, contract.ID, contract.Generation); err != nil {
			return err
		}
		return recordEvidence(tx, "", "control-pause", now, map[string]any{"contract": contract, "resumeAt": formatUTC(resumeAt), "reason": reason})
	})
}

// Resume ends a pause early. The recurrence keeps its original anchor, so
// instants missed during the pause follow the contract's catch-up policy.
func (s *Store) Resume(contract Ref, now int64, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: resuming needs a reason", ErrInvalidControl)
	}
	return s.transact(func(tx *sql.Tx) error {
		if _, err := loadActiveContract(tx, contract, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE contracts SET pause_until = 0 WHERE id = ? AND generation = ?`, contract.ID, contract.Generation); err != nil {
			return err
		}
		return recordEvidence(tx, "", "control-resume", now, map[string]any{"contract": contract, "reason": reason})
	})
}

// Retire explicitly ends future work and planning evaluation of an obligation.
// Undelivered work lapses at once. Delivered occurrences that remain unresolved
// keep their return reviews: retirement never erases owed returns.
func (s *Store) Retire(contract Ref, now int64, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: retirement needs a reason", ErrInvalidControl)
	}
	return s.transact(func(tx *sql.Tx) error {
		row, err := loadActiveContract(tx, contract, now)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE contracts SET retired = 1 WHERE id = ? AND generation = ?`, contract.ID, contract.Generation); err != nil {
			return err
		}
		if err := lapseUndelivered(tx, row.ref(), ordinaryKinds, "obligation retired: "+reason, now); err != nil {
			return err
		}
		return recordEvidence(tx, "", "control-retire", now, map[string]any{"contract": contract, "reason": reason})
	})
}
