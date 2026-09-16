// Package heartime keeps temporal obligations and temporal evidence for
// Powerfarm contracts.
//
// Heartime establishes that something is due and guarantees that every active
// obligation keeps a durable future evaluation. It never decides what work to
// do, never executes a work graph, and never treats transport success as an
// executed effect. Execution belongs to the relationship named by each
// occurrence's handoff reference.
package heartime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// APIVersion and Kind identify the HeartimeContract representation defined in
// powerfarm-specs/specs/HEARTIME_CONTRACT_v0.md.
const (
	APIVersion = "powerfarm.specs/v0"
	Kind       = "HeartimeContract"
)

// Catch-up policies decide what happens to nominal instants missed while
// Heartime was not evaluating.
const (
	CatchUpAll    = "all"
	CatchUpLatest = "latest"
	CatchUpSkip   = "skip"
)

// Overlap policies decide whether new work is delivered while earlier delivered
// work of the same contract remains unresolved.
const (
	OverlapAllow    = "allow"
	OverlapDefer    = "defer"
	OverlapCoalesce = "coalesce"
)

// FallbackModes are the contractual meanings of a missed or failed planning
// renewal. Heartime only emits the temporal condition; the downstream
// relationship enforces the meaning.
var FallbackModes = []string{"continue_previous", "reduce_scope", "retry_at", "alternate_planner", "safe_mode", "suspend"}

// maxSeconds bounds every contractual duration to ten years.
const maxSeconds int64 = 315360000

// never is the cursor of a one-shot obligation that has already occurred.
const never int64 = 1 << 62

// utcLayout is the only accepted timestamp profile: RFC 3339 UTC, whole seconds.
const utcLayout = "2006-01-02T15:04:05Z"

// Identity patterns mirror schemas/common.schema.json.
var (
	contractIDPattern = regexp.MustCompile(`^pf\.contract(?:\.[a-z0-9][a-z0-9-]*)+$`)
	entityIDPattern   = regexp.MustCompile(`^pf(?:\.[a-z0-9][a-z0-9-]*)+$`)
)

// Errors returned by Heartime are part of its contract.
var (
	ErrInvalidContract  = errors.New("invalid heartime contract")
	ErrConflict         = errors.New("conflicting immutable state")
	ErrClock            = errors.New("clock moved backwards")
	ErrUnknownContract  = errors.New("no active contract generation matches")
	ErrNotPending       = errors.New("occurrence is not pending delivery")
	ErrOutsideCoverage  = errors.New("ordinary work is outside active coverage")
	ErrOverlapDeferred  = errors.New("overlap policy defers delivery until earlier work is resolved")
	ErrNewerPending     = errors.New("a newer undelivered occurrence will be delivered instead")
	ErrNotPublished     = errors.New("occurrence has no publication attempt")
	ErrInvalidReport    = errors.New("report requires an explicit outcome and an immutable evidence digest")
	ErrInvalidPlan      = errors.New("invalid planning renewal")
	ErrInvalidControl   = errors.New("invalid control request")
	errUnsupportedValue = errors.New("unsupported value")
)

// Ref identifies one generation of a contract.
type Ref struct {
	ID         string `json:"id"`
	Generation int    `json:"generation"`
}

// Metadata is the common contract identity.
type Metadata struct {
	ID         string `json:"id"`
	Generation int    `json:"generation"`
	Subject    string `json:"subject"`
	Owner      string `json:"owner"`
}

// Recurrence is an anchored fixed-second recurrence. EverySeconds zero is a
// one-shot obligation. Civil calendar recurrence is deliberately not supported
// by this profile and must not be approximated with seconds.
type Recurrence struct {
	Anchor       string `json:"anchor"`
	EverySeconds int64  `json:"everySeconds"`
}

// Fallback names the relationship invoked when planning coverage is missed or
// a delivered occurrence returns failure or uncertainty.
type Fallback struct {
	Mode               string `json:"mode"`
	ReviewAfterSeconds int64  `json:"reviewAfterSeconds"`
	Handoff            Ref    `json:"handoff"`
}

// Planning is the coverage of the current period and its next review.
type Planning struct {
	PlanValidThrough     string `json:"planValidThrough"`
	NextPlanningReviewAt string `json:"nextPlanningReviewAt"`
	ReviewEverySeconds   int64  `json:"reviewEverySeconds"`
	Handoff              Ref    `json:"handoff"`
}

// Terms declare one temporal obligation of an existing responsibility.
type Terms struct {
	ObligationID       string     `json:"obligationId"`
	Responsibility     Ref        `json:"responsibility"`
	Recurrence         Recurrence `json:"recurrence"`
	GraceSeconds       int64      `json:"graceSeconds"`
	CatchUp            string     `json:"catchUp"`
	MaxBatch           int        `json:"maxBatch"`
	Overlap            string     `json:"overlap"`
	ReviewAfterSeconds int64      `json:"reviewAfterSeconds"`
	ExpiresAt          string     `json:"expiresAt,omitempty"`
	Handoff            Ref        `json:"handoff"`
	Fallback           Fallback   `json:"fallback"`
	Planning           Planning   `json:"planning"`
}

// Contract is one immutable generation of HeartimeContract terms.
type Contract struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Terms    `json:"spec"`
}

// Ref returns the identity of this contract generation.
func (c Contract) Ref() Ref { return Ref{ID: c.Metadata.ID, Generation: c.Metadata.Generation} }

// Parse decodes exactly one contract document. Unknown fields, trailing
// documents and implicit zero values for grace or recurrence are rejected so
// that no temporal term is ever assumed.
func Parse(raw []byte) (Contract, error) {
	var contract Contract
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		return contract, fmt.Errorf("%w: %v", ErrInvalidContract, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return contract, fmt.Errorf("%w: trailing JSON after the contract document", ErrInvalidContract)
	}
	var fields struct {
		Spec struct {
			GraceSeconds json.RawMessage `json:"graceSeconds"`
			Recurrence   struct {
				EverySeconds json.RawMessage `json:"everySeconds"`
			} `json:"recurrence"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return contract, fmt.Errorf("%w: %v", ErrInvalidContract, err)
	}
	for _, value := range []json.RawMessage{fields.Spec.GraceSeconds, fields.Spec.Recurrence.EverySeconds} {
		if len(value) == 0 || bytes.Equal(value, []byte("null")) {
			return contract, fmt.Errorf("%w: graceSeconds and recurrence.everySeconds must be explicit", ErrInvalidContract)
		}
	}
	return contract, contract.Validate()
}

// Validate checks every term Heartime relies on to keep the obligation covered.
func (c Contract) Validate() error {
	invalid := func(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidContract, reason) }
	terms := c.Spec
	if c.APIVersion != APIVersion || c.Kind != Kind {
		return invalid("apiVersion and kind must identify a HeartimeContract v0")
	}
	if !validRef(c.Ref()) || !entityIDPattern.MatchString(c.Metadata.Subject) || !entityIDPattern.MatchString(c.Metadata.Owner) {
		return invalid("metadata must carry a contract id, positive generation, subject and owner")
	}
	if terms.ObligationID == "" {
		return invalid("obligationId is required")
	}
	references := []struct {
		name string
		ref  Ref
	}{
		{"responsibility", terms.Responsibility},
		{"handoff", terms.Handoff},
		{"fallback.handoff", terms.Fallback.Handoff},
		{"planning.handoff", terms.Planning.Handoff},
	}
	for _, reference := range references {
		if !validRef(reference.ref) {
			return invalid(reference.name + " must reference a contract id and generation")
		}
	}
	anchor, err := parseUTC(terms.Recurrence.Anchor)
	if err != nil {
		return invalid("recurrence.anchor: " + err.Error())
	}
	coverageEnd, err := parseUTC(terms.Planning.PlanValidThrough)
	if err != nil {
		return invalid("planning.planValidThrough: " + err.Error())
	}
	review, err := parseUTC(terms.Planning.NextPlanningReviewAt)
	if err != nil {
		return invalid("planning.nextPlanningReviewAt: " + err.Error())
	}
	if review < anchor || review >= coverageEnd {
		return invalid("planning review must lie within the initial coverage and before it ends")
	}
	if terms.ExpiresAt != "" {
		expires, err := parseUTC(terms.ExpiresAt)
		if err != nil {
			return invalid("expiresAt: " + err.Error())
		}
		if expires <= anchor {
			return invalid("expiresAt must follow the recurrence anchor")
		}
	}
	if terms.Recurrence.EverySeconds < 0 || terms.Recurrence.EverySeconds > maxSeconds || terms.GraceSeconds < 0 || terms.GraceSeconds > maxSeconds {
		return invalid("recurrence.everySeconds and graceSeconds must be between 0 and ten years")
	}
	if terms.MaxBatch < 1 || terms.MaxBatch > 1000 {
		return invalid("maxBatch must be between 1 and 1000")
	}
	intervals := []struct {
		name    string
		seconds int64
	}{
		{"reviewAfterSeconds", terms.ReviewAfterSeconds},
		{"fallback.reviewAfterSeconds", terms.Fallback.ReviewAfterSeconds},
		{"planning.reviewEverySeconds", terms.Planning.ReviewEverySeconds},
	}
	for _, interval := range intervals {
		if interval.seconds < 1 || interval.seconds > maxSeconds {
			return invalid(interval.name + " must be between 1 second and ten years")
		}
	}
	if !oneOf(terms.CatchUp, CatchUpAll, CatchUpLatest, CatchUpSkip) {
		return invalid("catchUp must be all, latest or skip")
	}
	if !oneOf(terms.Overlap, OverlapAllow, OverlapDefer, OverlapCoalesce) {
		return invalid("overlap must be allow, defer or coalesce")
	}
	if !oneOf(terms.Fallback.Mode, FallbackModes...) {
		return invalid("fallback.mode is not a supported fallback meaning")
	}
	return nil
}

// Canonical returns RFC 8785 canonical JSON. A digest is always computed over
// these bytes and never stored inside them.
func Canonical(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || (raw[0] != '{' && raw[0] != '[') {
		return nil, fmt.Errorf("%w: canonical documents must be JSON objects or arrays", errUnsupportedValue)
	}
	return jsoncanonicalizer.Transform(raw)
}

// Digest returns the content identity of bytes as sha256:<hex>.
func Digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// OccurrenceID is the restart-stable identity of one nominal temporal
// satisfaction: sha256(RFC8785([contractId, generation, obligationId, kind, nominalUTC])).
func OccurrenceID(contract Ref, obligationID, kind string, nominal int64) (string, error) {
	canonical, err := Canonical([]any{contract.ID, contract.Generation, obligationID, kind, formatUTC(nominal)})
	if err != nil {
		return "", err
	}
	return Digest(canonical), nil
}

// ParseUTC parses the accepted timestamp profile into Unix seconds.
func ParseUTC(value string) (int64, error) { return parseUTC(value) }

// FormatUTC formats Unix seconds in the accepted timestamp profile.
func FormatUTC(unix int64) string { return formatUTC(unix) }

func parseUTC(value string) (int64, error) {
	instant, err := time.Parse(utcLayout, value)
	if err != nil {
		return 0, fmt.Errorf("timestamp %q must be RFC 3339 UTC with whole seconds", value)
	}
	return instant.Unix(), nil
}

func formatUTC(unix int64) string { return time.Unix(unix, 0).UTC().Format(utcLayout) }

func validRef(ref Ref) bool { return contractIDPattern.MatchString(ref.ID) && ref.Generation > 0 }

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(value[len("sha256:"):])
	return err == nil && strings.ToLower(value) == value
}

func oneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}
