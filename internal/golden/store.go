// Package golden persists the canonical outcome of a seed.
//
// Everything else about a session is in memory and dies with it: the sequencer,
// the components, the event log. That is fine for the log, which is large and
// rederivable, but it leaves nothing to check a replay against. A restarting
// replica can only be validated against a live sibling that happens to still
// exist in the same process, which is exactly the case that has failed if the
// process died.
//
// So one small thing is durable: for seed X over N games, the canonical state
// root at every position in the stream. Not just the final one -- a replica that
// is part way through replaying needs to be checked where it currently is, and
// waiting until it finishes to find out it was wrong at event three defeats the
// purpose of checking before it rejoins.
//
// The chain is small: four events per game, eight bytes each, so a hundred-game
// tournament is a couple of kilobytes.
package golden

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"order_of_things/internal/fsm"
)

// Event is one admitted event of the canonical tournament, in a form that
// survives being written to disk and read back.
type Event struct {
	Seq       int64             `json:"seq"`
	Component string            `json:"component"`
	NewGame   *fsm.NewGame      `json:"newGame,omitempty"`
	Decision  *fsm.GameDecision `json:"decision,omitempty"`
	Resolved  *fsm.GameResolved `json:"resolved,omitempty"`
}

// Payload rebuilds the value the component originally emitted.
func (e Event) Payload() any {
	switch {
	case e.NewGame != nil:
		return *e.NewGame
	case e.Decision != nil:
		return *e.Decision
	case e.Resolved != nil:
		return *e.Resolved
	default:
		return nil
	}
}

// NewEvent captures an admitted event.
func NewEvent(seq int64, component string, payload any) Event {
	event := Event{Seq: seq, Component: component}
	switch v := payload.(type) {
	case fsm.NewGame:
		event.NewGame = &v
	case fsm.GameDecision:
		event.Decision = &v
	case fsm.GameResolved:
		event.Resolved = &v
	}
	return event
}

// Outcome is the canonical result of a tournament.
type Outcome struct {
	Seed        int64                  `json:"seed"`
	Games       int                    `json:"games"`
	StateHash   uint64                 `json:"stateHash"`
	Leaderboard []fsm.LeaderboardEntry `json:"leaderboard"`
	// Chain is the state checksum after each event, indexed by sequence number. A
	// replica that has replayed up to seq N is checked against Chain[N].
	Chain []uint64 `json:"chain,omitempty"`
	// Log is the canonical tournament in full. A replica rejoining is made to
	// replay all of it privately before it is allowed near the live stream --
	// which is the only way to catch a defect that has not happened yet. A
	// corrupted decision function looks perfect until it is asked to decide, and
	// what has been logged so far may not have asked it.
	Log []Event `json:"log,omitempty"`
}

// RootAt returns the canonical state checksum after the event at seq, and false if
// the reference does not reach that far.
func (o Outcome) RootAt(seq int64) (uint64, bool) {
	if seq < 0 || seq >= int64(len(o.Chain)) {
		return 0, false
	}
	return o.Chain[seq], true
}

// DivergenceError reports that a replica's replayed state does not match the
// canonical chain. Unlike a disagreement between two live replicas, this does
// say which side is wrong: the reference was computed before either replica ran.
type DivergenceError struct {
	Seed     int64
	Seq      int64
	Expected uint64
	Got      uint64
}

func (e *DivergenceError) Error() string {
	return fmt.Sprintf(
		"replayed state diverges from the canonical chain for seed %d at seq %d: state checksum %016x, want %016x",
		e.Seed, e.Seq, e.Got, e.Expected,
	)
}

// Validator checks a replica's state checksum against the canonical chain as it
// replays. A replica that fails is refused rejoin rather than being allowed to
// serve, which is the point of checking at all.
type Validator struct {
	outcome Outcome
}

// NewValidator builds a checker straight from an outcome, without going through
// the store. The runtime uses this: it regenerates the canonical tournament and
// validates against what it just computed, never against a file.
func NewValidator(outcome Outcome) *Validator {
	return &Validator{outcome: outcome}
}

// Log is the canonical tournament, for a replica to rehearse against.
func (v *Validator) Log() []Event {
	if v == nil {
		return nil
	}
	return v.outcome.Log
}

// Root returns the canonical state checksum after the event at seq.
func (v *Validator) Root(seq int64) (uint64, bool) {
	if v == nil {
		return 0, false
	}
	return v.outcome.RootAt(seq)
}

// Seed is the tournament this validator describes.
func (v *Validator) Seed() int64 {
	if v == nil {
		return 0
	}
	return v.outcome.Seed
}

// Validate reports whether the state checksum a replica computed after applying the
// event at seq matches the canonical one. Positions past the end of the chain
// are not an error: a session may legitimately run beyond the reference.
func (v *Validator) Validate(seq int64, root uint64) error {
	if v == nil {
		return nil
	}
	expected, ok := v.outcome.RootAt(seq)
	if !ok || expected == root {
		return nil
	}
	return &DivergenceError{Seed: v.outcome.Seed, Seq: seq, Expected: expected, Got: root}
}

func (o Outcome) key() string { return fmt.Sprintf("%d:%d", o.Seed, o.Games) }

// MismatchError reports that a run disagreed with the recorded canonical result
// for its seed. The recorded outcome is authoritative; the run is suspect.
type MismatchError struct {
	Golden Outcome
	Got    Outcome
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf(
		"outcome for seed %d over %d games does not match the golden record: state checksum %016x, want %016x",
		e.Got.Seed, e.Got.Games, e.Got.StateHash, e.Golden.StateHash,
	)
}

// Store is a small durable map from seed to canonical outcome. It is safe for
// concurrent use: unlike the state machines, it is genuinely shared between the
// sessions a server is running at once.
type Store struct {
	mu       sync.Mutex
	path     string
	outcomes map[string]Outcome
}

// Open loads the store at path, creating it if it does not exist. A path of ""
// gives an in-memory store that persists nothing, which is what tests and
// ephemeral sessions want.
func Open(path string) (*Store, error) {
	s := &Store{path: path, outcomes: make(map[string]Outcome)}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("golden: reading %s: %w", path, err)
	}
	var records []Outcome
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("golden: parsing %s: %w", path, err)
	}
	for _, record := range records {
		s.outcomes[record.key()] = record
	}
	return s, nil
}

// Get returns the canonical outcome for a seed and game count, if one is
// recorded.
func (s *Store) Get(seed int64, games int) (Outcome, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	outcome, ok := s.outcomes[Outcome{Seed: seed, Games: games}.key()]
	return outcome, ok
}

// Validator returns a checker for the canonical chain of a seed, or nil if there
// is no reference for it. A nil Validator passes everything, so callers do not
// have to special-case the first run of a seed.
func (s *Store) Validator(seed int64, games int) *Validator {
	outcome, ok := s.Get(seed, games)
	if !ok || len(outcome.Chain) == 0 || len(outcome.Log) == 0 {
		return nil
	}
	return &Validator{outcome: outcome}
}

// Verify checks a run against the canonical record, recording it as canonical if
// this is the first run for that seed. First run wins: there is nothing to
// arbitrate against, so the first observation defines the reference and every
// later run is checked against it.
func (s *Store) Verify(got Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.outcomes[got.key()]
	if !ok {
		s.outcomes[got.key()] = got
		// Persist under the same lock that guards the map, so concurrent writers
		// serialize: the file always reflects a consistent snapshot, and two
		// writers can never race over the same temporary file.
		return s.flushLocked()
	}
	if existing.StateHash != got.StateHash {
		return &MismatchError{Golden: existing, Got: got}
	}
	return nil
}

// Record stores an outcome as canonical, replacing any existing record. Used
// when a session pre-generates its own reference, where there is nothing to
// verify against and the point is to establish the reference in the first place.
func (s *Store) Record(outcome Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomes[outcome.key()] = outcome
	return s.flushLocked()
}

// flushLocked writes the whole store to disk. The caller must hold s.mu.
func (s *Store) flushLocked() error {
	if s.path == "" {
		return nil
	}
	records := make([]Outcome, 0, len(s.outcomes))
	for _, outcome := range s.outcomes {
		records = append(records, outcome)
	}

	// Sorted so the file is stable across writes and readable in a diff.
	sort.Slice(records, func(i, j int) bool {
		if records[i].Seed != records[j].Seed {
			return records[i].Seed < records[j].Seed
		}
		return records[i].Games < records[j].Games
	})
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("golden: encoding: %w", err)
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("golden: creating %s: %w", dir, err)
	}

	// Write to a uniquely named temporary in the same directory and rename over
	// the target, so a crash mid-write cannot leave a truncated reference and a
	// stale temporary from a previous crash cannot be mistaken for this one.
	tmp, err := os.CreateTemp(dir, ".golden-*.json")
	if err != nil {
		return fmt.Errorf("golden: creating a temporary file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("golden: writing %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("golden: closing %s: %w", tmp.Name(), err)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return fmt.Errorf("golden: setting permissions on %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("golden: renaming into %s: %w", s.path, err)
	}
	return nil
}
