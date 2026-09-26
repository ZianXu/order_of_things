package session_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
	"order_of_things/internal/session"
)

func run(t *testing.T, cfg session.Config) (session.Result, []*platform.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := session.New(cfg)
	result := s.Run(ctx)
	if result.Games != cfg.Games {
		t.Fatalf("completed %d of %d games", result.Games, cfg.Games)
	}
	return result, s.Sequencer().EventLog()
}

// logSignature is the part of the log that must be reproducible. SenderId is
// excluded on purpose: which replica of a component won the race to the sequencer
// is a real-time outcome and is legitimately nondeterministic. Everything else --
// the order, the emitting component, the payload -- is not.
func logSignature(log []*platform.Event) []string {
	sig := make([]string, 0, len(log))
	for _, e := range log {
		sig = append(sig, fmt.Sprintf("%d|%s|%d|%#v",
			e.Header.Seq, e.Header.SenderComponent, e.Header.SenderSeq, e.Payload))
	}
	return sig
}

func TestSessionRunsAFullTournament(t *testing.T) {
	result, log := run(t, session.Config{Seed: 42, Games: 40})

	if want := 40 * 4; len(log) != want {
		t.Errorf("log has %d events, want %d (one game, two decisions, and resolution each)", len(log), want)
	}
	if want := 40 * 4; result.LogLength != want {
		t.Errorf("result.LogLength = %d, want %d", result.LogLength, want)
	}
	if len(result.Quarantined) != 0 {
		t.Errorf("replicas quarantined in a clean run: %v", result.Quarantined)
	}
	if len(result.Leaderboard) != len(fsm.AllStrategies) {
		t.Errorf("leaderboard has %d strategies, want %d", len(result.Leaderboard), len(fsm.AllStrategies))
	}
	// The tracker never plays, so it must never emit.
	for _, e := range log {
		if e.Header.SenderComponent == "game-tracker" {
			t.Error("the tracker emitted an event")
		}
	}
}

// Invariants 1 and 2: the same seed must produce the same event history and the
// same outcome, every time.
func TestSameSeedIsReproducible(t *testing.T) {
	first, firstLog := run(t, session.Config{Seed: 42, Games: 40})
	for i := 1; i < 5; i++ {
		next, nextLog := run(t, session.Config{Seed: 42, Games: 40})
		if !reflect.DeepEqual(logSignature(firstLog), logSignature(nextLog)) {
			t.Fatalf("run %d produced a different event log", i)
		}
		if !reflect.DeepEqual(first.Leaderboard, next.Leaderboard) {
			t.Fatalf("run %d leaderboard = %v, want %v", i, next.Leaderboard, first.Leaderboard)
		}
		if first.StateHash != next.StateHash {
			t.Fatalf("run %d state hash = %x, want %x", i, next.StateHash, first.StateHash)
		}
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	a, aLog := run(t, session.Config{Seed: 42, Games: 40})
	b, bLog := run(t, session.Config{Seed: 43, Games: 40})

	if reflect.DeepEqual(logSignature(aLog), logSignature(bLog)) {
		t.Error("seeds 42 and 43 produced the same log; the seed is not reaching the schedule")
	}
	if a.StateHash == b.StateHash {
		t.Error("seeds 42 and 43 produced the same state hash")
	}
}

// Active-active is only safe because the FSMs are deterministic. Two replicas of
// every component must produce exactly the same log as one, and no duplicates.
func TestActiveActiveMatchesSingleInstance(t *testing.T) {
	single, singleLog := run(t, session.Config{Seed: 42, Games: 40, Replicas: 1})
	paired, pairedLog := run(t, session.Config{Seed: 42, Games: 40, Replicas: 2})

	if !reflect.DeepEqual(logSignature(singleLog), logSignature(pairedLog)) {
		t.Error("the active-active pair produced a different log from the single instance")
	}
	if !reflect.DeepEqual(single.Leaderboard, paired.Leaderboard) {
		t.Errorf("active-active leaderboard = %v, want %v", paired.Leaderboard, single.Leaderboard)
	}
	if single.StateHash != paired.StateHash {
		t.Errorf("active-active state hash = %x, want %x", paired.StateHash, single.StateHash)
	}
	if len(paired.Quarantined) != 0 {
		t.Errorf("replicas quarantined with no bug injected: %v", paired.Quarantined)
	}

	perComponent := make(map[string]int64)
	for _, e := range pairedLog {
		want, seen := perComponent[e.Header.SenderComponent]
		if seen && e.Header.SenderSeq != want {
			t.Fatalf("component %q: SenderSeq %d, want %d -- a duplicate was admitted",
				e.Header.SenderComponent, e.Header.SenderSeq, want)
		}
		perComponent[e.Header.SenderComponent] = e.Header.SenderSeq + 1
	}
}

// Both replicas of a component race for every position, so over a long enough
// run each should win some. A pair where one replica never wins is not actually
// active-active, it is a hot standby.
func TestBothReplicasWinPositions(t *testing.T) {
	_, log := run(t, session.Config{Seed: 42, Games: 200, Replicas: 2})

	wins := make(map[string]map[string]int)
	for _, e := range log {
		if wins[e.Header.SenderComponent] == nil {
			wins[e.Header.SenderComponent] = make(map[string]int)
		}
		wins[e.Header.SenderComponent][e.Header.SenderId]++
	}
	for component, byReplica := range wins {
		if len(byReplica) < 2 {
			t.Logf("component %q: only %v won any position", component, byReplica)
		}
	}
	// Assert on the aggregate rather than per component: a single component
	// losing every race is plausible scheduling luck, all of them is a bug.
	total := make(map[string]int)
	for _, byReplica := range wins {
		for id, n := range byReplica {
			total[id] += n
		}
	}
	if len(total) < 2 {
		t.Errorf("only one replica ever won a position: %v", total)
	}
}

// Every component folds the same ordered stream, so replaying the log from
// scratch must land on exactly the state the live tracker reached -- including
// the chained state hash, which is what a restarting replica is checked against.
func TestReplayingTheLogReproducesTrackerState(t *testing.T) {
	result, log := run(t, session.Config{Seed: 42, Games: 40})

	replay := fsm.NewGameStore()
	for _, e := range log {
		replay.ApplyEvent(e.Header.Seq, e.Payload)
	}
	if !reflect.DeepEqual(replay.Leaderboard(), result.Leaderboard) {
		t.Errorf("replayed leaderboard %v, live tracker has %v", replay.Leaderboard(), result.Leaderboard)
	}
	if replay.StateHash() != result.StateHash {
		t.Errorf("replayed state hash %x, live tracker has %x", replay.StateHash(), result.StateHash)
	}
}

// The read model must be safe to poll from another goroutine while the
// tournament is running, and must converge on the final result.
func TestTrackerSnapshotIsLiveAndConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := session.New(session.Config{Seed: 42, Games: 60})
	polling := make(chan struct{})
	go func() {
		defer close(polling)
		last := 0
		for i := 0; i < 5000; i++ {
			snapshot := s.Tracker().Snapshot()
			if snapshot.Completed < last {
				panic("snapshot went backwards")
			}
			last = snapshot.Completed
		}
	}()

	result := s.Run(ctx)
	<-polling

	final := s.Tracker().Snapshot()
	if final.Completed != result.Games {
		t.Errorf("final snapshot has %d games, result has %d", final.Completed, result.Games)
	}
	if !reflect.DeepEqual(final.Leaderboard, result.Leaderboard) {
		t.Errorf("final snapshot leaderboard %v, result %v", final.Leaderboard, result.Leaderboard)
	}
	if final.StateHash != result.StateHash {
		t.Errorf("final snapshot hash %x, result %x", final.StateHash, result.StateHash)
	}
}

// Sessions are fully independent: running several at once must not let one
// affect another's outcome.
func TestConcurrentSessionsAreIndependent(t *testing.T) {
	reference, _ := run(t, session.Config{Seed: 7, Games: 30})

	results := make(chan session.Result, 6)
	for i := 0; i < 6; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			seed := int64(7)
			if i%2 == 1 {
				seed = 99 // interleave a different tournament
			}
			results <- session.New(session.Config{Seed: seed, Games: 30}).Run(ctx)
		}()
	}
	for i := 0; i < 6; i++ {
		got := <-results
		if got.Seed != 7 {
			continue
		}
		if got.StateHash != reference.StateHash {
			t.Errorf("concurrent session on seed 7 diverged: hash %x, want %x", got.StateHash, reference.StateHash)
		}
	}
}

// The philosophy, pinned. Two runs of the same tournament do not produce the same
// log and never will: both replicas of a player race for every position and which
// one wins is a real-time outcome. What is identical is the logical outcome.
//
// The goal was never to remove the nondeterminism -- it is to confine it to the
// place where it cannot change the answer. This test asserts both halves of that
// at once: the logical signature is invariant, and the identity of the winners
// underneath it is not.
func TestTheRacesVaryWhileTheOutcomeDoesNot(t *testing.T) {
	const runs = 6

	signatures := make(map[string]bool)
	winners := make(map[string]bool)
	outcomes := make(map[uint64]bool)

	for i := 0; i < runs; i++ {
		result, log := run(t, session.Config{Seed: 42, Games: 25, Replicas: 2})
		signatures[strings.Join(logSignature(log), "|")] = true
		outcomes[result.StateHash] = true

		ids := make([]string, 0, len(log))
		for _, e := range log {
			ids = append(ids, e.Header.SenderComponent+"/"+e.Header.SenderId)
		}
		winners[strings.Join(ids, ",")] = true
	}

	if len(signatures) != 1 {
		t.Errorf("%d runs produced %d different logical logs, want 1", runs, len(signatures))
	}
	if len(outcomes) != 1 {
		t.Errorf("%d runs produced %d different outcomes, want 1", runs, len(outcomes))
	}
	// If this ever fails, the pair has stopped racing -- one replica is winning
	// everything and the other is a hot standby, which is not active-active.
	if len(winners) < 2 {
		t.Errorf("%d runs produced the same winners every time; the replicas are not racing", runs)
	}
	t.Logf("%d runs: %d logical log(s), %d outcome(s), %d distinct winner sequences",
		runs, len(signatures), len(outcomes), len(winners))
}
