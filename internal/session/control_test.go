package session_test

import (
	"context"
	"testing"
	"time"

	"order_of_things/internal/golden"
	"order_of_things/internal/session"
)

func started(t *testing.T, cfg session.Config) (*session.Session, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	s := session.New(cfg)
	s.Start(ctx)
	t.Cleanup(cancel)
	return s, cancel
}

// restsAt reports whether the tournament published nothing new for the given
// window.
func restsAt(s *session.Session, window time.Duration) bool {
	before := s.Tracker().Snapshot().Version
	time.Sleep(window)
	return s.Tracker().Snapshot().Version == before
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Pacing is a presentation control: it changes when events are admitted, never
// which or in what order. Two runs of the same seed at different speeds must
// produce the same outcome.
func TestPacingDoesNotChangeTheOutcome(t *testing.T) {
	fast, _ := run(t, session.Config{Seed: 42, Games: 20})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	slow := session.New(session.Config{Seed: 42, Games: 20, Interval: 2 * time.Millisecond}).Run(ctx)

	if slow.StateHash != fast.StateHash {
		t.Errorf("paced run state checksum %016x, unpaced %016x", slow.StateHash, fast.StateHash)
	}
	if slow.LogLength != fast.LogLength {
		t.Errorf("paced run has %d events, unpaced %d", slow.LogLength, fast.LogLength)
	}
}

func TestPauseHoldsTheTournamentAndResumeReleasesIt(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 100, Interval: time.Millisecond})

	waitFor(t, "the tournament to get going", func() bool {
		return s.Tracker().Snapshot().Completed >= 3
	})
	s.Pause()

	// Let anything already in flight settle, then confirm it is genuinely held.
	time.Sleep(50 * time.Millisecond)
	held := s.Tracker().Snapshot().Version
	time.Sleep(100 * time.Millisecond)
	if got := s.Tracker().Snapshot().Version; got != held {
		t.Errorf("advanced from version %d to %d while paused", held, got)
	}
	if running, _ := s.Pacing(); running {
		t.Error("Pacing reports running while paused")
	}

	s.Resume()
	waitFor(t, "the tournament to resume", func() bool {
		return s.Tracker().Snapshot().Version > held
	})
}

func TestStepAdmitsOneEventAtATime(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 100, Interval: time.Hour})
	s.Pause()

	waitFor(t, "the first event to be ready", func() bool {
		return true
	})
	time.Sleep(50 * time.Millisecond)

	before := s.Tracker().Snapshot()
	for i := 1; i <= 4; i++ {
		s.Step()
		want := before.Seq + int64(i)
		waitFor(t, "a stepped event", func() bool {
			return s.Tracker().Snapshot().Seq >= want
		})
		time.Sleep(30 * time.Millisecond)
		if got := s.Tracker().Snapshot().Seq; got != want {
			t.Fatalf("after %d steps seq = %d, want %d", i, got, want)
		}
	}
	// Stepping must not have changed the configured speed.
	if running, interval := s.Pacing(); running || interval != time.Hour {
		t.Errorf("after stepping: running=%v interval=%v, want paused at 1h", running, interval)
	}
}

func TestRoundTakesFourVisibleSteps(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 1, StartPaused: true})

	step := func(seq int64) {
		s.Step()
		waitFor(t, "a stepped event", func() bool {
			return s.Tracker().Snapshot().Seq >= seq
		})
	}

	step(0) // game admitted: both players light up
	game := s.Tracker().Snapshot().CurrentGame
	if game == nil || game.DecisionA != nil || game.DecisionB != nil {
		t.Fatalf("after game admission, current game = %+v; want two undecided players", game)
	}

	step(1) // player A decides
	game = s.Tracker().Snapshot().CurrentGame
	if game == nil || game.DecisionA == nil || game.DecisionB != nil {
		t.Fatalf("after first decision, current game = %+v; want only player A decided", game)
	}

	step(2) // player B decides, but scoring waits for resolution
	snapshot := s.Tracker().Snapshot()
	game = snapshot.CurrentGame
	if game == nil || game.DecisionA == nil || game.DecisionB == nil || snapshot.Completed != 0 {
		t.Fatalf("after second decision, game = %+v completed = %d; want an unresolved decided game", game, snapshot.Completed)
	}

	step(3) // resolution clears both players and publishes scores
	snapshot = s.Tracker().Snapshot()
	if snapshot.CurrentGame != nil || snapshot.Completed != 1 {
		t.Fatalf("after resolution, game = %+v completed = %d; want no current game and one score", snapshot.CurrentGame, snapshot.Completed)
	}
}

func TestSetIntervalTakesEffectImmediately(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 100, Interval: time.Hour})

	// Nothing should move at an hour per event.
	time.Sleep(50 * time.Millisecond)
	stuck := s.Tracker().Snapshot().Version

	s.SetInterval(time.Millisecond)
	waitFor(t, "the speed change to take effect", func() bool {
		return s.Tracker().Snapshot().Version > stuck
	})
	if _, interval := s.Pacing(); interval != time.Millisecond {
		t.Errorf("interval = %v, want 1ms", interval)
	}
}

// Killing both halves of a pair is allowed, and the consequence is visible: the
// tournament blocks as soon as that strategy is scheduled. This is the most
// direct demonstration of why there are two of everything.
func TestKillingBothReplicasStallsTheTournament(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 100, Replicas: 2})

	waitFor(t, "the tournament to get going", func() bool {
		return s.Tracker().Snapshot().Completed >= 3
	})
	for _, replica := range []string{"r0", "r1"} {
		if err := s.Kill("flipper", replica); err != nil {
			t.Fatalf("killing flipper/%s: %v", replica, err)
		}
	}

	// Killing a replica that already had an emission in flight leaves a couple of
	// events still to settle, so wait for the tournament to come to rest rather
	// than for the first moment the stall is reported.
	waitFor(t, "the tournament to come to rest on flipper", func() bool {
		stalled, on := s.Stalled()
		return stalled && on == "flipper" && restsAt(s, 20*time.Millisecond)
	})

	// It really is stuck, not just slow.
	held := s.Tracker().Snapshot().Version
	if !restsAt(s, 200*time.Millisecond) {
		t.Errorf("advanced from version %d to %d while stalled",
			held, s.Tracker().Snapshot().Version)
	}
	if stalled, on := s.Stalled(); !stalled || on != "flipper" {
		t.Errorf("Stalled = %v/%q after settling, want true/flipper", stalled, on)
	}

	// Restarting either half clears it.
	if err := s.Restart("flipper", "r1"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	waitFor(t, "the tournament to recover", func() bool {
		stalled, _ := s.Stalled()
		return !stalled && s.Tracker().Snapshot().Version > held
	})

	result := s.Wait()
	if result.Games != 100 {
		t.Errorf("completed %d of 100 games after recovering", result.Games)
	}
	// A recovery replays previously derived decisions, but the sequencer must
	// reject those copies. Each game has exactly four display events: new game,
	// two decisions, and resolution. If a recovered replica admitted a second
	// decision, this is the first count that would grow.
	if got, want := len(s.Tracker().Snapshot().Feed), 4*result.Games; got != want {
		t.Errorf("feed has %d events after recovery, want %d", got, want)
	}
	if len(result.Quarantined) != 0 {
		t.Errorf("a healthy restart quarantined something: %v", result.Quarantined)
	}
}

func TestStalledIsFalseWhileHealthy(t *testing.T) {
	s, _ := started(t, session.Config{Seed: 42, Games: 30})
	for i := 0; i < 50; i++ {
		if stalled, on := s.Stalled(); stalled {
			t.Fatalf("healthy session reported stalled on %q", on)
		}
		time.Sleep(time.Millisecond)
	}
}

// Restarting a replica with a corrupted decision function: it replays the log,
// re-derives a decision that disagrees with what the log records, and is refused
// rejoin. Note which replica is named -- on the replay path the comparison is
// against recorded history the replica cannot influence, so unlike the live race
// it always identifies the actually-defective half.
func TestRestartWithImpureClockIsRefusedRejoin(t *testing.T) {
	for attempt := 0; attempt < 3; attempt++ {
		s, _ := started(t, session.Config{Seed: 42, Games: 80, Replicas: 2})
		waitFor(t, "the tournament to get going", func() bool {
			return s.Tracker().Snapshot().Completed >= 10
		})
		if err := s.Kill("flipper", "r1"); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the tournament to move on", func() bool {
			return s.Tracker().Snapshot().Completed >= 20
		})
		if err := s.RestartWithBug("flipper", "r1", session.Defect{ImpureClock: true}); err != nil {
			t.Fatal(err)
		}

		result := s.Wait()
		if result.Games != 80 {
			t.Fatalf("attempt %d: completed %d of 80 games", attempt, result.Games)
		}
		if len(result.Quarantined) != 1 || result.Quarantined[0] != "flipper/r1" {
			t.Fatalf("attempt %d: quarantined %v, want exactly [flipper/r1]", attempt, result.Quarantined)
		}
	}
}

// A corrupted transition function is invisible to the emission comparison: Flip
// reads its own last decision, never the scores, so its emissions stay correct
// while its idea of the world rots. Only a reference established before the
// replica ran catches it -- which is also the answer to a defect present in both
// halves of a pair, where the two agree with each other while both being wrong.
func TestCorruptStateIsCaughtOnlyByTheCanonicalChain(t *testing.T) {
	// Corrupt from game 5, and restart at game 20, so the defect falls inside the
	// history the replica has to rebuild. That is the window the chain covers: it
	// is admission control for a replica replaying the past, so a defect that only
	// bites after it has rejoined is not this mechanism's to catch.
	defect := session.Defect{CorruptPayoff: true, CorruptFromGame: 5}

	t.Run("missed without a reference", func(t *testing.T) {
		if got := restartWithDefect(t, defect, nil); len(got) != 0 {
			t.Errorf("quarantined %v without a reference; expected the emission check to miss this", got)
		}
	})

	t.Run("caught with a reference", func(t *testing.T) {
		got := restartWithDefect(t, defect, reference(t, 42, 80))
		if len(got) != 1 || got[0] != "flipper/r1" {
			t.Errorf("quarantined %v, want exactly [flipper/r1]", got)
		}
	})
}

// A replica restarted clean must pass the chain and rejoin.
func TestHealthyRestartPassesTheCanonicalChain(t *testing.T) {
	if got := restartWithDefect(t, session.Defect{}, reference(t, 42, 80)); len(got) != 0 {
		t.Errorf("a clean restart was quarantined: %v", got)
	}
}

func reference(t *testing.T, seed int64, games int) *golden.Validator {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := golden.Open("")
	if err != nil {
		t.Fatalf("golden: %v", err)
	}
	result := session.New(session.Config{Seed: seed, Games: games, Replicas: 1}).Run(ctx)
	if result.Games != games {
		t.Fatalf("reference run reached %d of %d games", result.Games, games)
	}
	if err := store.Record(result.Golden()); err != nil {
		t.Fatalf("recording the reference: %v", err)
	}
	validator := store.Validator(seed, games)
	if validator == nil {
		t.Fatal("no validator after recording a reference")
	}
	return validator
}

func restartWithDefect(t *testing.T, defect session.Defect, ref *golden.Validator) []string {
	t.Helper()
	s, _ := started(t, session.Config{Seed: 42, Games: 80, Replicas: 2, Reference: ref})

	waitFor(t, "the tournament to get going", func() bool {
		return s.Tracker().Snapshot().Completed >= 10
	})
	if err := s.Kill("flipper", "r1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the tournament to move on", func() bool {
		return s.Tracker().Snapshot().Completed >= 20
	})
	if err := s.RestartWithBug("flipper", "r1", defect); err != nil {
		t.Fatal(err)
	}

	result := s.Wait()
	if result.Games != 80 {
		t.Fatalf("completed %d of 80 games", result.Games)
	}
	return result.Quarantined
}

// A defective replica that wins a race puts a bad event into the log, so every
// component's state legitimately stops matching the canonical chain. The chain
// check must not react to that: it is admission control for a replica rebuilding
// the past, not a live policy. Policing it live would quarantine the whole system
// on one bad admission, healthy components included.
func TestOneBadAdmissionDoesNotQuarantineEveryone(t *testing.T) {
	ref := reference(t, 42, 60)

	// Bug a replica from the start, so it races -- and sometimes wins -- rather
	// than only ever replaying.
	s, _ := started(t, session.Config{
		Seed: 42, Games: 60, Replicas: 2, Reference: ref,
		Bug: &session.Bug{Component: "flipper", Replica: "r1",
			Defect: session.Defect{ImpureClock: true}},
	})
	result := s.Wait()

	if result.Games != 60 {
		t.Fatalf("completed %d of 60 games; the system collapsed instead of losing one replica", result.Games)
	}
	if len(result.Quarantined) != 1 {
		t.Fatalf("quarantined %v, want exactly one half of the flipper pair", result.Quarantined)
	}
	if got := result.Quarantined[0]; got != "flipper/r0" && got != "flipper/r1" {
		t.Errorf("quarantined %q, want a flipper replica", got)
	}
	for _, st := range s.Status() {
		if st.Component != "flipper" && st.Quarantined {
			t.Errorf("%s/%s was quarantined by an unrelated replica's bug", st.Component, st.Replica)
		}
	}
}
