package strategy

import (
	"testing"

	"order_of_things/internal/fsm"
)

// pending admits a game at seq and returns it, leaving it unresolved.
func pending(store *fsm.GameStore, seq int64, a, b fsm.Strategy) *fsm.Game {
	store.ApplyEvent(seq, fsm.NewGame{Id: seq, StrategyA: a, StrategyB: b})
	return store.CurrentGame()
}

// resolve settles the game in flight and returns the next free sequence number.
func resolve(store *fsm.GameStore, seq int64, da, db fsm.Decision) int64 {
	game := store.CurrentGame()
	store.ApplyEvent(seq+1, fsm.GameDecision{Strategy: game.StrategyA, Decision: da})
	store.ApplyEvent(seq+2, fsm.GameDecision{Strategy: game.StrategyB, Decision: db})
	return seq + 3
}

// play admits and settles a game in one step.
func play(store *fsm.GameStore, seq int64, a, b fsm.Strategy, da, db fsm.Decision) int64 {
	pending(store, seq, a, b)
	return resolve(store, seq, da, db)
}

func TestCooperateReadsNothing(t *testing.T) {
	// Deliberately given a store and a game it must not consult.
	if got := Cooperate(nil, fsm.Cooperator, nil); got != fsm.Cooperate {
		t.Errorf("Cooperate = %v, want cooperate", got)
	}
	store := fsm.NewGameStore()
	play(store, 0, fsm.Cooperator, fsm.Flipper, fsm.Defect, fsm.Defect)
	if got := Cooperate(store, fsm.Cooperator, nil); got != fsm.Cooperate {
		t.Errorf("Cooperate after a history = %v, want cooperate", got)
	}
}

func TestFlipAlternatesItsOwnDecisions(t *testing.T) {
	store := fsm.NewGameStore()
	game := pending(store, 0, fsm.Flipper, fsm.Cooperator)

	if got := Flip(store, fsm.Flipper, game); got != fsm.Cooperate {
		t.Fatalf("opening move = %v, want cooperate", got)
	}

	seq := resolve(store, 0, fsm.Cooperate, fsm.Cooperate)
	for round, want := range []fsm.Decision{fsm.Defect, fsm.Cooperate, fsm.Defect} {
		game := pending(store, seq, fsm.Flipper, fsm.Cooperator)
		got := Flip(store, fsm.Flipper, game)
		if got != want {
			t.Fatalf("round %d = %v, want %v", round, got, want)
		}
		seq = resolve(store, seq, got, fsm.Cooperate)
	}
}

// Flip reads its own last game whoever it was against, so an intervening game
// against a different opponent still flips it.
func TestFlipIgnoresWhoTheOpponentWas(t *testing.T) {
	store := fsm.NewGameStore()
	seq := play(store, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	seq = play(store, seq, fsm.Retaliator, fsm.CopyLeader, fsm.Defect, fsm.Defect)

	game := pending(store, seq, fsm.Flipper, fsm.CopyLeader)
	if got := Flip(store, fsm.Flipper, game); got != fsm.Cooperate {
		t.Errorf("Flip = %v, want cooperate (flipping its own last defect)", got)
	}
}

func TestRetaliateMirrorsThisOpponentsLastMove(t *testing.T) {
	tests := []struct {
		name         string
		opponentPlay *fsm.Decision
		want         fsm.Decision
	}{
		{"opens by cooperating", nil, fsm.Cooperate},
		{"punishes a defect", ptr(fsm.Defect), fsm.Defect},
		{"forgives once cooperation resumes", ptr(fsm.Cooperate), fsm.Cooperate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := fsm.NewGameStore()
			seq := int64(0)
			if tc.opponentPlay != nil {
				seq = play(store, seq, fsm.Retaliator, fsm.Flipper, fsm.Cooperate, *tc.opponentPlay)
			}
			game := pending(store, seq, fsm.Retaliator, fsm.Flipper)
			if got := Retaliate(store, fsm.Retaliator, game); got != tc.want {
				t.Errorf("Retaliate = %v, want %v", got, tc.want)
			}
		})
	}
}

// Retaliate's view is the pair's history and nothing wider: another strategy
// defecting it does not change how it treats this opponent.
func TestRetaliateScopesToThePair(t *testing.T) {
	store := fsm.NewGameStore()
	seq := play(store, 0, fsm.Retaliator, fsm.Flipper, fsm.Cooperate, fsm.Cooperate)
	seq = play(store, seq, fsm.Retaliator, fsm.CopyLeader, fsm.Cooperate, fsm.Defect)

	againstFlipper := pending(store, seq, fsm.Retaliator, fsm.Flipper)
	if got := Retaliate(store, fsm.Retaliator, againstFlipper); got != fsm.Cooperate {
		t.Errorf("against flipper = %v, want cooperate; copy-leader's defect is not flipper's business", got)
	}
	seq = resolve(store, seq, fsm.Cooperate, fsm.Cooperate)

	againstCopyLeader := pending(store, seq, fsm.Retaliator, fsm.CopyLeader)
	if got := Retaliate(store, fsm.Retaliator, againstCopyLeader); got != fsm.Defect {
		t.Errorf("against copy-leader = %v, want defect", got)
	}
}

func TestCopyLeaderCooperatesBeforeAnyoneLeads(t *testing.T) {
	for _, watermarked := range []bool{true, false} {
		store := fsm.NewGameStore()
		game := pending(store, 0, fsm.CopyLeader, fsm.Cooperator)
		if got := CopyLeader(watermarked)(store, fsm.CopyLeader, game); got != fsm.Cooperate {
			t.Errorf("watermarked=%v: opening move = %v, want cooperate", watermarked, got)
		}
	}
}

func TestCopyLeaderCopiesTheLeadersLastMove(t *testing.T) {
	store := fsm.NewGameStore()
	// Flipper defects a cooperator and takes the lead on +3.
	seq := play(store, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	if got := store.LeadingStrategy(); got != fsm.Flipper {
		t.Fatalf("leader = %q, want flipper (test setup)", got)
	}

	game := pending(store, seq, fsm.CopyLeader, fsm.Retaliator)
	if got := CopyLeader(true)(store, fsm.CopyLeader, game); got != fsm.Defect {
		t.Errorf("CopyLeader = %v, want defect (copying the leader)", got)
	}

	// The leader switches to cooperating; so does CopyLeader.
	seq = resolve(store, seq, fsm.Defect, fsm.Defect)
	seq = play(store, seq, fsm.Flipper, fsm.Retaliator, fsm.Cooperate, fsm.Cooperate)
	if got := store.LeadingStrategy(); got != fsm.Flipper {
		t.Fatalf("leader = %q, want flipper (test setup)", got)
	}
	game = pending(store, seq, fsm.CopyLeader, fsm.Cooperator)
	if got := CopyLeader(true)(store, fsm.CopyLeader, game); got != fsm.Cooperate {
		t.Errorf("CopyLeader = %v, want cooperate", got)
	}
}

// The watermark check: with a game admitted earlier still unresolved, the
// watermarked read declines to answer rather than deciding on a partial past.
// The unwatermarked read answers anyway, which is the injected bug.
func TestWatermarkedCopyLeaderWaitsOnAnUnresolvedEarlierGame(t *testing.T) {
	store := fsm.NewGameStore()
	play(store, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)

	// A game admitted at seq 3 that has not resolved, and ours admitted later.
	earlier := pending(store, 3, fsm.Retaliator, fsm.Cooperator)
	ours := &fsm.Game{Id: 9, Seq: 9, StrategyA: fsm.CopyLeader, StrategyB: fsm.Flipper}

	if store.ResolvedBefore(ours.Seq) {
		t.Fatalf("game %d should be blocking the watermark for seq %d", earlier.Id, ours.Seq)
	}
	if got := CopyLeader(true)(store, fsm.CopyLeader, ours); got != fsm.Unknown {
		t.Errorf("watermarked CopyLeader = %v, want unknown (wait)", got)
	}
	if got := CopyLeader(false)(store, fsm.CopyLeader, ours); got == fsm.Unknown {
		t.Error("unwatermarked CopyLeader waited; the injected bug is supposed to answer regardless")
	}
}

// In v1 there is never more than one game in flight, so the safe and unsafe reads
// agree on every decision. The bug is real but dormant until v2 admits concurrent
// games -- which is exactly why it needs an injection hook to demonstrate.
func TestBothCopyLeaderVariantsAgreeWhileOnlyOneGameIsInFlight(t *testing.T) {
	store := fsm.NewGameStore()
	safe, unsafe := CopyLeader(true), CopyLeader(false)

	seq := int64(0)
	pairs := [][2]fsm.Strategy{
		{fsm.Flipper, fsm.Cooperator},
		{fsm.Retaliator, fsm.Flipper},
		{fsm.CopyLeader, fsm.Cooperator},
		{fsm.Flipper, fsm.CopyLeader},
		{fsm.Retaliator, fsm.Cooperator},
	}
	for round := 0; round < 40; round++ {
		pair := pairs[round%len(pairs)]
		game := pending(store, seq, pair[0], pair[1])

		got, want := safe(store, fsm.CopyLeader, game), unsafe(store, fsm.CopyLeader, game)
		if got != want {
			t.Fatalf("round %d: watermarked read gave %v, unwatermarked gave %v", round, got, want)
		}
		if got == fsm.Unknown {
			t.Fatalf("round %d: watermarked read waited, but only one game is ever in flight", round)
		}

		seq = resolve(store, seq, fsm.Defect, fsm.Cooperate)
	}
}

func TestDecideForCoversEveryStrategy(t *testing.T) {
	for _, s := range fsm.AllStrategies {
		if DecideFor(s, Config{}) == nil {
			t.Errorf("no decision function for %q", s)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("expected a panic for an unknown strategy")
		}
	}()
	DecideFor(fsm.Strategy("nonsense"), Config{})
}

func ptr(d fsm.Decision) *fsm.Decision { return &d }
