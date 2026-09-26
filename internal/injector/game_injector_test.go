package injector

import (
	"testing"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
)

func TestPairingNeverPitsAStrategyAgainstItself(t *testing.T) {
	for _, seed := range []int64{0, 1, 42, -7, 1 << 40} {
		injector := &GameInjector{seed: seed}
		for id := int64(0); id < 10000; id++ {
			a, b := injector.pairing(id)
			if a == b {
				t.Fatalf("seed %d game %d paired %q against itself", seed, id, a)
			}
		}
	}
}

func TestPairingIsAPureFunctionOfSeedAndPosition(t *testing.T) {
	injector := &GameInjector{seed: 42}
	want := make(map[int64][2]fsm.Strategy)
	for id := int64(0); id < 500; id++ {
		a, b := injector.pairing(id)
		want[id] = [2]fsm.Strategy{a, b}
	}

	// A replica that computes pairings out of order, or that computes the same one
	// twice, must get identical answers -- it carries no generator state.
	fresh := &GameInjector{seed: 42}
	for id := int64(499); id >= 0; id-- {
		for repeat := 0; repeat < 3; repeat++ {
			a, b := fresh.pairing(id)
			if got := [2]fsm.Strategy{a, b}; got != want[id] {
				t.Fatalf("game %d: got %v, want %v", id, got, want[id])
			}
		}
	}
}

func TestPairingUsesEveryStrategy(t *testing.T) {
	injector := &GameInjector{seed: 42}
	appearances := make(map[fsm.Strategy]int)
	const games = 4000
	for id := int64(0); id < games; id++ {
		a, b := injector.pairing(id)
		appearances[a]++
		appearances[b]++
	}
	for _, s := range fsm.AllStrategies {
		// Uniform would be games/2 each; a very loose bound is enough to catch a
		// strategy that can never be drawn.
		if got := appearances[s]; got < games/4 {
			t.Errorf("%q appeared %d times in %d games, want roughly %d", s, got, games, games/2)
		}
	}
}

func TestDifferentSeedsGiveDifferentSchedules(t *testing.T) {
	a, b := &GameInjector{seed: 42}, &GameInjector{seed: 43}
	same := 0
	const games = 200
	for id := int64(0); id < games; id++ {
		x1, x2 := a.pairing(id)
		y1, y2 := b.pairing(id)
		if x1 == y1 && x2 == y2 {
			same++
		}
	}
	if same == games {
		t.Fatal("seeds 42 and 43 produce identical schedules; the seed is not reaching the draw")
	}
}

func newInjector(seed int64, maxGames int) *GameInjector {
	return &GameInjector{seed: seed, maxGames: maxGames, gameStore: fsm.NewGameStore()}
}

func marker() *platform.Event {
	return &platform.Event{Payload: platform.ReplayComplete{}}
}

func sequenced(seq int64, payload any) *platform.Event {
	return &platform.Event{Header: platform.Header{Seq: seq}, Payload: payload}
}

func TestInjectorBootstrapsOnlyAnEmptyStream(t *testing.T) {
	cold := newInjector(42, 0)
	out := cold.HandleEvent(marker())
	genesis, ok := out.(fsm.NewGame)
	if !ok {
		t.Fatalf("cold start emitted %T, want fsm.NewGame", out)
	}
	if genesis.Id != 0 {
		t.Errorf("genesis game id = %d, want 0", genesis.Id)
	}

	// A replica joining a tournament already in progress must not inject a game.
	joining := newInjector(42, 0)
	joining.HandleEvent(sequenced(0, genesis))
	if out := joining.HandleEvent(marker()); out != nil {
		t.Errorf("replica joining mid-tournament emitted %#v, want nothing", out)
	}
}

func TestInjectorInjectsTheNextGameOnlyOnCompletion(t *testing.T) {
	injector := newInjector(42, 0)
	genesis := injector.HandleEvent(marker()).(fsm.NewGame)
	if out := injector.HandleEvent(sequenced(0, genesis)); out != nil {
		t.Fatalf("emitted %#v on admitting a game, want nothing", out)
	}

	first := injector.HandleEvent(sequenced(1, fsm.GameDecision{
		Strategy: genesis.StrategyA, Decision: fsm.Cooperate,
	}))
	if first != nil {
		t.Fatalf("emitted %#v on the first decision, want nothing", first)
	}

	out := injector.HandleEvent(sequenced(2, fsm.GameDecision{
		Strategy: genesis.StrategyB, Decision: fsm.Defect,
	}))
	next, ok := out.(fsm.NewGame)
	if !ok {
		t.Fatalf("emitted %T on completion, want fsm.NewGame", out)
	}
	if next.Id != 1 {
		t.Errorf("next game id = %d, want 1", next.Id)
	}
}

func TestInjectorStopsInjectingAtMaxGames(t *testing.T) {
	injector := newInjector(42, 2)
	seq := int64(0)
	next := injector.HandleEvent(marker())

	for played := 0; played < 2; played++ {
		game := next.(fsm.NewGame)
		injector.HandleEvent(sequenced(seq, game))
		seq++
		injector.HandleEvent(sequenced(seq, fsm.GameDecision{Strategy: game.StrategyA, Decision: fsm.Cooperate}))
		seq++
		next = injector.HandleEvent(sequenced(seq, fsm.GameDecision{Strategy: game.StrategyB, Decision: fsm.Cooperate}))
		seq++
	}

	if next != nil {
		t.Fatalf("emitted %#v after reaching maxGames, want nothing", next)
	}
	if got := len(injector.gameStore.CompletedGames()); got != 2 {
		t.Errorf("completed %d games, want 2", got)
	}
}
