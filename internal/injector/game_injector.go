package injector

import (
	"context"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
)

// Component is the sequencer-level component name for the injector. Both
// replicas share it; that is the key the sequencer deduplicates on.
const Component = "game-injector"

// GameInjector owns the admission policy: when it is safe to put the next game
// onto the stream. It is the only component that differs between v1 and v2.
//
// The v1 policy is that a game may be admitted once the previous one has fully
// resolved, so there is never more than one game in flight and global total
// ordering holds by construction. The v2 policy will instead track each strategy
// as busy or free and admit any game whose participants are both free, which
// lets disjoint pairs run concurrently. Neither the sequencer nor any strategy
// FSM has to change for that.
//
// It keeps a GameStore purely to know when a game has resolved and what the next
// game id is. Scores are the tracker's business, not the injector's.
type GameInjector struct {
	seed      int64
	maxGames  int
	eventloop *platform.Eventloop
	gameStore *fsm.GameStore
}

// NewGameInjector builds an injector replica. seed makes the pairing schedule
// reproducible: it is the single injectable source of entropy for the whole
// system. maxGames bounds the tournament; 0 runs forever.
func NewGameInjector(
	replicaId string,
	seed int64,
	maxGames int,
	sequencer *platform.Sequencer,
	validator platform.StateValidator,
) *GameInjector {
	t := &GameInjector{
		seed:      seed,
		maxGames:  maxGames,
		gameStore: fsm.NewGameStore(),
	}
	opts := make([]platform.Option, 0, 1)
	if validator != nil {
		opts = append(opts, platform.WithStateValidation(t.StateHash, validator))
	}
	t.eventloop = platform.NewEventloop(Component, replicaId, sequencer, t.HandleEvent, opts...)
	return t
}

func (t *GameInjector) Run(ctx context.Context) error {
	return t.eventloop.Run(ctx)
}

// GameStore exposes the injector's materialized state. Only safe to read once
// its event loop has stopped.
func (t *GameInjector) GameStore() *fsm.GameStore {
	return t.gameStore
}

// StateHash is the chained state checksum as of the last event this replica applied.
// Only safe once its event loop has stopped.
func (t *GameInjector) StateHash() uint64 {
	return t.gameStore.StateHash()
}

func (t *GameInjector) HandleEvent(e *platform.Event) any {
	if e.IsReplayComplete() {
		// Bootstrap the stream, but only if it is genuinely empty. A replica that
		// joined a tournament already in progress has nothing to bootstrap.
		if t.gameStore.AppliedSeq() < 0 {
			return t.newGame()
		}
		return nil
	}

	if completed := t.gameStore.ApplyEvent(e.Header.Seq, e.Payload); completed != nil {
		if t.maxGames > 0 && len(t.gameStore.CompletedGames()) >= t.maxGames {
			return nil
		}
		return t.newGame()
	}
	// A game has two decisions and then a distinct resolution beat. The injector
	// is the only component that can derive both transitions, so it emits the
	// resolution event once the second decision has landed.
	if _, ok := e.Payload.(fsm.GameDecision); ok {
		if game := t.gameStore.CurrentGame(); game != nil && game.NextToMove() == "" {
			return fsm.GameResolved{Id: game.Id}
		}
	}
	return nil
}

// newGame draws the next pairing.
func (t *GameInjector) newGame() fsm.NewGame {
	id := t.gameStore.NextGameId()
	a, b := t.pairing(id)
	return fsm.NewGame{Id: id, StrategyA: a, StrategyB: b}
}

// pairing is a pure function of the session seed and the game's position in the
// stream. There is deliberately no PRNG object anywhere in the injector.
//
// A stateful generator would be a second, hidden source of entropy that the seed
// does not actually govern: it is advanced by *emissions*, and replicas do not
// emit uniformly. A replica that joins after the first game is admitted never
// draws for it, so from that point on its generator is one step behind its
// sibling's and the two compute different pairings from the same event history --
// which is invariant 1 breaking, even though every individual draw was "seeded".
// Deriving the pairing from (seed, gameId) instead means any replica can compute
// any game's pairing at any time, having observed nothing but the log.
func (t *GameInjector) pairing(gameId int64) (fsm.Strategy, fsm.Strategy) {
	n := uint64(len(fsm.AllStrategies))
	h := mix(uint64(t.seed) ^ mix(uint64(gameId)))

	a := h % n
	// Draw the opponent from the n-1 strategies that are not A, so a strategy can
	// never be paired against itself. Doing this by construction rather than by
	// rejection sampling keeps the draw a fixed-cost pure function.
	b := (h / n) % (n - 1)
	if b >= a {
		b++
	}
	return fsm.AllStrategies[a], fsm.AllStrategies[b]
}

// mix is splitmix64: a fast integer mixer with good avalanche, used here to turn
// (seed, position) into a well-distributed draw without carrying any state.
func mix(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}
