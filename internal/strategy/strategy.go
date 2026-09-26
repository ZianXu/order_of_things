package strategy

import (
	"context"
	"time"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
)

// Decide is a strategy's decision function. It is the whole of what makes one
// strategy different from another: the event loop, the replicated store, the
// turn-taking gate and the replica pairing are identical across all four.
//
// This signature is the correspondence table made executable. What a decision
// function reads out of store is exactly the coordination that strategy requires:
//
//	Cooperate   reads nothing                     -> none; fully parallel
//	Flip        reads its own last game           -> private per-strategy state
//	Retaliate   reads its last game vs this rival -> the (A,B) pair only
//	CopyLeader  reads the global leaderboard      -> genuine global coordination
//
// Returning fsm.Unknown means "not yet": the state this decision depends on is
// not complete, so the strategy declines to answer and will be asked again when
// the next event lands. Only CopyLeader can do this, and only in v2 -- which is
// precisely the cost of depending on global state.
//
// Decision functions must be pure. They may read store and game; they may not
// consult the clock, a generator, or map iteration order.
type Decide func(store *fsm.GameStore, self fsm.Strategy, game *fsm.Game) fsm.Decision

// Player runs one strategy as a component on the sequenced stream.
//
// All four strategies share this one implementation because in v1 they genuinely
// are the same component: each folds the whole global FSM, each is gated by the
// same turn-taking rule, and the only thing that differs is the decision
// function. Collapsing them keeps that visible -- the correspondence table is
// about what each one *reads*, and four near-identical copies of the event loop
// would bury it.
//
// This is expected to stop being true in v2, and this type is the seam to split
// on when it does. Two reasons it will come apart:
//
//   - Watermark tracking stops being uniform. Only CopyLeader needs to know
//     whether earlier games have resolved; making the others carry that machinery
//     costs nothing today because it is a single read, but it will not stay a
//     single read once games run concurrently.
//   - Not every strategy needs the whole FSM. Cooperate reads nothing at all, so
//     once the injector stops serializing everything it needs no store beyond
//     "am I in this game and is it my turn" -- and Flip needs only its own
//     history, not the leaderboard or other pairs' games. Feeding all of them the
//     full global state is the v1 shortcut, not the design.
//
// When either of those bites, give each strategy its own component with the store
// it actually depends on, and keep Decide as the shared shape. The point of the
// project is that the amount of state a component holds should match the amount
// its decision depends on, so a Cooperator carrying the global leaderboard is
// exactly the thing the demo argues against.
type Player struct {
	self      fsm.Strategy
	decide    Decide
	gameStore *fsm.GameStore
	eventloop *platform.Eventloop
}

// Config selects behaviour that differs per replica rather than per strategy.
type Config struct {
	// SkipWatermark makes CopyLeader read the newest leaderboard it holds instead
	// of the one scoped to its own game's admission point. This is the
	// determinism-bug injection hook: a one-line difference that is harmless in
	// v1, where only one game is ever in flight, and breaks invariant 1 in v2,
	// where the newest view can be missing the outcome of an earlier game.
	SkipWatermark bool

	// ImpureClock makes this replica's decision depend on the wall clock, the
	// canonical violation of invariant 3.
	//
	// SkipWatermark is the interesting bug but it is dormant in v1, where only
	// one game is ever in flight and the two reads agree on every decision. This
	// one diverges immediately, which is what makes the quarantine path
	// demonstrable before v2 exists.
	//
	// It corrupts what the replica *decides*, not what it computes, so it is
	// caught by comparing emissions against the log -- and deliberately not by
	// the state-checksum chain, which this replica satisfies perfectly.
	//
	// Being nondeterministic, it is also not reliably caught by any single check:
	// see WrongDecision.
	ImpureClock bool

	// WrongDecision inverts every decision. Deterministically wrong rather than
	// intermittently wrong, which is the difference that matters for a check that
	// runs once.
	//
	// A nondeterministic defect can pass a finite examination by luck, and
	// ImpureClock does exactly that: rehearsal asks all of a player's decisions
	// inside a loop lasting microseconds, so on a machine whose clock granularity
	// is coarser than that loop every call reads the same instant, the whole
	// rehearsal comes out honest, and it is let through -- only to diverge later,
	// live, where decisions are a second apart. That is a true property of
	// intermittent faults and not a bug in the check, but it makes ImpureClock a
	// poor thing to hang a demonstration on. This one is caught every time, on
	// every machine, and is what the UI injects.
	WrongDecision bool

	// CorruptPayoff corrupts what the replica *computes*: it mis-applies scores
	// while its emissions stay plausible. Nothing in the pair can catch this,
	// because a second replica running the same defect would agree with it. Only
	// the canonical chain, established before either ran, can.
	CorruptPayoff bool
	// CorruptFromGame delays the corruption so a restarting replica replays
	// correctly for a while and then diverges at a visible point.
	CorruptFromGame int64

	// Validator checks this replica's state checksum against the canonical chain
	// after every applied event, including during replay. Nil disables the check.
	Validator platform.StateValidator
}

// New builds a replica of the given strategy.
func New(self fsm.Strategy, replicaId string, sequencer *platform.Sequencer, cfg Config) *Player {
	store := fsm.NewGameStore()
	if cfg.CorruptPayoff {
		store = fsm.NewGameStoreWithBug(fsm.Bug{
			CorruptPayoff: true,
			FromGame:      cfg.CorruptFromGame,
		})
	}
	p := &Player{
		self:      self,
		decide:    DecideFor(self, cfg),
		gameStore: store,
	}

	opts := make([]platform.Option, 0, 1)
	if cfg.Validator != nil {
		opts = append(opts, platform.WithStateValidation(p.StateHash, cfg.Validator))
	}
	p.eventloop = platform.NewEventloop(string(self), replicaId, sequencer, p.HandleEvent, opts...)
	return p
}

// DecideFor returns the decision function for a strategy, with any injected bug
// wrapped around it.
func DecideFor(self fsm.Strategy, cfg Config) Decide {
	decide := decideFor(self, cfg)
	if cfg.WrongDecision {
		decide = inverted(decide)
	}
	if cfg.ImpureClock {
		decide = impure(decide)
	}
	return decide
}

// inverted answers the opposite of the honest answer, every time. Both replicas
// still run the same code; only one of them runs this wrapper, so they compute
// different answers from the same history and the pair diverges immediately and
// always.
func inverted(decide Decide) Decide {
	return func(store *fsm.GameStore, self fsm.Strategy, game *fsm.Game) fsm.Decision {
		if decide(store, self, game) == fsm.Cooperate {
			return fsm.Defect
		}
		return fsm.Cooperate
	}
}

// impure corrupts a decision function with a dependency on real time. Both
// replicas still run the same code; only one of them runs this wrapper, so they
// compute different answers from the same history and the pair diverges.
//
// Note what this does not establish: the replica that wins the race to the
// sequencer may well be the buggy one, in which case it is the healthy replica
// that gets quarantined. Divergence detection proves the pair disagreed, never
// which half was right.
func impure(decide Decide) Decide {
	return func(store *fsm.GameStore, self fsm.Strategy, game *fsm.Game) fsm.Decision {
		honest := decide(store, self, game)
		if time.Now().UnixNano()%2 != 0 {
			return honest
		}
		// Invert rather than substituting a fixed answer. Returning a constant
		// only diverges when the honest answer happened to be the other one,
		// which halves the detection rate again and makes the defect look
		// intermittent when it is really the detector that is being starved.
		if honest == fsm.Cooperate {
			return fsm.Defect
		}
		return fsm.Cooperate
	}
}

func decideFor(self fsm.Strategy, cfg Config) Decide {
	switch self {
	case fsm.Cooperator:
		return Cooperate
	case fsm.Flipper:
		return Flip
	case fsm.Retaliator:
		return Retaliate
	case fsm.CopyLeader:
		return CopyLeader(!cfg.SkipWatermark)
	default:
		panic("no decision function for strategy " + string(self))
	}
}

func (p *Player) Run(ctx context.Context) error { return p.eventloop.Run(ctx) }

// GameStore exposes the player's materialized state. Only safe to read from the
// player's own goroutine, or once its event loop has stopped.
func (p *Player) GameStore() *fsm.GameStore { return p.gameStore }

// StateHash is the chained state checksum as of the last event this replica applied.
// Only safe once its event loop has stopped.
func (p *Player) StateHash() uint64 { return p.gameStore.StateHash() }

func (p *Player) HandleEvent(e *platform.Event) any {
	if e.IsReplayComplete() {
		return nil
	}
	p.gameStore.ApplyEvent(e.Header.Seq, e.Payload)

	game := p.gameStore.CurrentGame()
	if !ShouldRespond(game, p.self) {
		return nil
	}
	decision := p.decide(p.gameStore, p.self, game)
	if decision == fsm.Unknown {
		// Waiting on state that has not resolved. The gate stays open, so the next
		// event re-asks. No deadlock is possible: a game can only ever wait on
		// strictly lower sequence numbers, never on itself.
		return nil
	}
	return fsm.GameDecision{Strategy: p.self, Decision: decision}
}

// Cooperate always cooperates. It reads nothing at all -- not the opponent, not
// its own history, not the scores -- so two Cooperate replicas need no shared
// state whatsoever to agree.
func Cooperate(*fsm.GameStore, fsm.Strategy, *fsm.Game) fsm.Decision {
	return fsm.Cooperate
}

// Flip alternates its own previous decision, opening with cooperation.
//
// It reads its own last game and nothing else, so it needs its own history
// serialized but does not care what any other strategy did. The v2 injector gives
// it that for free by never letting a strategy be in two games at once.
func Flip(store *fsm.GameStore, self fsm.Strategy, _ *fsm.Game) fsm.Decision {
	previous := store.LastCompletedGameFor(self)
	if previous == nil {
		return fsm.Cooperate
	}
	if *previous.Decision(self) == fsm.Cooperate {
		return fsm.Defect
	}
	return fsm.Cooperate
}

// Retaliate mirrors what this specific opponent did the last time these two
// played, opening with cooperation. Tit-for-tat.
//
// Its view is the (self, opponent) pair's history: narrower than global, wider
// than private. Games between other strategies cannot affect it, so it needs
// ordering only within the pair -- a natural two-party shard.
func Retaliate(store *fsm.GameStore, self fsm.Strategy, game *fsm.Game) fsm.Decision {
	previous := store.LastCompletedGameBetween(self, game.Opponent(self))
	if previous == nil {
		return fsm.Cooperate
	}
	if *previous.Decision(game.Opponent(self)) == fsm.Defect {
		return fsm.Defect
	}
	return fsm.Cooperate
}

// CopyLeader copies the most recent decision of whoever is currently winning.
//
// This is the one strategy whose decision depends on global shared state, and so
// the only one that can be forced to wait. watermarked selects between the two
// reads the design doc contrasts:
//
//   - watermarked: the scores as of this game's own admission point, which
//     requires every earlier game to have resolved first. Deterministic, because
//     an earlier game's outcome does not depend on when it finished.
//   - unwatermarked: whatever scores this replica happens to hold right now. In
//     v1 that is the same answer. In v2 it is whichever concurrent games won the
//     race to resolve, which is a real-time property, not a logical one.
func CopyLeader(watermarked bool) Decide {
	return func(store *fsm.GameStore, _ fsm.Strategy, game *fsm.Game) fsm.Decision {
		var (
			decision fsm.Decision
			known    bool
		)

		if watermarked {
			if !store.ResolvedBefore(game.Seq) {
				return fsm.Unknown // wait: some earlier game has not resolved
			}
			if leader := store.LeaderBefore(game.Seq); leader != "" {
				decision, known = store.LastDecisionBefore(leader, game.Seq)
			}
		} else if leader := store.LeadingStrategy(); leader != "" {
			if previous := store.LastCompletedGameFor(leader); previous != nil {
				decision, known = *previous.Decision(leader), true
			}
		}

		if !known {
			// Nobody is leading yet, so there is nothing to copy.
			return fsm.Cooperate
		}
		return decision
	}
}
