package tracker

import (
	"context"
	"sync/atomic"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
)

// Component is the sequencer-level component name for the tracker.
const Component = "game-tracker"

// Feed event kinds.
const (
	KindNewGame       = "new-game"
	KindDecision      = "decision"
	KindGameCompleted = "game-completed"
)

// FeedEvent is one thing that happened, in the order it happened.
//
// The UI is driven entirely by this feed and never by polled aggregate state: it
// folds these in order, the same way every component folds the sequenced stream,
// which makes the browser one more replica rather than a dashboard.
//
// Two of the three kinds are sequenced log events. "game-completed" is derived,
// emitted when the second decision of a game lands. Deriving it here rather than
// in the browser is deliberate: the tracker already is a replica of the state
// machine, so letting it say what a game came to avoids a second, drifting copy
// of the payoff rules in JavaScript.
type FeedEvent struct {
	// Index is the feed's own position, which is not the sequence number: one
	// admitted event can produce two feed events. Clients resume from it.
	Index int    `json:"index"`
	Seq   int64  `json:"seq"`
	Kind  string `json:"kind"`

	Component string `json:"component,omitempty"`
	// Replica is the half of the pair that won the race to admit this event, and
	// is the only field here that is not a logical fact. The same tournament runs
	// identically whichever replica wins, so watching the winner change is
	// watching redundancy work.
	Replica string `json:"replica,omitempty"`

	GameId    int64        `json:"gameId"`
	StrategyA fsm.Strategy `json:"strategyA,omitempty"`
	StrategyB fsm.Strategy `json:"strategyB,omitempty"`

	Strategy fsm.Strategy `json:"strategy,omitempty"`
	Decision string       `json:"decision,omitempty"`

	// Not omitempty: when both players defect the payoffs are legitimately zero,
	// and omitting them leaves the page with nothing to render.
	PayoffA     int                    `json:"payoffA"`
	PayoffB     int                    `json:"payoffB"`
	Leaderboard []fsm.LeaderboardEntry `json:"leaderboard,omitempty"`
}

// Snapshot is an immutable view of the tournament, published for readers outside
// the event loop.
type Snapshot struct {
	// Version increases on every published snapshot, so a polling client can skip
	// frames it has already drawn.
	Version     uint64
	Seq         int64
	Completed   int
	Leaderboard []fsm.LeaderboardEntry
	// Recent holds completed games, which are never mutated again and so can be
	// shared. CurrentGame is a copy, because the game in flight still is.
	Recent      []*fsm.Game
	CurrentGame *fsm.Game
	// Feed is every event of the session so far. A session is a fixed, small
	// number of games, so this is bounded by construction at a few hundred
	// entries, and keeping all of it means a client that reconnects catches up
	// from where it left off rather than losing the beginning.
	Feed []FeedEvent
	// Wins counts admitted events per "component/replica".
	Wins      map[string]int
	StateHash uint64
}

// Tracker is the scores read model. It subscribes to the stream, folds it into a
// GameStore like every other component, and emits nothing -- it never plays, so
// it never needs to be arbitrated.
//
// It exists to give the world outside the event loop something to read. Rather
// than exposing its store, which is owned by its own goroutine, it publishes an
// immutable Snapshot after every event. That is a different thing from sharing
// mutable state: readers get a value that will never change under them, and the
// store itself is still touched by exactly one goroutine.
type Tracker struct {
	gameStore *fsm.GameStore
	eventloop *platform.Eventloop
	snapshot  atomic.Pointer[Snapshot]

	version uint64
	feed    []FeedEvent
	wins    map[string]int

	target int
	done   chan struct{}
	closed bool
}

// New builds a tracker. target is the number of completed games after which Done
// fires; 0 means never.
func New(replicaId string, target int, sequencer *platform.Sequencer) *Tracker {
	t := newTracker(target)
	t.eventloop = platform.NewEventloop(Component, replicaId, sequencer, t.HandleEvent)
	return t
}

func newTracker(target int) *Tracker {
	t := &Tracker{
		gameStore: fsm.NewGameStore(),
		feed:      make([]FeedEvent, 0, 64),
		wins:      make(map[string]int),
		target:    target,
		done:      make(chan struct{}),
	}
	t.snapshot.Store(&Snapshot{
		Seq:         -1,
		Leaderboard: []fsm.LeaderboardEntry{},
		Feed:        []FeedEvent{},
		Wins:        map[string]int{},
	})
	return t
}

func (t *Tracker) Run(ctx context.Context) error { return t.eventloop.Run(ctx) }

// Snapshot returns the most recently published view. Safe from any goroutine.
func (t *Tracker) Snapshot() Snapshot { return *t.snapshot.Load() }

// Done is closed once the tournament reaches its target number of games.
func (t *Tracker) Done() <-chan struct{} { return t.done }

// GameStore exposes the underlying store. Only safe once Run has returned.
func (t *Tracker) GameStore() *fsm.GameStore { return t.gameStore }

// StateHash is the chained state checksum as of the last event applied. Prefer
// Snapshot for live reads; this is only safe once Run has returned.
func (t *Tracker) StateHash() uint64 { return t.gameStore.StateHash() }

const recentGames = 12

func (t *Tracker) HandleEvent(e *platform.Event) any {
	if e.IsReplayComplete() {
		return nil
	}
	completed := t.gameStore.ApplyEvent(e.Header.Seq, e.Payload)
	t.record(e, completed)

	all := t.gameStore.CompletedGames()
	recent := all
	if len(recent) > recentGames {
		recent = recent[len(recent)-recentGames:]
	}

	t.version++
	t.snapshot.Store(&Snapshot{
		Version:     t.version,
		Seq:         t.gameStore.AppliedSeq(),
		Completed:   len(all),
		Leaderboard: t.gameStore.Leaderboard(),
		Recent:      recent,
		CurrentGame: t.gameStore.CurrentGame().Clone(),
		Feed:        t.feedSnapshot(),
		Wins:        t.winsSnapshot(),
		StateHash:   t.gameStore.StateHash(),
	})

	if t.target > 0 && len(all) >= t.target && !t.closed {
		t.closed = true
		close(t.done)
	}
	return nil
}

// record turns one admitted event into the one or two feed events the UI folds.
func (t *Tracker) record(e *platform.Event, completed *fsm.Game) {
	base := FeedEvent{
		Index:     len(t.feed),
		Seq:       e.Header.Seq,
		Component: e.Header.SenderComponent,
		Replica:   e.Header.SenderId,
	}

	switch payload := e.Payload.(type) {
	case fsm.NewGame:
		event := base
		event.Kind, event.GameId = KindNewGame, payload.Id
		event.StrategyA, event.StrategyB = payload.StrategyA, payload.StrategyB
		t.feed = append(t.feed, event)

	case fsm.GameDecision:
		event := base
		event.Kind = KindDecision
		event.Strategy, event.Decision = payload.Strategy, payload.Decision.String()
		if game := t.gameStore.CurrentGame(); game != nil {
			event.GameId = game.Id
		} else if completed != nil {
			event.GameId = completed.Id
		}
		t.feed = append(t.feed, event)
	}

	if completed != nil {
		t.feed = append(t.feed, FeedEvent{
			Index:       len(t.feed),
			Seq:         e.Header.Seq,
			Kind:        KindGameCompleted,
			GameId:      completed.Id,
			StrategyA:   completed.StrategyA,
			StrategyB:   completed.StrategyB,
			PayoffA:     completed.PayoffA,
			PayoffB:     completed.PayoffB,
			Leaderboard: t.gameStore.Leaderboard(),
		})
	}

	t.wins[e.Header.SenderComponent+"/"+e.Header.SenderId]++
}

// Snapshots are published values, so the slices and maps in them must be copies:
// a reader holding an old snapshot must never see it change underneath.
func (t *Tracker) feedSnapshot() []FeedEvent {
	feed := make([]FeedEvent, len(t.feed))
	copy(feed, t.feed)
	return feed
}

func (t *Tracker) winsSnapshot() map[string]int {
	wins := make(map[string]int, len(t.wins))
	for replica, n := range t.wins {
		wins[replica] = n
	}
	return wins
}
