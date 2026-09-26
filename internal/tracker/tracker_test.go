package tracker

import (
	"encoding/json"
	"testing"

	"order_of_things/internal/fsm"
	"order_of_things/internal/platform"
)

func sequenced(seq int64, payload any) *platform.Event {
	return &platform.Event{
		Header:  platform.Header{Seq: seq, SenderComponent: "test", SenderId: "r0"},
		Payload: payload,
	}
}

func playGame(tr *Tracker, seq int64, a, b fsm.Strategy, da, db fsm.Decision) int64 {
	tr.HandleEvent(sequenced(seq, fsm.NewGame{Id: seq, StrategyA: a, StrategyB: b}))
	tr.HandleEvent(sequenced(seq+1, fsm.GameDecision{Strategy: a, Decision: da}))
	tr.HandleEvent(sequenced(seq+2, fsm.GameDecision{Strategy: b, Decision: db}))
	tr.HandleEvent(sequenced(seq+3, fsm.GameResolved{Id: seq}))
	return seq + 4
}

// The tracker is a read model: it folds the stream like everyone else but must
// never emit, so it is never arbitrated and never needs a replica pair.
func TestTrackerNeverEmits(t *testing.T) {
	tr := newTracker(0)
	events := []*platform.Event{
		{Payload: platform.ReplayComplete{}},
		sequenced(0, fsm.NewGame{Id: 0, StrategyA: fsm.Flipper, StrategyB: fsm.Cooperator}),
		sequenced(1, fsm.GameDecision{Strategy: fsm.Flipper, Decision: fsm.Defect}),
		sequenced(2, fsm.GameDecision{Strategy: fsm.Cooperator, Decision: fsm.Cooperate}),
		sequenced(3, fsm.GameResolved{Id: 0}),
	}
	for i, e := range events {
		if out := tr.HandleEvent(e); out != nil {
			t.Errorf("event %d: tracker emitted %#v", i, out)
		}
	}
}

func TestSnapshotTracksTheStream(t *testing.T) {
	tr := newTracker(0)
	if got := tr.Snapshot(); got.Seq != -1 || got.Completed != 0 {
		t.Errorf("initial snapshot = %+v, want seq -1 and no games", got)
	}

	seq := playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	snapshot := tr.Snapshot()
	if snapshot.Completed != 1 {
		t.Errorf("Completed = %d, want 1", snapshot.Completed)
	}
	if snapshot.Seq != seq-1 {
		t.Errorf("Seq = %d, want %d", snapshot.Seq, seq-1)
	}
	if snapshot.StateHash != tr.gameStore.StateHash() {
		t.Error("snapshot hash does not match the store")
	}
	if len(snapshot.Leaderboard) != 2 || snapshot.Leaderboard[0].Strategy != fsm.Flipper {
		t.Errorf("leaderboard = %v, want flipper leading", snapshot.Leaderboard)
	}
}

func TestTrackerNotifiesWhenItPublishes(t *testing.T) {
	tr := newTracker(0)
	updates := tr.Updates()
	tr.HandleEvent(sequenced(0, fsm.NewGame{Id: 0, StrategyA: fsm.Flipper, StrategyB: fsm.Cooperator}))

	select {
	case <-updates:
	default:
		t.Fatal("publishing a snapshot did not notify subscribers")
	}
	select {
	case <-tr.Updates():
		t.Fatal("a fresh update channel was already closed")
	default:
	}
}

// Snapshots are published values, not views onto live state: one taken earlier
// must not change when the store moves on.
func TestSnapshotsAreImmutable(t *testing.T) {
	tr := newTracker(0)
	seq := playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	early := tr.Snapshot()

	for i := 0; i < 5; i++ {
		seq = playGame(tr, seq, fsm.Retaliator, fsm.CopyLeader, fsm.Cooperate, fsm.Cooperate)
	}

	if early.Completed != 1 {
		t.Errorf("an earlier snapshot now reports %d games, want 1", early.Completed)
	}
	if tr.Snapshot().Completed != 6 {
		t.Errorf("current snapshot = %d games, want 6", tr.Snapshot().Completed)
	}
}

func TestRecentGamesAreBounded(t *testing.T) {
	tr := newTracker(0)
	seq := int64(0)
	for i := 0; i < recentGames+10; i++ {
		seq = playGame(tr, seq, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	}
	snapshot := tr.Snapshot()
	if len(snapshot.Recent) != recentGames {
		t.Errorf("Recent has %d games, want %d", len(snapshot.Recent), recentGames)
	}
	if last := snapshot.Recent[len(snapshot.Recent)-1]; last.Id != seq-4 {
		t.Errorf("last recent game id = %d, want %d", last.Id, seq-4)
	}
}

func TestDoneFiresOnceAtTheTarget(t *testing.T) {
	tr := newTracker(2)
	seq := playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	select {
	case <-tr.Done():
		t.Fatal("Done fired after 1 of 2 games")
	default:
	}

	seq = playGame(tr, seq, fsm.Retaliator, fsm.CopyLeader, fsm.Cooperate, fsm.Cooperate)
	select {
	case <-tr.Done():
	default:
		t.Fatal("Done did not fire at the target")
	}

	// Games beyond the target must not close it a second time.
	playGame(tr, seq, fsm.Flipper, fsm.Retaliator, fsm.Defect, fsm.Defect)
}

// The feed is what the UI folds. Replica is the only field in it that is not a
// logical fact -- it is which half of the pair won the race, and the only place
// active-active is visible.
func TestFeedRecordsWhoWonEachPosition(t *testing.T) {
	tr := newTracker(0)
	tr.HandleEvent(&platform.Event{
		Header:  platform.Header{Seq: 0, SenderComponent: "game-injector", SenderId: "r1"},
		Payload: fsm.NewGame{Id: 0, StrategyA: fsm.Flipper, StrategyB: fsm.Cooperator},
	})
	tr.HandleEvent(&platform.Event{
		Header:  platform.Header{Seq: 1, SenderComponent: "flipper", SenderId: "r0"},
		Payload: fsm.GameDecision{Strategy: fsm.Flipper, Decision: fsm.Defect},
	})

	feed := tr.Snapshot().Feed
	if len(feed) != 2 {
		t.Fatalf("feed has %d events, want 2", len(feed))
	}
	if feed[0].Kind != KindNewGame || feed[0].Component != "game-injector" || feed[0].Replica != "r1" {
		t.Errorf("feed[0] = %+v, want a new-game from game-injector/r1", feed[0])
	}
	if feed[0].StrategyA != fsm.Flipper || feed[0].StrategyB != fsm.Cooperator {
		t.Errorf("feed[0] participants = %s vs %s", feed[0].StrategyA, feed[0].StrategyB)
	}
	if feed[1].Kind != KindDecision || feed[1].Replica != "r0" {
		t.Errorf("feed[1] = %+v, want a decision from r0", feed[1])
	}
	if feed[1].Strategy != fsm.Flipper || feed[1].Decision != "defect" {
		t.Errorf("feed[1] decision = %s %s, want flipper defect", feed[1].Strategy, feed[1].Decision)
	}
	if feed[1].GameId != 0 {
		t.Errorf("feed[1] gameId = %d, want 0", feed[1].GameId)
	}

	wins := tr.Snapshot().Wins
	if wins["game-injector/r1"] != 1 || wins["flipper/r0"] != 1 {
		t.Errorf("wins = %v", wins)
	}
}

// A game completion is derived from its resolution event, so the browser does
// not need its own copy of the payoff rules.
func TestFeedDerivesGameCompletion(t *testing.T) {
	tr := newTracker(0)
	playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)

	feed := tr.Snapshot().Feed
	if len(feed) != 4 {
		t.Fatalf("feed has %d events, want 4 (new-game, two decisions, completion)", len(feed))
	}
	for i, want := range []string{KindNewGame, KindDecision, KindDecision, KindGameCompleted} {
		if feed[i].Kind != want {
			t.Errorf("feed[%d] kind = %q, want %q", i, feed[i].Kind, want)
		}
		if feed[i].Index != i {
			t.Errorf("feed[%d] index = %d", i, feed[i].Index)
		}
	}

	completion := feed[3]
	if completion.PayoffA != 3 || completion.PayoffB != -1 {
		t.Errorf("payoffs = (%d, %d), want (3, -1)", completion.PayoffA, completion.PayoffB)
	}
	if len(completion.Leaderboard) != 2 || completion.Leaderboard[0].Strategy != fsm.Flipper {
		t.Errorf("completion leaderboard = %v, want flipper leading", completion.Leaderboard)
	}
	if completion.Seq == feed[2].Seq {
		t.Errorf("completion seq = %d, should follow the second decision", completion.Seq)
	}
}

// A client that reconnects has to be able to catch up from where it left off, so
// the feed keeps the whole session rather than a sliding window.
func TestFeedKeepsTheWholeSession(t *testing.T) {
	tr := newTracker(0)
	seq := int64(0)
	const games = 40
	for i := 0; i < games; i++ {
		seq = playGame(tr, seq, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	}
	feed := tr.Snapshot().Feed
	if want := games * 4; len(feed) != want {
		t.Fatalf("feed has %d events after %d games, want %d", len(feed), games, want)
	}
	for i := range feed {
		if feed[i].Index != i {
			t.Fatalf("feed[%d] has index %d; indices must be contiguous for resume to work", i, feed[i].Index)
		}
	}
}

// Everything published in a snapshot must be a copy, or a reader holding what it
// believes is an immutable view watches it change.
func TestSnapshotCollectionsAreCopies(t *testing.T) {
	tr := newTracker(0)
	seq := playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Cooperate)
	early := tr.Snapshot()

	if early.CurrentGame != nil {
		t.Fatal("a completed game left something in flight")
	}
	playGame(tr, seq, fsm.Retaliator, fsm.CopyLeader, fsm.Defect, fsm.Defect)

	if len(early.Feed) != 4 {
		t.Errorf("an earlier snapshot's feed grew to %d events", len(early.Feed))
	}
	if early.Wins["test/r0"] != 4 {
		t.Errorf("an earlier snapshot's wins changed to %v", early.Wins)
	}
	if tr.Snapshot().Version <= early.Version {
		t.Error("Version did not advance")
	}
}

func TestSnapshotPublishesTheGameInFlight(t *testing.T) {
	tr := newTracker(0)
	tr.HandleEvent(sequenced(0, fsm.NewGame{Id: 0, StrategyA: fsm.Flipper, StrategyB: fsm.Cooperator}))

	published := tr.Snapshot()
	if published.CurrentGame == nil {
		t.Fatal("no game in flight published")
	}
	if got := published.CurrentGame.NextToMove(); got != fsm.Flipper {
		t.Errorf("NextToMove = %q, want flipper", got)
	}

	tr.HandleEvent(sequenced(1, fsm.GameDecision{Strategy: fsm.Flipper, Decision: fsm.Defect}))
	if published.CurrentGame.DecisionA != nil {
		t.Error("the published copy changed when the store did")
	}
	if got := tr.Snapshot().CurrentGame.NextToMove(); got != fsm.Cooperator {
		t.Errorf("NextToMove after flipper moved = %q, want cooperator", got)
	}
}

// Both players defecting scores zero for each, which is a real result and not an
// absent one. It has to survive the trip to the page.
func TestFeedCarriesZeroPayoffs(t *testing.T) {
	tr := newTracker(0)
	playGame(tr, 0, fsm.Flipper, fsm.Cooperator, fsm.Defect, fsm.Defect)

	completion := tr.Snapshot().Feed[3]
	if completion.Kind != KindGameCompleted {
		t.Fatalf("feed[3] is %q, want a completion", completion.Kind)
	}
	if completion.PayoffA != 0 || completion.PayoffB != 0 {
		t.Fatalf("payoffs = (%d, %d), want (0, 0)", completion.PayoffA, completion.PayoffB)
	}

	encoded, err := json.Marshal(completion)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	for _, field := range []string{"payoffA", "payoffB"} {
		if _, present := decoded[field]; !present {
			t.Errorf("%s was omitted from %s; the page renders it as undefined", field, encoded)
		}
	}
}
