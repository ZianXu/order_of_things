package fsm

import "sort"

// Game is the materialized state of a single pairwise game.
type Game struct {
	Id        int64
	Seq       int64
	StrategyA Strategy
	DecisionA *Decision
	PayoffA   int
	StrategyB Strategy
	DecisionB *Decision
	PayoffB   int
}

// Decision returns the decision made by s in this game, or nil if s is not in
// this game or has not decided yet.
func (g *Game) Decision(s Strategy) *Decision {
	switch s {
	case g.StrategyA:
		return g.DecisionA
	case g.StrategyB:
		return g.DecisionB
	}
	return nil
}

// Opponent returns the other participant, or "" if s is not in this game.
func (g *Game) Opponent(s Strategy) Strategy {
	switch s {
	case g.StrategyA:
		return g.StrategyB
	case g.StrategyB:
		return g.StrategyA
	}
	return ""
}

// Involves reports whether s is a participant.
func (g *Game) Involves(s Strategy) bool {
	return g.StrategyA == s || g.StrategyB == s
}

// NextToMove returns the participant whose decision is expected next, or "" if
// the game is already resolved. Decisions are taken in order -- A, then B -- so
// there is always exactly one strategy the game is waiting on.
func (g *Game) NextToMove() Strategy {
	if g == nil {
		return ""
	}
	if g.DecisionA == nil {
		return g.StrategyA
	}
	if g.DecisionB == nil {
		return g.StrategyB
	}
	return ""
}

// Clone returns a deep copy.
//
// Completed games are never touched again, so they can be shared freely. A game
// still in flight is not: its decisions are filled in as they arrive. Anything
// published outside the owning goroutine has to be a copy, or a reader holding
// what it thinks is an immutable snapshot will watch it change.
func (g *Game) Clone() *Game {
	if g == nil {
		return nil
	}
	clone := *g
	if g.DecisionA != nil {
		decision := *g.DecisionA
		clone.DecisionA = &decision
	}
	if g.DecisionB != nil {
		decision := *g.DecisionB
		clone.DecisionB = &decision
	}
	return &clone
}

// GameStore is the replicated state machine every component runs. It is fed the
// same globally ordered event stream, so every component -- and every replica of
// every component -- holds an identical copy. All transitions here must be pure.
type GameStore struct {
	currentGame    *Game
	leaderboard    map[Strategy]int
	completedGames []*Game
	nextGameId     int64
	appliedSeq     int64
	stateHash      uint64
	bug            *Bug
}

// Bug corrupts a store's transition function on purpose, so the state-checksum chain
// has something real to catch.
//
// This is a different class of defect from a strategy that decides badly. A bad
// decision shows up as an emission that disagrees with the log; a bad transition
// leaves the replica's emissions looking perfectly reasonable while its idea of
// the world quietly rots. Only a reference computed independently of this replica
// can tell the difference.
type Bug struct {
	// CorruptPayoff makes the store award the wrong score, from the given game
	// onward. Emissions are unaffected until some later decision happens to
	// depend on the scores, which may be never.
	CorruptPayoff bool
	// FromGame delays the corruption, so a replica can replay correctly for a
	// while and then diverge at a visible point rather than at event zero.
	FromGame int64
}

func NewGameStore() *GameStore {
	return newStore(nil)
}

// NewGameStoreWithBug builds a deliberately defective store.
func NewGameStoreWithBug(bug Bug) *GameStore {
	return newStore(&bug)
}

func newStore(bug *Bug) *GameStore {
	store := newEmptyStore()
	store.bug = bug
	return store
}

func newEmptyStore() *GameStore {
	return &GameStore{
		currentGame:    nil,
		leaderboard:    make(map[Strategy]int),
		completedGames: make([]*Game, 0),
		nextGameId:     0,
		appliedSeq:     -1,
		stateHash:      offset64,
	}
}

// ApplyEvent folds one sequenced event into the store. It returns the game that
// this event completed, or nil if no game completed.
func (g *GameStore) ApplyEvent(seq int64, payload any) *Game {
	g.appliedSeq = seq
	defer g.rehash()

	switch v := payload.(type) {
	case NewGame:
		if g.currentGame != nil {
			panic("new game admitted while a game is still in flight")
		}
		g.currentGame = &Game{
			Id:        v.Id,
			Seq:       seq,
			StrategyA: v.StrategyA,
			StrategyB: v.StrategyB,
		}
		g.nextGameId = v.Id + 1

	case GameDecision:
		if g.currentGame == nil {
			panic("game decision received before game is created")
		}
		decision := v.Decision
		switch {
		case v.Strategy == g.currentGame.StrategyA && g.currentGame.DecisionA == nil:
			g.currentGame.DecisionA = &decision
		case v.Strategy == g.currentGame.StrategyB && g.currentGame.DecisionB == nil:
			g.currentGame.DecisionB = &decision
		}
		if g.currentGame.DecisionA != nil && g.currentGame.DecisionB != nil {
			completed := g.currentGame
			g.currentGame = nil
			calculatePayoff(completed)
			g.corrupt(completed)
			g.leaderboard[completed.StrategyA] += completed.PayoffA
			g.leaderboard[completed.StrategyB] += completed.PayoffB
			g.completedGames = append(g.completedGames, completed)
			return completed
		}
	}
	return nil
}

// corrupt applies the injected transition bug, if any.
func (g *GameStore) corrupt(game *Game) {
	if g.bug == nil || !g.bug.CorruptPayoff || game.Id < g.bug.FromGame {
		return
	}
	game.PayoffA++
}

func calculatePayoff(game *Game) {
	switch {
	case *game.DecisionA == Cooperate && *game.DecisionB == Cooperate:
		game.PayoffA, game.PayoffB = 2, 2
	case *game.DecisionA == Cooperate && *game.DecisionB == Defect:
		game.PayoffA, game.PayoffB = -1, 3
	case *game.DecisionA == Defect && *game.DecisionB == Cooperate:
		game.PayoffA, game.PayoffB = 3, -1
	default: // both defect: nobody scores
		game.PayoffA, game.PayoffB = 0, 0
	}
}

// CurrentGame returns the game in flight, or nil.
func (g *GameStore) CurrentGame() *Game {
	return g.currentGame
}

// NextGameId is the id the next admitted game should carry. Derived from applied
// state rather than from emission count, so a replica rebuilding from the log
// lands on the same value as the live replica.
func (g *GameStore) NextGameId() int64 {
	return g.nextGameId
}

// AppliedSeq is the sequence number of the last applied event, or -1 if none.
// This is the watermark: state read from this store is state as of AppliedSeq.
func (g *GameStore) AppliedSeq() int64 {
	return g.appliedSeq
}

// CompletedGames returns the completed games in sequence order.
func (g *GameStore) CompletedGames() []*Game {
	return g.completedGames
}

// LastCompletedGame returns the most recently completed game, or nil.
func (g *GameStore) LastCompletedGame() *Game {
	if len(g.completedGames) == 0 {
		return nil
	}
	return g.completedGames[len(g.completedGames)-1]
}

// LastCompletedGameFor returns the most recent completed game involving s, or
// nil. This is the whole state Flipper needs: its own history, nobody else's.
func (g *GameStore) LastCompletedGameFor(s Strategy) *Game {
	for i := len(g.completedGames) - 1; i >= 0; i-- {
		if g.completedGames[i].Involves(s) {
			return g.completedGames[i]
		}
	}
	return nil
}

// LastCompletedGameBetween returns the most recent completed game between s and
// opponent, or nil. This is the whole state Retaliator needs: the (s, opponent)
// pair's history, which is a natural two-party shard.
func (g *GameStore) LastCompletedGameBetween(s, opponent Strategy) *Game {
	for i := len(g.completedGames) - 1; i >= 0; i-- {
		game := g.completedGames[i]
		if game.Involves(s) && game.Involves(opponent) {
			return game
		}
	}
	return nil
}

// Score returns s's total score across all completed games.
func (g *GameStore) Score(s Strategy) int {
	return g.leaderboard[s]
}

// LeadingStrategy returns the highest-scoring strategy as of AppliedSeq, with
// ties broken by name. It returns "" before any game has completed.
//
// The tie-break is not cosmetic. Ranging over the leaderboard map to find a
// maximum would make the answer depend on Go's randomized map iteration order,
// which breaks invariant 1 the same way a time.Now call would -- and CopyLeader
// reads exactly this value, so it would break the whole tournament.
func (g *GameStore) LeadingStrategy() Strategy {
	if len(g.completedGames) == 0 {
		return ""
	}
	return leaderOf(g.leaderboard)
}

// LeaderboardEntry is one row of the ranked leaderboard.
type LeaderboardEntry struct {
	Strategy Strategy `json:"strategy"`
	Score    int      `json:"score"`
}

// Leaderboard returns every strategy that has played, ranked by score descending
// and then by name ascending. The ordering is total and deterministic.
func (g *GameStore) Leaderboard() []LeaderboardEntry {
	return rank(g.leaderboard)
}

// The three methods below are the watermark mechanism. There is no separate
// watermark component: it is a property of how the store is read.
//
// Every component folds the same ordered stream into its own copy of this state
// machine, so "the tracker" a strategy consults is simply its own store, and the
// blocking query the design doc describes becomes a read scoped to a point in the
// stream. In v1 that scoping is a no-op -- only one game is ever in flight, so
// everything below the current game has already resolved. It earns its keep in
// v2, where disjoint games run concurrently and the newest view of the scores can
// legitimately be missing the outcome of a game admitted earlier than this one.

// ResolvedBefore reports whether every game admitted before seq has completed.
//
// This is the watermark check. When it is false, a strategy whose decision
// depends on the scores must wait rather than decide: the past it is reading is
// incomplete, and what it is missing depends on which games happened to finish
// first in real time.
func (g *GameStore) ResolvedBefore(seq int64) bool {
	return g.currentGame == nil || g.currentGame.Seq >= seq
}

// LeaderBefore returns the highest-scoring strategy counting only games admitted
// before seq, with the same deterministic tie-break as LeadingStrategy. It
// returns "" if no game had resolved by that point.
//
// Callers should check ResolvedBefore first; this method answers from what it
// has, which is exactly the unsafe read when used on its own.
func (g *GameStore) LeaderBefore(seq int64) Strategy {
	scores := make(map[Strategy]int)
	for _, game := range g.completedGames {
		// Filter rather than stop early: completedGames is in completion order,
		// which equals admission order in v1 but will not in v2.
		if game.Seq >= seq {
			continue
		}
		scores[game.StrategyA] += game.PayoffA
		scores[game.StrategyB] += game.PayoffB
	}
	return leaderOf(scores)
}

// LastDecisionBefore returns s's most recent decision among games admitted before
// seq, and false if it had not played by that point.
func (g *GameStore) LastDecisionBefore(s Strategy, seq int64) (Decision, bool) {
	// "Most recent" means latest by admission sequence, not last to finish.
	var latest *Game
	for _, game := range g.completedGames {
		if game.Seq >= seq || game.Decision(s) == nil {
			continue
		}
		if latest == nil || game.Seq > latest.Seq {
			latest = game
		}
	}
	if latest == nil {
		return Unknown, false
	}
	return *latest.Decision(s), true
}

// leaderOf ranks a score map deterministically. Ranging a map to find a maximum
// would make the answer depend on Go's randomized iteration order.
func leaderOf(scores map[Strategy]int) Strategy {
	best := Strategy("")
	bestScore := 0
	for _, entry := range rank(scores) {
		if best == "" || entry.Score > bestScore {
			best, bestScore = entry.Strategy, entry.Score
		}
	}
	return best
}

func rank(scores map[Strategy]int) []LeaderboardEntry {
	entries := make([]LeaderboardEntry, 0, len(scores))
	for strategy, score := range scores {
		entries = append(entries, LeaderboardEntry{Strategy: strategy, Score: score})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		return entries[i].Strategy < entries[j].Strategy
	})
	return entries
}

// State hashing. A replica that restarts replays the log and chains a hash of its
// materialized state at every step; comparing that chain against a replica that
// stayed up -- or against a stored golden outcome -- decides whether it is safe
// to let it rejoin. This mirrors state-checksum verification in replicated ledgers,
// where a node proves it computed the same state before it is trusted.
//
// It is a chain rather than a hash of the final state on purpose. Two replicas
// that end up in the same place having disagreed along the way are still a bug,
// and a chain catches that; a snapshot hash would not.
//
// It is a checksum, not a Merkle root, and the distinction is the threat model.
// FNV-1a is not a cryptographic hash: it is fast, it has good avalanche, and it
// is trivial to find a collision for on purpose. That is fine here, because the
// thing being defended against is a replica that computes the wrong answer by
// accident -- a bad deploy, a stray time.Now, environment drift. It would be
// worth nothing against a replica trying to *pass*: forging a matching checksum
// for a chosen wrong state is easy. A Byzantine threat model would want a
// cryptographic hash and signatures over the chain, which is a different project.

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// StateHash is the chained state checksum as of the last applied event.
func (g *GameStore) StateHash() uint64 { return g.stateHash }

// rehash folds a hash of the current materialized state into the chain. Only
// derived state is included, and the leaderboard is walked in ranked order --
// hashing a Go map by iteration would make the digest itself nondeterministic,
// which would be a spectacular way to fail the check it exists to perform.
func (g *GameStore) rehash() {
	h := g.stateHash
	h = writeUint(h, uint64(g.appliedSeq))
	h = writeUint(h, uint64(g.nextGameId))
	h = writeUint(h, uint64(len(g.completedGames)))

	if game := g.currentGame; game != nil {
		h = writeUint(h, uint64(game.Id))
		h = writeUint(h, uint64(game.Seq))
		h = writeString(h, string(game.StrategyA))
		h = writeString(h, string(game.StrategyB))
		h = writeDecision(h, game.DecisionA)
		h = writeDecision(h, game.DecisionB)
	} else {
		h = writeUint(h, 0)
	}

	for _, entry := range g.Leaderboard() {
		h = writeString(h, string(entry.Strategy))
		h = writeUint(h, uint64(entry.Score))
	}
	g.stateHash = h
}

func writeUint(h, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h = (h ^ (v >> (i * 8) & 0xff)) * prime64
	}
	return h
}

func writeString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * prime64
	}
	return (h ^ 0xff) * prime64
}

func writeDecision(h uint64, d *Decision) uint64 {
	if d == nil {
		return writeUint(h, 0)
	}
	return writeUint(h, uint64(*d)+1)
}
