package session

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"order_of_things/internal/fsm"
	"order_of_things/internal/golden"
	"order_of_things/internal/injector"
	"order_of_things/internal/platform"
	"order_of_things/internal/strategy"
	"order_of_things/internal/tracker"
)

// Defect is a determinism bug to inject into a single replica, so the divergence
// and quarantine paths can be exercised on demand rather than existing only as
// untested code.
//
// The two live defects are caught by different mechanisms, which is the reason
// both mechanisms exist:
//
//	ImpureClock    wrong decisions, right state -> caught by comparing this
//	               replica's emissions against what the log recorded. Proves the
//	               pair disagreed; cannot say which half was right.
//	CorruptPayoff  right decisions, wrong state -> invisible to the pair, since a
//	               sibling with the same defect would agree. Caught by the
//	               canonical state-checksum chain, which does say which is wrong,
//	               because the reference was computed before either replica ran.
type Defect struct {
	// WrongDecision makes the replica answer the opposite of the honest answer,
	// every time. Deterministically wrong, so a check that runs once catches it
	// every time on every machine. This is what the UI injects.
	WrongDecision bool
	// ImpureClock makes the replica's decision depend on the wall clock. Kept
	// because it is the canonical violation of invariant 3, but it is
	// intermittent, and an intermittent fault can pass a finite examination by
	// luck -- which is a true thing about intermittent faults and a bad thing to
	// build a demonstration on.
	ImpureClock bool
	// CorruptPayoff makes the replica mis-apply scores while still deciding
	// plausibly.
	//
	// This is only caught while the replica is replaying, because that is the
	// window the canonical chain covers -- it is admission control, not a live
	// policy (see platform.StateValidator). So injecting it into a replica with
	// no history to rebuild catches nothing, correctly: there is no past for it
	// to have got wrong yet, and its emissions stay plausible until something
	// finally reads the scores it has been quietly ruining.
	CorruptPayoff bool
	// CorruptFromGame delays CorruptPayoff, so a restarting replica replays
	// correctly for a while and then diverges at a visible point. Set it inside
	// the range the replica will actually replay.
	CorruptFromGame int64
	// SkipWatermark makes CopyLeader read the newest scores rather than the ones
	// scoped to its own game. Dormant in v1; breaks invariant 1 in v2.
	SkipWatermark bool
}

// Any reports whether the defect asks for anything at all.
func (d Defect) Any() bool {
	return d.WrongDecision || d.ImpureClock || d.CorruptPayoff || d.SkipWatermark
}

// Bug places a Defect on one specific replica at session construction. Only ever
// one: a pair where both replicas share a bug agrees with itself, which is the
// limitation of active-active the design names explicitly.
type Bug struct {
	Component string
	Replica   string
	Defect
}

// Config describes one tournament.
type Config struct {
	// Seed is the single injectable source of entropy for the whole session.
	Seed int64
	// Games is how many games to play before the tournament stops.
	Games int
	// Replicas is how many instances of every component to run. 1 is a
	// single-instance system; 2 is the active-active pair the design calls for.
	Replicas int
	// Bug optionally corrupts one replica from the start.
	Bug *Bug
	// Interval paces admission, one event per interval. Zero runs flat out,
	// which is what headless reference runs and tests want.
	Interval time.Duration
	// Reference is the canonical state-checksum chain every replica is checked
	// against as it applies events. Nil disables the check, which is what the
	// run that generates the reference has to do.
	Reference *golden.Validator
	// StartPaused holds admission from the moment the session is built, so
	// nothing happens until something asks for it. A server wants this: a viewer
	// pressing Play should see the tournament from its first event, not join one
	// already in progress because the page took a moment to connect.
	StartPaused bool
}

func (c Config) withDefaults() Config {
	if c.Replicas < 1 {
		c.Replicas = 1
	}
	if c.Games < 1 {
		c.Games = 1
	}
	return c
}

// component is what the session supervises. Every component is the same shape:
// an event loop over the sequenced stream, plus a state checksum it can be checked
// against.
type component interface {
	Run(ctx context.Context) error
	// HandleEvent applies one event outside any event loop, which is what lets a
	// candidate replica be rehearsed privately before it is wired to anything.
	HandleEvent(*platform.Event) any
	StateHash() uint64
}

// slot is one supervised replica: how to build a fresh instance of it, and the
// handle on the instance currently running.
type slot struct {
	component string
	id        string
	build     func(*platform.Sequencer, Defect) component
	// defect is what this slot was last built with, so a restart without an
	// explicit defect comes back clean rather than inheriting one.
	defect Defect
	// The tracker is a single long-lived read model rather than one half of an
	// arbitrated pair, so it is supervised but not fault-injectable.
	restartable bool

	cancel context.CancelFunc
	// done is closed once this instance has stopped AND its outcome has been
	// recorded below, so anyone who waits on it sees settled state.
	done chan struct{}
	// gen identifies the current instance, so a watcher left over from a previous
	// one cannot report the death of something that has since been replaced.
	gen     int
	running bool
	// killed and quarantined are the two ways a replica leaves the pair. Killed
	// is a simulated fault and is recoverable; quarantined means the replica
	// disagreed with its sibling and refused to keep serving.
	killed      bool
	quarantined bool
}

func (s *slot) String() string { return s.component + "/" + s.id }

// Session is one fully in-memory instance of the whole system: a sequencer, an
// injector, the four strategies and a tracker. Nothing is shared between
// sessions, so a server can run many at once and each stays reproducible from
// its own seed alone.
//
// The mutex here guards supervision bookkeeping -- which replicas are running --
// and nothing else. No state machine is behind it; those stay owned by their own
// goroutines.
type Session struct {
	cfg       Config
	sequencer *platform.Sequencer
	tracker   *tracker.Tracker

	pacer *platform.Pacer
	chain []uint64
	log   []golden.Event
	// lifecycle serializes the operator actions -- kill, restart -- which have to
	// release mu while they wait for a goroutine to stop. Without it two of them
	// could interleave in that window and leave a slot running two instances.
	lifecycle sync.Mutex

	mu            sync.Mutex
	slots         []*slot
	ctx           context.Context
	cancel        context.CancelFunc
	sequencerDone chan struct{}
	started       bool
	// finished is set once the tournament has stopped. A session that has ended
	// is not controllable: restarting a replica into a cancelled context would
	// appear to work and then do nothing, which is worse than being refused.
	finished bool

	// Wait is one-shot but callable from anywhere: the registry waits on every
	// session it owns, and whoever holds a handle will naturally wait too. Running
	// the teardown twice would drain the same channels twice and block forever, so
	// it happens once and every caller gets the same Result.
	waitOnce sync.Once
	result   Result
}

func New(cfg Config) *Session {
	cfg = cfg.withDefaults()
	sequencer := platform.NewSequencer()
	pacer := platform.NewPacer(cfg.Interval)
	if cfg.StartPaused {
		// Paused before Run is ever called, so no event can slip out first.
		pacer.Pause()
	}
	sequencer.SetPacer(pacer)

	s := &Session{
		cfg:       cfg,
		sequencer: sequencer,
		pacer:     pacer,
		tracker:   tracker.New("r0", cfg.Games, sequencer),
	}

	// The tracker never plays, so it is never arbitrated and needs no pair.
	s.slots = append(s.slots, &slot{
		component: tracker.Component,
		id:        "r0",
		build:     func(*platform.Sequencer, Defect) component { return s.tracker },
	})

	var validator platform.StateValidator
	if cfg.Reference != nil {
		validator = cfg.Reference
	}

	for r := 0; r < cfg.Replicas; r++ {
		id := fmt.Sprintf("r%d", r)
		s.add(injector.Component, id, func(seq *platform.Sequencer, _ Defect) component {
			return injector.NewGameInjector(id, cfg.Seed, cfg.Games, seq, validator)
		})
		for _, name := range fsm.AllStrategies {
			s.add(string(name), id, func(seq *platform.Sequencer, defect Defect) component {
				return strategy.New(name, id, seq, strategy.Config{
					SkipWatermark:   defect.SkipWatermark,
					WrongDecision:   defect.WrongDecision,
					ImpureClock:     defect.ImpureClock,
					CorruptPayoff:   defect.CorruptPayoff,
					CorruptFromGame: defect.CorruptFromGame,
					Validator:       validator,
				})
			})
		}
	}

	if bug := cfg.Bug; bug != nil {
		for _, sl := range s.slots {
			if sl.component == bug.Component && sl.id == bug.Replica {
				sl.defect = bug.Defect
			}
		}
	}
	return s
}

func (s *Session) add(component, id string, build func(*platform.Sequencer, Defect) component) {
	s.slots = append(s.slots, &slot{
		component: component, id: id, build: build, restartable: true,
	})
}

// Tracker is the session's read model, safe to poll while the session runs.
func (s *Session) Tracker() *tracker.Tracker { return s.tracker }

// Sequencer is the session's admission gate. Its log is only safe to read once
// the session has stopped.
func (s *Session) Sequencer() *platform.Sequencer { return s.sequencer }

// Result is what a finished session produced. It is the unit compared against a
// golden outcome, and between v1 and v2.
type Result struct {
	Seed        int64
	Games       int
	Leaderboard []fsm.LeaderboardEntry
	StateHash   uint64
	LogLength   int
	Chain       []uint64
	Log         []golden.Event
	Quarantined []string
}

// Golden converts the result into the durable record for its seed.
func (r Result) Golden() golden.Outcome {
	return golden.Outcome{
		Seed:        r.Seed,
		Games:       r.Games,
		StateHash:   r.StateHash,
		Leaderboard: r.Leaderboard,
		Chain:       r.Chain,
		Log:         r.Log,
	}
}

// Start launches the sequencer and every replica, and returns immediately.
func (s *Session) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(ctx)

	sequencerDone := make(chan struct{})
	go func() {
		defer close(sequencerDone)
		s.sequencer.Run(s.ctx)
	}()
	s.sequencerDone = sequencerDone

	for _, sl := range s.slots {
		s.startLocked(sl)
	}
}

func (s *Session) startLocked(sl *slot) {
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	instance := sl.build(s.sequencer, sl.defect)

	sl.gen++
	sl.cancel, sl.done = cancel, done
	sl.running, sl.killed, sl.quarantined = true, false, false
	gen := sl.gen

	// Watch it. A replica that quarantines itself does so on its own goroutine,
	// and with nobody waiting the supervisor would not find out until it happened
	// to join that goroutine for some unrelated reason -- reporting the replica as
	// healthy in the meantime, and then appearing to quarantine it in response to
	// whatever finally did the join.
	go func() {
		err := instance.Run(ctx)

		s.mu.Lock()
		if sl.gen == gen { // not superseded by a restart
			sl.running = false
			if err != nil {
				sl.quarantined = true
			}
		}
		s.mu.Unlock()

		// Closed last, so a waiter that sees this sees the outcome too.
		close(done)
	}()
}

// Wait blocks until the tournament reaches its target or ctx is cancelled, then
// stops everything and reports the outcome. Safe to call more than once, and from
// more than one goroutine; every caller gets the same Result.
func (s *Session) Wait() Result {
	if !s.started {
		panic("session: Wait called before Start")
	}
	s.waitOnce.Do(func() { s.result = s.shutdown() })
	return s.result
}

func (s *Session) shutdown() Result {
	select {
	case <-s.tracker.Done():
	case <-s.ctx.Done():
	}

	// Stop the components, wait for every watcher to record its outcome, then
	// stop the sequencer and read its log. Joining before reading is what makes
	// those reads safe without putting a lock on the state itself.
	s.cancel()

	// Let any operator action already in flight finish rather than racing it.
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()

	s.mu.Lock()
	waits := make([]chan struct{}, 0, len(s.slots))
	for _, sl := range s.slots {
		if sl.done != nil {
			waits = append(waits, sl.done)
		}
	}
	s.mu.Unlock()

	for _, done := range waits {
		<-done
	}

	s.mu.Lock()
	s.finished = true
	quarantined := make([]string, 0)
	for _, sl := range s.slots {
		if sl.quarantined {
			quarantined = append(quarantined, sl.String())
		}
	}
	s.mu.Unlock()
	<-s.sequencerDone

	// Rebuild the state-checksum chain from the log. Doing it here rather than
	// having the tracker accumulate it keeps the chain a property of the log,
	// which is what a replaying replica is actually checked against.
	replay := fsm.NewGameStore()
	s.chain = make([]uint64, 0, len(s.sequencer.EventLog()))
	s.log = make([]golden.Event, 0, len(s.sequencer.EventLog()))
	for _, e := range s.sequencer.EventLog() {
		replay.ApplyEvent(e.Header.Seq, e.Payload)
		s.chain = append(s.chain, replay.StateHash())
		s.log = append(s.log, golden.NewEvent(e.Header.Seq, e.Header.SenderComponent, e.Payload))
	}

	store := s.tracker.GameStore()
	return Result{
		Seed:        s.cfg.Seed,
		Games:       len(store.CompletedGames()),
		Leaderboard: store.Leaderboard(),
		StateHash:   store.StateHash(),
		LogLength:   len(s.sequencer.EventLog()),
		Chain:       s.chain,
		Log:         s.log,
		Quarantined: quarantined,
	}
}

// Run starts the session and waits for it to finish.
func (s *Session) Run(ctx context.Context) Result {
	s.Start(ctx)
	return s.Wait()
}

// Kill stops one replica's event loop, simulating a process death. There is no
// OS process to signal in a single-process design, so "killing" means the replica
// stops responding; its sibling carries the component alone.
func (s *Session) Kill(component, id string) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()

	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return errFinished
	}
	sl, err := s.findLocked(component, id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !sl.restartable {
		s.mu.Unlock()
		return fmt.Errorf("session: %s is not fault-injectable", sl)
	}
	if !sl.running {
		s.mu.Unlock()
		return fmt.Errorf("session: %s is not running", sl)
	}
	cancel, done := sl.cancel, sl.done
	s.mu.Unlock()

	// Wait outside mu: the watcher takes it on the way out, so holding it here
	// would deadlock against the goroutine being waited on.
	cancel()
	<-done

	s.mu.Lock()
	defer s.mu.Unlock()
	// A replica that had already quarantined itself is not also "killed" -- it
	// was gone before the request arrived, and saying so is more useful.
	if !sl.quarantined {
		sl.killed = true
	}
	return nil
}

// Restart brings a replica back as a fresh instance with empty state, stopping
// the one that is there if it is still up. It subscribes, replays the log from
// the sequencer, and rejoins its pair -- unless
// its replay disagrees with what the log records or with the canonical chain, in
// which case it quarantines itself before it can affect anything.
//
// A quarantined replica can be restarted. Quarantine refuses a divergent
// *instance*, not the name forever: an operator redeploying a fixed build is
// entitled to try again, and the fresh instance has to earn its place by
// replaying correctly like any other. What it may not do is talk its way back in
// without being re-checked, and it cannot -- the check is on the replay path and
// happens whether anyone asked for it or not.
func (s *Session) Restart(component, id string) error {
	return s.restart(component, id, Defect{})
}

// RestartWithBug brings a replica back defective, stopping the one that is there
// if it is still up. Both defects are caught during replay, by different
// mechanisms, and neither gets to rejoin.
func (s *Session) RestartWithBug(component, id string, defect Defect) error {
	return s.restart(component, id, defect)
}

func (s *Session) restart(component, id string, defect Defect) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()

	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return errFinished
	}
	sl, err := s.findLocked(component, id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !sl.restartable {
		s.mu.Unlock()
		return fmt.Errorf("session: %s is not fault-injectable", sl)
	}
	cancel, done, running := sl.cancel, sl.done, sl.running
	s.mu.Unlock()

	// Restarting something still up means stopping it first. A restart is a
	// redeploy of that replica, not a second instance of it, and refusing until
	// the caller has killed it themselves makes the operator do bookkeeping the
	// supervisor is better placed to do.
	if running {
		cancel()
		<-done
	}

	// Rehearse before wiring it to anything. A candidate that cannot reproduce
	// the canonical tournament is refused here, having spoken to nobody -- which
	// is the only point at which refusing it is free.
	//
	// Asked more than once, because a pure function gives the same answer every
	// time and an impure one may not. This raises the odds against an
	// intermittent defect; it does not make catching one certain, and nothing
	// could -- a fault that shows up one time in a thousand will pass any finite
	// examination nine hundred and ninety-nine times.
	if s.cfg.Reference != nil {
		for pass := 0; pass < rehearsalPasses; pass++ {
			// A private sequencer each time: the candidate is built against
			// something inert, so nothing it does while being examined can reach
			// the live stream.
			candidate := sl.build(platform.NewSequencer(), defect)
			if err := rehearse(candidate, sl.component, s.cfg.Reference); err != nil {
				s.mu.Lock()
				defer s.mu.Unlock()
				sl.defect = defect
				sl.quarantined, sl.running, sl.killed = true, false, false
				log.Printf("Rehearsal refused on pass %d: %v", pass+1, err)
				return nil
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return errFinished
	}
	sl.defect = defect
	s.startLocked(sl)
	return nil
}

// Pause holds event admission. Everything stays where it is; nothing is lost.
func (s *Session) Pause() { s.pacer.Pause() }

// Resume releases event admission.
func (s *Session) Resume() { s.pacer.Resume() }

// Step admits exactly one event and leaves the session paused.
func (s *Session) Step() { s.pacer.Step() }

// SetInterval changes the time between admissions, taking effect immediately.
func (s *Session) SetInterval(interval time.Duration) { s.pacer.SetInterval(interval) }

// Pacing reports whether events are being admitted, and how fast.
func (s *Session) Pacing() (running bool, interval time.Duration) { return s.pacer.State() }

// Seed is the session's seed, which is all that is needed to reproduce it.
func (s *Session) Seed() int64 { return s.cfg.Seed }

// Games is the number of games this session will play.
func (s *Session) Games() int { return s.cfg.Games }

// ReplicaStatus reports what the supervisor knows about one replica. The UI
// draws one character per replica, so this is the per-character state.
type ReplicaStatus struct {
	Component   string
	Replica     string
	Running     bool
	Killed      bool
	Quarantined bool
	Defect      Defect
	// Wins is how many events this replica won the race to admit. A replica that
	// never wins is a hot standby, not half of an active-active pair.
	//
	// The page no longer shows this as a number -- it read like a score and
	// invited comparison with the leaderboard, which it has nothing to do with.
	// The event log carries the same fact better: every line names the replica
	// that won that position.
	Wins int
}

// Live reports whether this replica is able to serve.
func (r ReplicaStatus) Live() bool { return r.Running && !r.Quarantined }

// Status lists every supervised replica. Safe from any goroutine.
func (s *Session) Status() []ReplicaStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	wins := s.tracker.Snapshot().Wins
	statuses := make([]ReplicaStatus, 0, len(s.slots))
	for _, sl := range s.slots {
		statuses = append(statuses, ReplicaStatus{
			Component:   sl.component,
			Replica:     sl.id,
			Running:     sl.running,
			Killed:      sl.killed,
			Quarantined: sl.quarantined,
			Defect:      sl.defect,
			Wins:        wins[sl.component+"/"+sl.id],
		})
	}
	return statuses
}

// Stalled reports whether the tournament cannot make progress because a strategy
// in the current game has no live replica, and names the strategy if so.
//
// It is derived, not timed: the supervisor knows which replicas are live and the
// tracker knows who is playing, so no clock is involved. Killing both halves of a
// pair is allowed precisely so this can happen -- it is the most direct
// demonstration of why there are two of everything, and restarting either half
// clears it.
//
// The condition is eventual, not instantaneous. It is read from the tracker's
// published snapshot, but an admitted emission can still be waiting at the
// pacer. That emission will settle the apparent wait, so it is not a stall.
func (s *Session) Stalled() (bool, string) {
	game := s.tracker.Snapshot().CurrentGame
	if game == nil {
		return false, ""
	}
	// Only the strategy actually due to move can block the game. Its opponent
	// being dead does not stall anything yet: the game still has this move left
	// in it, and will not be waiting on the dead half until its turn comes round.
	next := game.NextToMove()
	if next == "" {
		// Both decisions are visible, so the injector is now responsible for
		// emitting the separate resolution event that applies the scores.
		if s.componentIsLive(injector.Component) || s.sequencer.HasPendingEmission(injector.Component) {
			return false, ""
		}
		return true, injector.Component
	}
	if s.componentIsLive(string(next)) {
		return false, ""
	}
	// The tracker has not applied this component's decision yet, but it may
	// already be admitted and waiting to fan out. In that case killing the
	// emitter cannot block the tournament, and reporting a stall would make the
	// UI flash a warning that immediately disappears.
	if s.sequencer.HasPendingEmission(string(next)) {
		return false, ""
	}
	return true, string(next)
}

func (s *Session) componentIsLive(component string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sl := range s.slots {
		if sl.component == component && sl.running && !sl.quarantined {
			return true
		}
	}
	return false
}

// rehearsalPasses is how many times a candidate is made to replay the canonical
// tournament before it is let in. One pass proves it can reproduce the past; the
// rest are asking the same question again to see whether it gives the same
// answer, which is the only thing that distinguishes a pure function from one
// that happened to be right once.
const rehearsalPasses = 3

// errFinished is returned by the fault-injection controls once the tournament
// has ended.
var errFinished = errors.New("session: the tournament has finished")

// Finished reports whether the tournament has stopped.
func (s *Session) Finished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

func (s *Session) findLocked(component, id string) (*slot, error) {
	for _, sl := range s.slots {
		if sl.component == component && sl.id == id {
			return sl, nil
		}
	}
	return nil, fmt.Errorf("session: no replica %s/%s", component, id)
}
