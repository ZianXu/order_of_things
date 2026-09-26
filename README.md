# The Order of Things

**A debugger for distributed systems, built on replay.**

You cannot step through a distributed system. The interesting bugs live in the
interleaving — which goroutine won which race, which node was mid-restart when
the message arrived — and by the time you have a stack trace the interleaving is
gone. Attaching a debugger changes the timing you were trying to observe.

Replay is the way out, and it costs one thing: every component has to be a
deterministic state machine fed a single ordered log. Buy that, and a great deal
follows.

This is a working demonstration of what follows. It runs an iterated
Prisoner's Dilemma tournament — four strategies, each as an active-active pair of
replicas, eleven components in all, every one on its own goroutine — and invites
you to break it while it runs.

## The claim

Kill a replica mid-game. Restart it. Bring one back corrupted. **The
tournament ends exactly the same way**, and the corrupted replica never gets back
in.

That is not a fixed random seed doing the work. A seeded generator repeating
itself is a property of the generator, not of this system, which is why the seed
here is a constant and is not offered as a control. The claim is about what
survives the shocks *you* introduce, with no coordination between components
beyond the order of the log.

## The races are still there

It would be easy to read the above as "this system is deterministic, therefore
nothing races." It is the opposite. **Two runs of this tournament do not necessarily
produce the same log, and never need to.**

Every player is two replicas computing the same answer and racing to the
sequencer. Whichever arrives first is admitted; the other is dropped as a
duplicate. Which one wins is a real-time outcome, and it differs every run — open
the event log and you will see `flip/r0` on a position where the last run had
`flip/r1`. Nine goroutines, contending for one gate, all the way down.

What is identical is the *logical* outcome: the same games, the same decisions,
the same scores, the same final state. So the tests compare the log with the
winning replica's identity deliberately excluded — that field is nondeterministic
on purpose, and asserting on it would be asserting on the wrong thing:

```go
// SenderId is excluded on purpose: which replica of a component won the race to
// the sequencer is a real-time outcome and is legitimately nondeterministic.
// Everything else -- the order, the emitting component, the payload -- is not.
```

This is the whole philosophy, and it is the opposite of the usual instinct. The
goal is not to remove nondeterminism — you cannot, and a system that tried would
be slower and no more correct. The goal is to **confine** it: let the races happen
wherever they cannot change the answer, and make every interleaving converge on
the same logical outcome. Determinism is not a property you switch on globally.
It is something you scope, and the skill is scoping it no tighter than it needs to
be.

Everything else here follows from that. Active-active is safe because it does not
matter which replica wins. Pacing is safe because it changes when events are
admitted, not which. And v2 — dependency-aware concurrency, out of scope for this
build — is the same idea taken further: let genuinely disjoint games run at once,
because their interleaving cannot change the result either.

## What replay buys you

**Recovery.** A replica that dies rebuilds its entire state by replaying the log.
No snapshots, no state transfer, no catch-up protocol — it re-derives the past
from the same events everyone else saw.

**Verification before trust.** This is the part that makes it a debugger rather
than a runtime, and it is worth being exact about what it needs. A candidate
replica replays the *entire* canonical tournament in private — every event, every
emission, every state checksum — before it is connected to anything, and is refused
if it diverges anywhere. Not "we noticed later" — refused at the door, at the
exact event where it first went wrong:

```
Rehearsal refused: session: flipper/r1 failed rehearsal at seq 7:
computed GameDecision{flipper defect}, canonical is GameDecision{flipper cooperate}
```

Replaying only the log *so far* is not enough, and the gap is not academic: a
corrupted decision function looks perfect until it is asked to decide, and the
history a replica happens to rejoin against may never have asked it. Let such a
replica through and it wins a race, its wrong answer enters the log, and the
*healthy* sibling is the one that disagrees with the record and quarantines —
after which every later restart is refused for disagreeing too.

Which leads to the honest limit, and it is a real one: **you cannot catch a
latent decision bug at rejoin time by any mechanism, because the bug has not
happened yet.** You can only catch it by asking the candidate a question you
already know the answer to. Production has no canonical future to ask about, so
this particular check cannot live at a live rejoin gate anywhere — here or
elsewhere. Where it does live is before deployment: replay a recorded trace
against the new build and refuse to ship it if it diverges. That is deterministic
simulation testing, and it is what FoundationDB's simulator, TigerBeetle's VOPR
and Antithesis are all doing. Determinism plus a recorded trace is what makes
having a known answer possible at all.

The three checks in this project need quite different things, and only two of
them generalise to a running system:

These are checksums, not Merkle roots. FNV-1a is fast and has good avalanche, and
it is trivial to find a collision for deliberately — which is fine, because what
is being defended against is a replica that computes the wrong answer *by
accident*: a bad deploy, a stray `time.Now`, environment drift. It would be worth
nothing against a replica trying to pass. **The threat model is bugs, not
Byzantine replicas**; that one would want a cryptographic hash and signatures over
the chain, and is a different project.

| check | needs | catches | live? |
|---|---|---|---|
| emission comparison | a live sibling | that they disagreed — never which is wrong | yes |
| state checksum vs. the log so far | a reference for the past | a corrupted transition function | yes |
| rehearsal vs. the canonical run | a recorded canonical trace | anything that would ever diverge | no — pre-deployment |

**Cheap redundancy.** Active-active is normally hard: you cannot re-run
side-effecting code twice and reconcile the results. It is trivial here precisely
*because* the components are deterministic — there is nothing to reconcile, so
the race can simply be allowed. The sequencer admits the first response and drops
the second as a duplicate, and it never has to ask which was better.

## Defects, and the mechanisms that catch them

Three defects can be injected, and they are caught by different machinery, which
is why more than one mechanism exists:

| defect | what it corrupts | caught by |
|---|---|---|
| `WrongDecision` | the **decision** function, always | rehearsal, every time — this is what the UI injects |
| `CorruptPayoff` | the **transition** function | the canonical state-checksum chain |
| `ImpureClock` | the decision function, *intermittently* | rehearsal, usually — and see below |

A bad decision is invisible to the chain: a replica deciding wrongly still
computes its state perfectly. A bad transition is invisible to the pair: a
sibling running the same defect agrees with it, which is the known limit of
active-active. Only a reference computed before either replica ran can say which
one is wrong.

Quarantine refuses a divergent *instance*, not the name forever: redeploy the
replica clean and it is let back in, having earned it by replaying correctly.

All three name the actually-defective replica, because on the replay path a
replica is compared against recorded history it cannot influence. On the *live*
path that is not true: two replicas race, the loser is quarantined, and it may
well be the healthy half. Divergence detection proves they disagreed — never
which was right.

### Why the injected defect is not the interesting one

`ImpureClock` — reading `time.Now()` inside a decision — was the obvious choice.
It is *the* canonical violation of invariant 3, and it is what the UI injected,
until it produced a bug report worth the whole detour.

The symptom: click the bug button a few times and eventually the replica comes
back **running** instead of refused, and moments later its perfectly healthy
partner is quarantined instead. Reproducible in about four clicks on a remote
server. Not reproducible at all on the development machine — 200 attempts, 200
refusals.

Rehearsal asks a player for every decision it makes across the canonical
tournament. Flipper makes twelve. A clock-dependent defect flips a coin per
decision, so it should survive all twelve by chance 0.5¹² ≈ **0.024%** of the
time. Four clicks implies something nearer **25%**, a thousand times higher — and
that is only possible if the twelve flips are not independent. They are not.
Rehearsal asks all twelve inside a loop lasting *microseconds*, so wherever the
clock's granularity is coarser than that loop, every call reads the same instant
and the twelve coins collapse into **one**. It comes out honest half the time.
Four clicks then reproduce it with 94% probability, which is what was observed.
Once through, it goes live — where decisions are a second apart, the coin really
does flip, it wins a race, and its wrong answer enters the log.

The lesson is not that the check is weak. The check is as strong as a check can
be, and the arithmetic above is the proof:

> **An intermittent fault will pass a finite examination.** You cannot verify your
> way out of it, and no amount of repetition closes the gap — it only moves the
> decimal point.

Rehearsal makes three passes rather than one for exactly this reason: a pure
function gives the same answer every time it is asked, and asking repeatedly is
the most any finite check can do. It improves the odds and settles nothing.

So the defect the UI injects is deterministically wrong instead, and is refused
every time on every machine. `ImpureClock` stays, reachable from the API and the
CLI, because it is the honest illustration of the limit — and because a defect
that is invisible on the developer's laptop and reliable in production is not a
contrived example. It is the normal shape of the worst bugs there are.

### Where the guardrail ends

The previous section is about a fault that is *sometimes* wrong. There is a
plainer limit that applies even to one that is *always* wrong: **rehearsal only
asks the questions the recorded history happens to ask.**

A candidate is made to replay the canonical tournament — twenty-five games,
seventy-five events. That is not every state four strategies can reach. A defect
sitting on a path the canonical run never takes is not caught late or caught
probabilistically; it is not caught at all, because nothing in the recording
exercises it. This is not special to this project. It is the ordinary limit of
replay and regression testing everywhere: **they test what you recorded.**

So rehearsal is a guardrail, not a proof. It raises the cost of shipping a broken
replica and makes a whole class of them impossible to sneak in. It does not make
a correct system.

Which means divergence still happens in production, and the honest question is
what the system does then. It does the only thing available to it: quarantine one
half, so that a single unambiguous line of events survives.

That is worth something real, and it is worth being precise about what. It buys
**consistency**, not correctness. Afterwards there is exactly one history to
reason about rather than a fork — one log, one order, one state at every point,
and no argument about which timeline is the real one. What it emphatically does
not buy is any assurance that the surviving log is the *right* one. The replica
that won the race may be the broken one, and the events it committed are already
committed.

Deciding which side was right, and what to do about what has already been
written, is reconciliation, and in every real system that ends in a person
looking at it. There is no mechanism here — or, as far as I know, anywhere — that
does it unaided. The design doc names this and declines to solve it, and so does
this build.

That is less defeatist than it sounds, and it is the reason the rest of the
machinery is worth having. Manual reconciliation is *tractable* against a single
ordered log that replays deterministically from any point with a state checksum at
every step: you can find where it went wrong, replay both sides of it, and see
exactly what diverged. It is close to hopeless against a distributed system where
the interleaving is gone and every node has a slightly different story. The
mechanisms in this project do not remove the human from the loop. They make the
loop something a human can actually close.

## The coordination table

The four strategies are not decoration. Each needs a different amount of the
world to make up its mind, and that is exactly how much coordination it costs:

| strategy | reads | coordination required |
|---|---|---|
| Cooperator | nothing | none |
| Flipper | its own last game | private state |
| Retaliator | its last game against *this* opponent | the (A, B) pair |
| CopyLeader | the global leaderboard | genuine global ordering |

In the code this is one function type, and what each implementation reads out of
the store is the whole of the difference between them (`internal/strategy`).

## Running it

```
go run ./cmd/server        # http://localhost:8080
go run ./cmd/tournament    # headless, verifies against the golden record
go test -race ./...
```

### The committed tournament

`testdata/golden.json` holds the canonical tournament — its full event log, the
state checksum after every event, and the final standings. It is checked in, and it is
a **regression fixture, not a cache**. Change a payoff, a strategy, the pairing
draw or the hashing scheme and this fails:

```
this build produces a different tournament than the committed one:
  state checksum b6321599e4be266b, committed 1b034db9c4c9e1ca
  leaderboard [{copy-leader 28} {flipper 24} …], committed [{copy-leader 26} …]
```

If that was intended, regenerate it deliberately and read the diff before
committing:

```
go test ./internal/session -run Golden -update
```

A change there you did not intend is the system telling you it stopped being the
same system, which for a project whose entire claim is reproducibility is the one
thing worth failing a build over.

Nothing at runtime reads that file as truth. The server regenerates the canonical
tournament from the code at startup — it costs a few milliseconds — validates
replicas against what it just computed, and only *compares* the committed record,
reporting a mismatch rather than believing it. That distinction matters: a
reference left over from an older build, trusted, would refuse perfectly healthy
replicas for disagreeing with something that was itself wrong.

The page is driven entirely by folding an ordered feed of events. Nothing is
polled, nothing is recomputed from a rendered view: a game row appears because a
`new-game` event arrived, a decision fills in because a `decision` event arrived,
the leaderboard moves because a game completed. The browser is one more replica of
the same state machine, not a dashboard bolted onto it.

## Layout

```
internal/platform   sequencer, client, event loop, pacer — knows nothing of games
internal/fsm        the replicated state machine and its state-checksum chain
internal/injector   admission policy: when a game may start
internal/strategy   the four decision functions
internal/tracker    scores read model and the UI event feed
internal/golden     canonical outcomes and chains, persisted
internal/session    one system instance, plus supervision and fault injection
internal/web        HTTP, SSE, and the page
```

## Running it in public

Sessions are real work — each one is a live tournament with a sequencer and nine
components on their own goroutines — so the server reclaims them and refuses to
accumulate them:

| | default | flag |
|---|---|---|
| tournaments **in progress** | 64, then `503` with `Retry-After` | `-max-sessions` |
| reclaimed when unwatched for | 2 minutes | `-idle` |
| reclaimed regardless after | 1 hour | `-max-age` |

The cap counts tournaments *in progress*, not entries in the registry. A finished
one has already stopped everything it started and costs nothing but the memory
holding its result, so counting it would let a handful of quick tournaments lock
out new visitors for no reason. The slot is also reserved before any work begins,
so a burst of rejected requests costs nothing — checking first and starting
afterwards would let them all pass the check and each spin up a sequencer and
eleven components before being turned away.

An open event stream counts as activity, so a viewer watching — or paused part
way through explaining something — is not reclaimed out from under themselves.
Close the tab and the session stops counting as watched immediately, which is why
the idle window can be short. The stream ends when the tournament does: there is
nothing further to send, and holding it open would keep a finished session alive
for as long as the tab existed.

Request bodies are capped at 4KB and the whole request must arrive within 15
seconds. Every request this server accepts is a few dozen bytes of JSON, so a
slow or oversized one is either broken or hostile, and without a bound it can
hold a handler goroutine open inside the decoder for as long as the sender likes.
`WriteTimeout` stays unset on purpose — any write deadline would cut off the
event stream.

Shutdown is bounded. A session that will not stop is abandoned after ten seconds
with a log line rather than waited on, because blocking turns one stuck session
into a stuck server: the reclaim loop would never tick again and nothing would
ever be reclaimed after the first hang. Leaking a few goroutines is bounded by
how often that happens; wedging the reclaimer is bounded by nothing.

## What v2 would actually take

The design doc says v2 changes only the injector's admission policy and leaves
the state machines alone. That is not true of this build, and the difference is
worth being straight about.

The admission rule itself is small — "a game may start when both its participants
are free" rather than "when the previous one has resolved" — but it has a trap in
it. Taken literally it reorders games: if `G7` is blocked and `G8` is not, `G8`
starts first, and a player in both sees its own history in a different order,
which changes what it decides and breaks the v1/v2 outcome diff before the
comparison is even run. The rule needs head-of-line reservation, so a skipped
game reserves its participants and nothing containing them can jump the queue.

The larger part is not the injector at all:

- **The store holds one game.** `GameStore` deliberately permits a single game in
  flight and panics otherwise, and every strategy reads one `CurrentGame`. v2
  needs a multi-game store and per-strategy dependency views.
- **One emission per event stops being enough.** Two games can become admissible
  on the same event, and the event loop returns one payload.
- **The log stops being reproducible.** Admission still depends only on the
  applied prefix, so replicas still agree — but *which* prefix exists at a given
  moment depends on which concurrent game resolves first. The outcome stays
  deterministic; the log does not.
- **Which means the state checksum chain, and rehearsal with it, do not survive.**
  The chain folds in applied sequence and completion order, so it differs run to
  run. v2 needs an order-independent outcome digest, and there is no canonical log
  to rehearse a candidate against — leaving only the two live checks, and losing
  the one that could say which replica was wrong.

So v2 buys throughput and costs the strongest verification mechanism here. That
trade is more interesting than the speed number, and it is not one the design doc
anticipated.

## Deliberate non-goals

* Single process with goroutines rather than services across hosts
* Head-of-line blocking in state transition for visualization in the UI and simplicity
* The sequencer is not itself made highly available (a real one would be Raft-backed)
* Faults are simulated by stopping a loop rather than by signalling a process
* Only divergence is detected, not correctness

Each is a scoping decision, argued in `deterministic-tournament-design-doc.md`.

---

Inspired by [Nicky Case's *The Evolution of Trust*](https://ncase.me/trust/) and [Adaptive's *Aeron Sequencer*](https://aeron.io/aeron-sequencer/)

Music by Zian Xu, made with [Suno](https://suno.com/s/yjNvZutbRTCvK0ko) and
embedded in the binary, so the demo has no runtime dependency on anything outside
itself. There is a mute button.
