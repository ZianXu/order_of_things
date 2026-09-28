# The Order of Things

**A debugger for distributed systems, built on replay.**

You cannot step through a distributed system. The interesting bugs live in the
interleaving — which goroutine won which race, which node was mid-restart when
the message arrived — and by the time you have a stack trace the interleaving is
gone. Attaching a debugger changes the timing you were trying to observe.

Event sourcing is one way out, and it costs one thing: every component has to be a
deterministic state machine fed a single ordered log. Buy that, and a great deal
follows.

This is a working demonstration of what follows. It runs an iterated
Prisoner's Dilemma tournament — four strategies, each as an active-active pair of
replicas, eleven components in all, every one on its own goroutine — and invites
you to break it while it runs.

## The claim

Kill a replica mid-game. Restart it. Bring one back corrupted. **The
tournament ends exactly the same way**, thanks to a few key properties that
reinforce one another.

* An append-only, durable log serves as the source of truth and drives every component's state.
* State machines can be replicated across the system.
* Active-active replication becomes straightforward.
* Replay at startup makes it possible to detect whether new logic would diverge from recorded outcomes.

## The races are still there

It would be easy to read the above as "this system is deterministic, therefore
nothing races." It is the opposite. **Two runs of this tournament do not necessarily
produce the same log, and never need to.**

Depending on which replica wins a race or is killed, quarantined, or restarted,
the event log across runs will not be byte-identical. What is identical is the *logical* outcome:
the same games, the same decisions, the same scores, the same final state. So the tests compare
the log with the winning replica's identity deliberately excluded — that field is nondeterministic
on purpose.

This is the whole philosophy, and it is the opposite of the usual instinct. The
goal is not to remove nondeterminism — you cannot, and a system that tried would
be slower and no more correct. The goal is to **confine** it: let the races happen
wherever they cannot change the answer, and make every interleaving converge on
the same logical outcome. Determinism is not a property you switch on globally.
It is something you scope, and the skill is scoping it no tighter than it needs to
be.

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

## A challenge for you

A v2 was originally planned to introduce more deliberate races: games created and
emitted concurrently, with each player able to make decisions as quickly as possible.
Instead of implementing it, I offer it as a challenge to you.

Can you build a system in which components track distributed state, each caring only
about what is relevant to its own business logic and moving as fast as possible? It is
a system of chaos and races, yet if the state machines and dependency gates are designed
and implemented properly everywhere, the overall system can still produce deterministic
outcomes: in this example, the same results for the same series of games.

### Where the guardrail ends

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
reason about rather than a set of interleaved decisions — one log, one order, one state at every point.
What it emphatically does not buy is any assurance that the surviving log is the *right* one.
The replica that won the race may be the broken one, and the events it committed are already
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

## Running it

```
go run ./cmd/server        # http://localhost:8080
go run ./cmd/tournament    # headless, verifies against the golden record
go test -race ./...
```

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

## Deliberate non-goals

* Single process with goroutines rather than services across hosts
* Head-of-line blocking for V1 in state transition for visualization in the UI and simplicity
* The sequencer is not itself made highly available (a real one would be Raft-backed)
* Faults are simulated by stopping a loop rather than by signalling a process
* Only divergence is detected, not correctness

Each is a scoping decision, argued in `deterministic-tournament-design-doc.md`.

---

Inspired by [Nicky Case's *The Evolution of Trust*](https://ncase.me/trust/) and [Adaptive's *Aeron Sequencer*](https://aeron.io/aeron-sequencer/)

Music by Zian Xu, made with [Suno](https://suno.com/s/yjNvZutbRTCvK0ko) and
embedded in the binary, so the demo has no runtime dependency on anything outside
itself. There is a mute button.
