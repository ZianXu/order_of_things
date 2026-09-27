package platform

import (
	"context"
	"log"
)

// EventloopHandler applies one sequenced event to the component's state machine
// and optionally returns a payload to emit in response.
//
// It must be a pure function of the events it has been given: no time.Now, no
// ungoverned randomness, no map-iteration-order dependence. Both replicas of a
// component run this same function over the same event history and are expected
// to compute the same answer -- which is what makes it safe for whichever of them
// reaches the sequencer first to be the one that counts.
//
// The handler is also invoked once with a ReplayComplete event, after the log has
// been replayed and before any live event. Components that need to bootstrap the
// stream (the game injector emitting the first game) do it there, because that is
// the first point at which an emission will actually be transmitted.
type EventloopHandler func(*Event) any

// StateValidator checks a replica's state checksum against a reference while it
// replays the log at boot. Returning an error refuses it rejoin.
//
// This is the second, independent quarantine path. The one built into the
// sequencer client compares what a replica *emitted* against what the log
// recorded, which catches a corrupted decision function but says nothing about
// which of two disagreeing replicas is right. This one compares what a replica
// *computed* against a reference established before it ran, which catches a
// corrupted transition function and does say which side is wrong.
//
// It deliberately stops at the end of replay. The chain describes the canonical
// tournament, so if a defective replica ever wins a race its bad event enters the
// log and every component's state legitimately stops matching -- running this
// check live would then quarantine the entire system on one bad admission,
// healthy components included. Admission control is what this is for: prove a
// replica rebuilt the past correctly, then let it take its chances in the
// present like everyone else.
type StateValidator interface {
	Validate(seq int64, root uint64) error
}

// Eventloop is the common substrate every component runs on: read a sequenced
// event, apply it, emit at most one event in response.
type Eventloop struct {
	eventHandler    EventloopHandler
	sequencerClient *SequencerClient
	sequencer       *Sequencer

	stateRoot func() uint64
	validator StateValidator
	// replaying is true until the in-band activation marker arrives, which is
	// exactly the window the state-checksum check applies to.
	replaying bool
}

// Option configures an event loop at construction.
type Option func(*Eventloop)

// WithStateValidation checks the component's state checksum against a reference for
// every event it replays at boot, and not afterwards. That is the point of it: a
// restarting replica is refused rejoin before it can serve, rather than after it
// has already answered something wrong.
func WithStateValidation(stateRoot func() uint64, validator StateValidator) Option {
	return func(e *Eventloop) {
		e.stateRoot, e.validator = stateRoot, validator
	}
}

func NewEventloop(
	component string,
	componentId string,
	sequencer *Sequencer,
	eventHandler EventloopHandler,
	opts ...Option,
) *Eventloop {
	e := &Eventloop{
		sequencerClient: NewSequencerClient(component, componentId, sequencer),
		eventHandler:    eventHandler,
		sequencer:       sequencer,
		replaying:       true,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run subscribes to the sequencer and processes events until ctx is cancelled or
// the replica quarantines itself. A *DivergenceError return means this replica
// disagreed with its sibling and has removed itself from the pair; the rest of
// the system carries on with the surviving replica.
func (e *Eventloop) Run(ctx context.Context) error {
	e.sequencer.Subscribe(e.sequencerClient)
	defer e.sequencerClient.Close()

	for {
		if ctx.Err() != nil {
			log.Println("Eventloop shutting down")
			return nil
		}
		event, err := e.sequencerClient.Read(ctx)
		if err != nil {
			log.Printf("Eventloop quarantined: %v", err)
			return err
		}
		if event == nil {
			return nil
		}
		wasReplaying := e.replaying
		payload := e.eventHandler(event)

		// Validate before emitting. A replica whose state has diverged must not
		// be allowed to put a decision derived from that state onto the stream.
		if err := e.validateState(event); err != nil {
			log.Printf("Eventloop quarantined: %v", err)
			e.sequencerClient.quarantined = true
			e.sequencerClient.Close()
			return err
		}
		if wasReplaying && !event.IsReplayComplete() && payload != nil {
			// A replay response proves this replica reached the same conclusion as
			// the committed history. Keep it local until that history either
			// confirms it or ends.
			e.sequencerClient.ExpectReplay(payload)
			continue
		}
		if event.IsReplayComplete() {
			if pending := e.sequencerClient.FinishReplay(); pending != nil {
				if payload != nil {
					panic("eventloop: replay ended with both a pending and bootstrap emission")
				}
				payload = pending
			}
		}
		if payload != nil {
			e.sequencerClient.Send(payload)
		}
	}
}

func (e *Eventloop) validateState(event *Event) error {
	if event.IsReplayComplete() {
		e.replaying = false
		return nil
	}
	if !e.replaying || e.validator == nil || e.stateRoot == nil {
		return nil
	}
	return e.validator.Validate(event.Header.Seq, e.stateRoot())
}

// IsQuarantined reports whether this replica removed itself after divergence.
func (e *Eventloop) IsQuarantined() bool {
	return e.sequencerClient.IsQuarantined()
}
