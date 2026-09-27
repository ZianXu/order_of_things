package platform

import (
	"context"
	"fmt"

	"golang.design/x/chann"
)

// DivergenceError is returned when a replica observes a sequenced event from its
// own component that does not match what it computed itself.
//
// This is the divergence signal described in the design doc: it proves the two
// replicas of a component disagreed, but it does not say which one is right. The
// observing replica quarantines itself rather than the process dying.
type DivergenceError struct {
	Component string
	ReplicaId string
	Expected  any
	Received  any
}

func (e *DivergenceError) Error() string {
	return fmt.Sprintf(
		"divergence detected in component %q replica %q: computed %#v, sequenced %#v",
		e.Component, e.ReplicaId, e.Expected, e.Received,
	)
}

// SequencerClient is a component's handle on the sequenced stream.
//
// Every mutable field below is owned exclusively by the component's own
// goroutine: Send, Read and Close are all called from the event loop and from
// nowhere else. Nothing here is shared, so nothing here needs synchronizing. The
// two channels are the only crossing points, and the sequencer's view of this
// client (whether it is still subscribed) lives in the sequencer, updated by
// message rather than by a flag read across the boundary.
type SequencerClient struct {
	senderComponent   string
	senderComponentId string
	closed            bool
	quarantined       bool
	senderSeq         int64
	inflightEvent     *Event
	// replayExpected is an emission derived while rebuilding the log. It stays
	// local until the matching committed event arrives; only an expectation left
	// over at ReplayComplete is a genuinely new event to send live.
	replayExpected any
	sequencer      *Sequencer
	ingressCh      *chann.Chann[*Event]
	egressCh       *chann.Chann[*Event]
}

func NewSequencerClient(
	senderComponent string,
	senderComponentId string,
	sequencer *Sequencer,
) *SequencerClient {
	return &SequencerClient{
		senderComponent:   senderComponent,
		senderComponentId: senderComponentId,
		senderSeq:         0,
		sequencer:         sequencer,
		ingressCh:         sequencer.IngressCh(),
		egressCh:          chann.New[*Event](),
	}
}

// Send publishes a payload to the sequencer. The payload must be a comparable
// value type: it is compared by equality to detect replica divergence, and it is
// fanned out by value to every other component.
//
// Send is for live emissions. During replay Eventloop records the derived
// response locally with ExpectReplay instead, then compares it with the
// committed event that follows. Historical events are never sent back to the
// sequencer merely to be deduplicated.
func (c *SequencerClient) Send(payload any) {
	if c.inflightEvent != nil {
		// The divergence check pairs each emission with the sequenced event that
		// settles it, so a component may have at most one emission outstanding.
		// The event loop guarantees this structurally (one event in, at most one
		// event out), but it is load-bearing enough to assert rather than assume:
		// a second emission would leave the first unsettled and silently disable
		// the divergence check for that position.
		panic("sequencer client: Send called with an unsettled inflight event")
	}
	event := &Event{
		Header: Header{
			SenderComponent: c.senderComponent,
			SenderId:        c.senderComponentId,
			SenderSeq:       c.senderSeq,
		},
		Payload: payload,
	}
	c.inflightEvent = event
	// In a real deployment this would go over the network.
	c.ingressCh.In() <- event
}

// ExpectReplay records an emission derived while replaying. The next committed
// event from this component must match it exactly. Eventloop calls
// FinishReplay at the activation marker: a remaining expectation belongs to an
// unfinished live turn and is returned for normal transmission.
func (c *SequencerClient) ExpectReplay(payload any) {
	if c.replayExpected != nil {
		panic("sequencer client: replay derived a second unsettled emission")
	}
	c.replayExpected = payload
}

func (c *SequencerClient) FinishReplay() any {
	payload := c.replayExpected
	c.replayExpected = nil
	return payload
}

// onSequencerEvent is called on the sequencer goroutine.
func (c *SequencerClient) onSequencerEvent(event *Event) {
	c.egressCh.In() <- event
}

// onReplayComplete is called on the sequencer goroutine. It queues the marker
// behind the replayed events rather than signalling out of band, so a component
// observes it only after it has applied the whole log -- which is what makes it
// safe to use as the "the stream is empty, bootstrap it" trigger.
func (c *SequencerClient) onReplayComplete() {
	c.egressCh.In() <- &Event{Payload: ReplayComplete{}}
}

// Read blocks for the next sequenced event. It returns a nil event when ctx is
// cancelled, and a *DivergenceError when this replica's own computation does not
// match what the sequencer committed.
func (c *SequencerClient) Read(ctx context.Context) (*Event, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	case event := <-c.egressCh.Out():
		if event.IsReplayComplete() {
			return event, nil
		}
		if event.Header.SenderComponent == c.senderComponent {
			// Derive the next emission slot from the stream rather than from a
			// local counter. Whether this replica or its sibling won the position
			// is irrelevant: both are now aligned on the same next slot, so a
			// replica that lost every race so far can still take the next one.
			c.senderSeq = event.Header.SenderSeq + 1
		}
		if err := c.checkEmission(event); err != nil {
			c.quarantined = true
			c.Close()
			return nil, err
		}
		return event, nil
	}
}

// Close unsubscribes the client from the sequencer. Idempotent, and called from
// the component goroutine only. The sequencer may still deliver a few events
// before it processes the request; nobody is reading them by then.
func (c *SequencerClient) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.sequencer.unsubscribe(c)
}

// IsQuarantined reports whether this replica removed itself from the pair after
// detecting divergence. Read it from the component's own goroutine, or after that
// goroutine has been joined -- the authoritative signal for a supervisor is the
// error Eventloop.Run returns.
func (c *SequencerClient) IsQuarantined() bool {
	return c.quarantined
}

// checkEmission compares a sequenced event from this component against what this
// replica computed for the same position.
//
// The event may be this replica's own (it won the race) or its sibling's (it
// lost). Either way the payloads must be identical, because both replicas run the
// same deterministic FSM over the same event history. A mismatch means one of
// them is not deterministic.
func (c *SequencerClient) checkEmission(event *Event) error {
	if event.Header.SenderComponent != c.senderComponent {
		return nil
	}
	if c.replayExpected != nil {
		expected := c.replayExpected
		c.replayExpected = nil
		if expected != event.Payload {
			return &DivergenceError{
				Component: c.senderComponent,
				ReplicaId: c.senderComponentId,
				Expected:  expected,
				Received:  event.Payload,
			}
		}
		return nil
	}
	if c.inflightEvent == nil {
		return nil
	}
	expected := c.inflightEvent.Payload
	// Clear before comparing: each inflight emission is settled exactly once, and
	// a stale inflight must never be matched against a later event.
	c.inflightEvent = nil
	if expected != event.Payload {
		return &DivergenceError{
			Component: c.senderComponent,
			ReplicaId: c.senderComponentId,
			Expected:  expected,
			Received:  event.Payload,
		}
	}
	return nil
}
