package platform

import (
	"context"
	"log"
	"sync"

	"golang.design/x/chann"
)

// Sequencer is the single admission gate for the whole system. It is
// deliberately dumb: it knows nothing about games, actors or strategies. It
// assigns a global sequence number to every admitted event and fans that event
// out to every subscriber in the same order.
// subscription is a join or leave request. Membership changes travel as messages
// on the same channel as everything else, so s.clients is only ever touched by
// the sequencer's own goroutine and needs no synchronization.
type subscription struct {
	client *SequencerClient
	join   bool
}

type Sequencer struct {
	subCh     *chann.Chann[subscription]
	ingressCh *chann.Chann[*Event]
	clients   []*SequencerClient
	eventLog  []*Event
	sequence  int64
	// senderSeqHwm is keyed on SenderComponent, not SenderId. That is what makes
	// active-active work: both replicas of a component emit the same SenderSeq for
	// the same logical decision, so the first to arrive is admitted and the second
	// is dropped as a duplicate. The race is resolved per event, not per replica.
	senderSeqHwm map[string]int64
	// pending records a component emission that has passed admission but has not
	// yet been fanned out. The pacer can hold that gap open, so observers need
	// to distinguish it from a component that is genuinely unable to answer.
	pendingMu sync.RWMutex
	pending   map[string]*Event
	// pacer optionally throttles admission. Nil means flat out.
	pacer *Pacer
}

func NewSequencer() *Sequencer {
	return &Sequencer{
		subCh:        chann.New[subscription](),
		ingressCh:    chann.New[*Event](),
		clients:      make([]*SequencerClient, 0),
		sequence:     0,
		senderSeqHwm: make(map[string]int64),
		pending:      make(map[string]*Event),
	}
}

// SetPacer throttles admission. Must be called before Run.
func (s *Sequencer) SetPacer(pacer *Pacer) {
	s.pacer = pacer
}

// IngressCh is the channel components publish to. Handed to each component so it
// can construct its own client.
func (s *Sequencer) IngressCh() *chann.Chann[*Event] {
	return s.ingressCh
}

// Subscribe registers a client to receive the replayed log followed by all
// subsequent events. Safe to call from any goroutine, including while the
// sequencer is running.
func (s *Sequencer) Subscribe(client *SequencerClient) {
	s.subCh.In() <- subscription{client: client, join: true}
}

// unsubscribe removes a client. Requests are processed in order with Subscribe,
// so a join enqueued after a leave is guaranteed to be applied after it.
func (s *Sequencer) unsubscribe(client *SequencerClient) {
	s.subCh.In() <- subscription{client: client, join: false}
}

func (s *Sequencer) Run(ctx context.Context) {
	log.Println("Sequencer started")
	for {
		// The pseudo-random choice between two ready cases is safe here, unlike
		// inside a state transition: subscription is not part of the logical event
		// stream. Whichever case wins, a joining client still sees every admitted
		// event exactly once -- either as part of its replay or as a live fanout,
		// never both and never neither.
		select {
		case <-ctx.Done():
			log.Println("Sequencer exited")
			return
		case sub := <-s.subCh.Out():
			if !sub.join {
				s.dropClient(sub.client)
				continue
			}
			s.clients = append(s.clients, sub.client)
			for _, event := range s.eventLog {
				sub.client.onSequencerEvent(event)
			}
			sub.client.onReplayComplete()
		case event := <-s.ingressCh.Out():
			if !s.canAdmit(event) || !event.isLive() {
				// Rejected duplicates are not paced: the sibling replica's copy
				// or a dead replica's queued work should not cost the viewer a tick.
				continue
			}
			s.setPending(event)
			if s.pacer != nil && !s.pacer.Wait(ctx) {
				return
			}
			if !event.isLive() {
				s.setPending(nil)
				continue
			}
			s.commit(event)

			// Sequence onto a copy. The sender still holds the original as its
			// inflight event, so mutating it here would be a data race and would
			// let one component's bookkeeping be rewritten by another goroutine.
			sequenced := &Event{
				Header:  event.Header,
				Payload: event.Payload,
			}
			sequenced.Header.Seq = s.sequence
			s.sequence++
			s.eventLog = append(s.eventLog, sequenced)

			for _, client := range s.clients {
				client.onSequencerEvent(sequenced)
			}
			s.setPending(nil)
		}
	}
}

// HasPendingEmission reports whether component has an admitted event waiting to
// be delivered. Safe to call while the sequencer is running.
func (s *Sequencer) HasPendingEmission(component string) bool {
	s.pendingMu.RLock()
	defer s.pendingMu.RUnlock()
	event := s.pending[component]
	return event != nil && event.isLive()
}

func (s *Sequencer) setPending(event *Event) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if event != nil {
		s.pending[event.Header.SenderComponent] = event
		return
	}
	for component := range s.pending {
		delete(s.pending, component)
	}
}

// Sequence returns the number of events admitted so far. Only safe to call once
// the sequencer has stopped.
func (s *Sequencer) Sequence() int64 {
	return s.sequence
}

// EventLog returns the admitted events in order. Only safe to call once the
// sequencer has stopped; used by tests and by golden-outcome comparison.
func (s *Sequencer) EventLog() []*Event {
	return s.eventLog
}

// canAdmit applies the per-component duplicate and gap check without mutating
// the high-water mark. A paced event is committed only after it is about to be
// sequenced, so killing its origin can withdraw it while it waits for a beat.
func (s *Sequencer) canAdmit(event *Event) bool {
	hwm, seen := s.senderSeqHwm[event.Header.SenderComponent]
	switch {
	case !seen && event.Header.SenderSeq == 0:
	case seen && event.Header.SenderSeq == hwm+1:
	case seen && event.Header.SenderSeq <= hwm:
		// The sibling replica already won this position. Expected in active-active.
		return false
	default:
		log.Printf(
			"sequencer: gap from component %q replica %q: got senderSeq %d, expected %d",
			event.Header.SenderComponent, event.Header.SenderId,
			event.Header.SenderSeq, hwm+1,
		)
		return false
	}
	return true
}

func (s *Sequencer) commit(event *Event) {
	s.senderSeqHwm[event.Header.SenderComponent] = event.Header.SenderSeq
}

func (e *Event) isLive() bool { return e.origin == nil || e.origin.isActive() }

func (s *Sequencer) dropClient(client *SequencerClient) {
	for i, subscribed := range s.clients {
		if subscribed == client {
			s.clients = append(s.clients[:i], s.clients[i+1:]...)
			return
		}
	}
}
