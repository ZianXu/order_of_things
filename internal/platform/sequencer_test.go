package platform

import (
	"context"
	"errors"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func startSequencer(t *testing.T) (*Sequencer, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	s := NewSequencer()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return s, ctx
}

// join subscribes a client and drains its replay, returning the replayed events.
func join(t *testing.T, ctx context.Context, s *Sequencer, c *SequencerClient, replayed int) []*Event {
	t.Helper()
	s.Subscribe(c)
	events := make([]*Event, 0, replayed)
	for i := 0; i < replayed; i++ {
		events = append(events, mustRead(t, ctx, c))
	}
	marker := mustRead(t, ctx, c)
	if !marker.IsReplayComplete() {
		t.Fatalf("expected ReplayComplete after %d replayed events, got %#v", replayed, marker.Payload)
	}
	return events
}

func mustRead(t *testing.T, ctx context.Context, c *SequencerClient) *Event {
	t.Helper()
	event, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if event == nil {
		t.Fatal("read returned nil event (context cancelled)")
	}
	return event
}

func TestSequencerAssignsMonotonicSeqAndFansOutInOrder(t *testing.T) {
	s, ctx := startSequencer(t)

	producer := NewSequencerClient("producer", "p1", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, producer, 0)
	join(t, ctx, s, observer, 0)

	payloads := []string{"a", "b", "c"}
	for i, payload := range payloads {
		producer.Send(payload)

		own := mustRead(t, ctx, producer)
		seen := mustRead(t, ctx, observer)

		for name, event := range map[string]*Event{"producer": own, "observer": seen} {
			if event.Header.Seq != int64(i) {
				t.Errorf("%s: event %d got Seq %d, want %d", name, i, event.Header.Seq, i)
			}
			if event.Payload != payload {
				t.Errorf("%s: event %d got payload %v, want %v", name, i, event.Payload, payload)
			}
			if event.Header.SenderSeq != int64(i) {
				t.Errorf("%s: event %d got SenderSeq %d, want %d", name, i, event.Header.SenderSeq, i)
			}
		}
	}
}

func TestSequencerMarksAnAdmittedEmissionPendingUntilItFansOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	done := make(chan struct{})
	defer func() {
		cancel()
		<-done
	}()
	pacer := NewPacer(0)
	pacer.Pause()
	s := NewSequencer()
	s.SetPacer(pacer)
	go func() {
		defer close(done)
		s.Run(ctx)
	}()

	producer := NewSequencerClient("producer", "p1", s)
	join(t, ctx, s, producer, 0)
	producer.Send("decision")

	deadline := time.Now().Add(testTimeout)
	for !s.HasPendingEmission("producer") {
		if time.Now().After(deadline) {
			t.Fatal("admitted emission was never marked pending")
		}
		time.Sleep(time.Millisecond)
	}

	pacer.Resume()
	mustRead(t, ctx, producer)
	deadline = time.Now().Add(testTimeout)
	for s.HasPendingEmission("producer") {
		if time.Now().After(deadline) {
			t.Fatal("pending emission remained marked after fanout")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLateSubscriberReceivesFullReplayThenActivationMarker(t *testing.T) {
	s, ctx := startSequencer(t)

	producer := NewSequencerClient("producer", "p1", s)
	join(t, ctx, s, producer, 0)
	for _, payload := range []string{"a", "b"} {
		producer.Send(payload)
		mustRead(t, ctx, producer)
	}

	late := NewSequencerClient("late", "l1", s)
	replayed := join(t, ctx, s, late, 2)

	for i, want := range []string{"a", "b"} {
		if replayed[i].Payload != want {
			t.Errorf("replayed[%d] = %v, want %v", i, replayed[i].Payload, want)
		}
		if replayed[i].Header.Seq != int64(i) {
			t.Errorf("replayed[%d] Seq = %d, want %d", i, replayed[i].Header.Seq, i)
		}
	}
}

// The marker has to arrive after the replayed events on the same stream. If it
// were signalled out of band, a component could act on "the stream is empty"
// while it was still applying the log.
func TestActivationMarkerArrivesAfterEveryReplayedEvent(t *testing.T) {
	s, ctx := startSequencer(t)

	producer := NewSequencerClient("producer", "p1", s)
	join(t, ctx, s, producer, 0)
	for i := 0; i < 20; i++ {
		producer.Send(i)
		mustRead(t, ctx, producer)
	}

	late := NewSequencerClient("late", "l1", s)
	s.Subscribe(late)

	for i := 0; i < 20; i++ {
		event := mustRead(t, ctx, late)
		if event.IsReplayComplete() {
			t.Fatalf("marker arrived after only %d of 20 replayed events", i)
		}
		if event.Payload != i {
			t.Fatalf("replayed[%d] = %v, want %d", i, event.Payload, i)
		}
	}
	if marker := mustRead(t, ctx, late); !marker.IsReplayComplete() {
		t.Fatalf("expected activation marker, got %#v", marker.Payload)
	}
}

// Active-active arbitration: both replicas of a component compute the same
// answer and race to the sequencer. The first to arrive is admitted, the second
// is dropped as a duplicate, and both replicas observe the committed event.
func TestActiveActiveAdmitsExactlyOneReplicaPerPosition(t *testing.T) {
	s, ctx := startSequencer(t)

	r1 := NewSequencerClient("worker", "r1", s)
	r2 := NewSequencerClient("worker", "r2", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, r1, 0)
	join(t, ctx, s, r2, 0)
	join(t, ctx, s, observer, 0)

	for round := 0; round < 5; round++ {
		payload := "round-" + string(rune('a'+round))
		r1.Send(payload)
		r2.Send(payload)

		// Both replicas see the committed event; neither diverges.
		mustRead(t, ctx, r1)
		mustRead(t, ctx, r2)

		// The observer sees it exactly once.
		event := mustRead(t, ctx, observer)
		if event.Payload != payload {
			t.Fatalf("round %d: observer got %v, want %v", round, event.Payload, payload)
		}
		if event.Header.Seq != int64(round) {
			t.Fatalf("round %d: duplicate admitted, Seq = %d", round, event.Header.Seq)
		}
	}
}

// A replica that computes a different answer from its sibling quarantines itself
// rather than taking the process down. Which replica wins the race is genuinely
// nondeterministic, so the assertion is that exactly one of them survives.
func TestDivergingReplicaQuarantinesItself(t *testing.T) {
	s, ctx := startSequencer(t)

	r1 := NewSequencerClient("worker", "r1", s)
	r2 := NewSequencerClient("worker", "r2", s)
	join(t, ctx, s, r1, 0)
	join(t, ctx, s, r2, 0)

	r1.Send("correct")
	r2.Send("buggy")

	_, err1 := r1.Read(ctx)
	_, err2 := r2.Read(ctx)

	errs := 0
	for _, tc := range []struct {
		client *SequencerClient
		err    error
	}{{r1, err1}, {r2, err2}} {
		if tc.err == nil {
			if tc.client.IsQuarantined() {
				t.Errorf("%s: quarantined without an error", tc.client.senderComponentId)
			}
			continue
		}
		errs++
		var divergence *DivergenceError
		if !errors.As(tc.err, &divergence) {
			t.Errorf("%s: got %T, want *DivergenceError", tc.client.senderComponentId, tc.err)
		}
		if !tc.client.IsQuarantined() {
			t.Errorf("%s: diverged but did not quarantine", tc.client.senderComponentId)
		}
		if !tc.client.closed {
			t.Errorf("%s: quarantined but still subscribed", tc.client.senderComponentId)
		}
	}
	if errs != 1 {
		t.Fatalf("got %d quarantined replicas, want exactly 1", errs)
	}
}

// A replica rebuilding from the log re-derives every emission its sibling already
// made. Those re-derived emissions are transmitted, not suppressed: the sequencer
// drops them as duplicates, and observing the committed event realigns the
// replica's next emission slot with the log. Without that realignment a replica
// that lost every race would emit at a slot the sequencer has already passed, and
// would be silently dropped forever -- including after its sibling died.
func TestReplayRealignsSenderSeqWithTheLog(t *testing.T) {
	s, ctx := startSequencer(t)

	live := NewSequencerClient("worker", "live", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, observer, 0)
	join(t, ctx, s, live, 0)

	for _, payload := range []string{"a", "b"} {
		live.Send(payload)
		mustRead(t, ctx, live)
		mustRead(t, ctx, observer)
	}

	// The rejoining replica replays "a" and "b" without emitting anything of its
	// own -- the case that used to leave it permanently one slot behind.
	rejoin := NewSequencerClient("worker", "rejoin", s)
	join(t, ctx, s, rejoin, 2)
	if got := rejoin.senderSeq; got != 2 {
		t.Fatalf("senderSeq after replay = %d, want 2", got)
	}

	// Its first emission is admitted rather than dropped as a duplicate.
	rejoin.Send("c")
	event := mustRead(t, ctx, observer)
	if event.Payload != "c" || event.Header.SenderId != "rejoin" {
		t.Fatalf("rejoined replica emission not admitted: %#v", event)
	}
	if event.Header.SenderSeq != 2 {
		t.Fatalf("admitted SenderSeq = %d, want 2", event.Header.SenderSeq)
	}
}

// A response derived from the final replayed event has no committed counterpart.
// Replay keeps it local until the marker, then releases it as the first live
// emission.
func TestResponseNeededAtEndOfReplayIsTransmitted(t *testing.T) {
	s, ctx := startSequencer(t)

	seed := NewSequencerClient("seed", "s1", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, observer, 0)
	join(t, ctx, s, seed, 0)
	seed.Send("question")
	mustRead(t, ctx, seed)
	mustRead(t, ctx, observer)

	// This component only exists after the event it needs to react to is logged.
	responder := NewSequencerClient("responder", "r1", s)
	s.Subscribe(responder)
	if got := mustRead(t, ctx, responder).Payload; got != "question" {
		t.Fatalf("replayed %v, want \"question\"", got)
	}
	responder.ExpectReplay("answer")
	if marker := mustRead(t, ctx, responder); !marker.IsReplayComplete() {
		t.Fatalf("expected replay marker, got %#v", marker.Payload)
	}
	if pending := responder.FinishReplay(); pending != "answer" {
		t.Fatalf("pending replay response = %#v, want \"answer\"", pending)
	}
	responder.Send("answer")

	if got := mustRead(t, ctx, observer).Payload; got != "answer" {
		t.Fatalf("observer saw %v, want \"answer\"", got)
	}
}

// A historical response is compared with the committed event locally. It is
// never put back onto ingress for the sequencer to reject as a duplicate.
func TestHistoricalReplayResponseStaysLocal(t *testing.T) {
	s, ctx := startSequencer(t)

	seed := NewSequencerClient("seed", "s1", s)
	responder := NewSequencerClient("responder", "live", s)
	join(t, ctx, s, seed, 0)
	join(t, ctx, s, responder, 0)
	seed.Send("question")
	mustRead(t, ctx, seed)
	mustRead(t, ctx, responder)
	responder.Send("answer")
	mustRead(t, ctx, seed)
	mustRead(t, ctx, responder)

	rejoin := NewSequencerClient("responder", "rejoin", s)
	s.Subscribe(rejoin)
	if got := mustRead(t, ctx, rejoin).Payload; got != "question" {
		t.Fatalf("replayed %v, want \"question\"", got)
	}
	rejoin.ExpectReplay("answer")
	if got := mustRead(t, ctx, rejoin).Payload; got != "answer" {
		t.Fatalf("replayed %v, want \"answer\"", got)
	}
	if marker := mustRead(t, ctx, rejoin); !marker.IsReplayComplete() {
		t.Fatalf("expected replay marker, got %#v", marker.Payload)
	}
	if pending := rejoin.FinishReplay(); pending != nil {
		t.Fatalf("historical replay response remained pending: %#v", pending)
	}
}

func TestSequencerRejectsGapsAndDuplicates(t *testing.T) {
	s, ctx := startSequencer(t)

	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, observer, 0)

	send := func(senderSeq int64, payload string) {
		s.IngressCh().In() <- &Event{
			Header:  Header{SenderComponent: "raw", SenderId: "x", SenderSeq: senderSeq},
			Payload: payload,
		}
	}

	send(3, "gap-at-start") // a new component must start at 0
	send(0, "first")
	send(0, "duplicate")
	send(2, "gap")
	send(1, "second")

	for _, want := range []string{"first", "second"} {
		if got := mustRead(t, ctx, observer).Payload; got != want {
			t.Fatalf("admitted %v, want %v", got, want)
		}
	}
	// Nothing else should have been admitted.
	drained, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if event, _ := observer.Read(drained); event != nil {
		t.Fatalf("unexpected extra admitted event: %#v", event.Payload)
	}
}

// Unsubscribing is a message rather than a flag the sequencer reads across a
// goroutine boundary, so the cutoff is ordered rather than instantaneous. Join
// and leave requests share one channel, so a subscription accepted after a
// close proves the close was already applied.
func TestClosedClientIsDroppedFromTheFanout(t *testing.T) {
	s, ctx := startSequencer(t)

	quiet := NewSequencerClient("quiet", "q1", s)
	producer := NewSequencerClient("producer", "p1", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, quiet, 0)
	join(t, ctx, s, producer, 0)
	join(t, ctx, s, observer, 0)

	quiet.Close()

	// Ordered behind the unsubscribe on the same channel.
	probe := NewSequencerClient("probe", "pr1", s)
	join(t, ctx, s, probe, 0)

	producer.Send("after-close")
	mustRead(t, ctx, producer)
	mustRead(t, ctx, observer)
	mustRead(t, ctx, probe)

	if n := quiet.egressCh.Len(); n != 0 {
		t.Fatalf("closed client received %d events, want 0", n)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, ctx := startSequencer(t)

	client := NewSequencerClient("worker", "w1", s)
	observer := NewSequencerClient("observer", "o1", s)
	join(t, ctx, s, client, 0)
	join(t, ctx, s, observer, 0)

	client.Close()
	client.Close()
	client.Close()

	// A second unsubscribe must not evict an unrelated client.
	probe := NewSequencerClient("probe", "pr1", s)
	join(t, ctx, s, probe, 0)

	observer.Send("still-here")
	if got := mustRead(t, ctx, observer).Payload; got != "still-here" {
		t.Fatalf("observer saw %v, want \"still-here\"", got)
	}
	if got := mustRead(t, ctx, probe).Payload; got != "still-here" {
		t.Fatalf("probe saw %v, want \"still-here\"", got)
	}
}
