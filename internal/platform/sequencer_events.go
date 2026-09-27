package platform

// Header carries the sequencing metadata attached to every event.
//
// Seq is the global position assigned by the sequencer at admission time and is
// the only ordering that components are allowed to depend on.
//
// SenderComponent identifies the logical component (e.g. "cooperator"), while
// SenderId identifies which replica of that component produced the event. The
// sequencer deduplicates on SenderComponent + SenderSeq, which is what makes the
// active-active replica race resolve to first-response-wins.
type Header struct {
	Seq             int64
	SenderComponent string
	SenderId        string
	SenderSeq       int64
}

type Event struct {
	Header  Header
	Payload any // immutable and opaque
	// origin is set only for events emitted by a SequencerClient. It lets the
	// sequencer discard work from a replica that died before its queued event
	// reached the log. Raw events used by low-level tests have no origin.
	origin *SequencerClient
}

// ReplayComplete is delivered in-band on a client's egress stream once the
// sequencer has replayed the whole existing log to it.
//
// It has to be in-band rather than a side-channel flag: the component is still
// draining replayed events on its own goroutine, so flipping a shared "you are
// live now" bit from the sequencer goroutine would let the component start
// emitting events derived from partially-replayed state.
type ReplayComplete struct{}

// IsReplayComplete reports whether the event is the in-band activation marker
// rather than a real sequenced event. Markers carry no sequence number and are
// never written to the log.
func (e *Event) IsReplayComplete() bool {
	_, ok := e.Payload.(ReplayComplete)
	return ok
}
