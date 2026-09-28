package platform

import (
	"context"
	"sync"
	"time"
)

// Pacer throttles how fast the sequencer admits events, and can hold it
// stopped entirely.
//
// This is a presentation affordance, not a mechanism: it changes *when* an event
// is admitted, never which event or in what order. That is safe here because v1
// is strictly turn-taking -- at any moment exactly one component has something to
// send -- so arrival order at the sequencer is forced by the shape of the
// tournament rather than by timing, and the log carries the same events in the
// same order at any speed. Which replica of a component won each position still
// varies, as it always does -- that race is not affected by pacing either.
// It would stop being safe the moment two unrelated components could be waiting
// to send at once, which is exactly what v2 introduces; the v2 injector has to
// fix the logical order at admission for that reason anyway.
//
// A Pacer is safe for concurrent use: the UI drives it from a different goroutine
// than the sequencer it gates.
type Pacer struct {
	mu       sync.Mutex
	interval time.Duration
	running  bool
	// credits are one-shot admissions handed out by Step, consumed even while
	// paused. A flag would not do: Step must release exactly one event, and the
	// waiter cannot observe a flag that is set and cleared under one lock hold.
	credits int
	resumed chan struct{}
	// nextBeat is an absolute schedule, not a delay measured after the previous
	// fanout. Advancing it by one interval per admission prevents delivery work
	// from accumulating into musical drift.
	nextBeat       time.Time
	waiting        bool
	waitGeneration uint64
}

// NewPacer returns a pacer admitting one event every interval. An interval of 0
// runs flat out, which is what headless runs and tests want.
func NewPacer(interval time.Duration) *Pacer {
	return &Pacer{interval: interval, running: true, resumed: make(chan struct{})}
}

// Wait blocks until the next event may be admitted, or until ctx is cancelled.
// It reports false if the wait was cut short by cancellation.
func (p *Pacer) Wait(ctx context.Context) bool {
	for {
		p.mu.Lock()
		if p.credits > 0 {
			p.credits--
			p.mu.Unlock()
			return ctx.Err() == nil
		}
		running, interval, resumed := p.running, p.interval, p.resumed

		if !running {
			p.mu.Unlock()
			// Paused: wait to be resumed, then re-read the interval, which may
			// have been changed while stopped.
			select {
			case <-ctx.Done():
				return false
			case <-resumed:
				continue
			}
		}
		if interval <= 0 {
			p.mu.Unlock()
			return ctx.Err() == nil
		}

		if p.nextBeat.IsZero() {
			p.nextBeat = time.Now().Add(interval)
		}
		delay := time.Until(p.nextBeat)
		if delay < -interval {
			// Nothing was waiting while the tournament was stalled. Missed beats
			// are not deferred actions, so skip to the next beat on the original
			// grid rather than draining a whole game at once or starting a new
			// rhythm out of phase with the soundtrack.
			missed := time.Since(p.nextBeat)/interval + 1
			p.nextBeat = p.nextBeat.Add(missed * interval)
			delay = time.Until(p.nextBeat)
		} else if delay < 0 {
			delay = 0
		}
		generation := p.waitGeneration
		p.waiting = true
		p.mu.Unlock()

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
			p.mu.Lock()
			admit := p.running && p.waiting && p.waitGeneration == generation
			if admit {
				p.waiting = false
				p.nextBeat = p.nextBeat.Add(interval)
			}
			p.mu.Unlock()
			if admit {
				return true
			}
		case <-resumed:
			// The speed changed mid-wait; start again at the new rate.
			timer.Stop()
		}
	}
}

// Pause holds admission. Events already emitted stay queued; nothing is lost.
func (p *Pacer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
	p.wakeLocked()
}

// Resume releases admission.
func (p *Pacer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return
	}
	// The soundtrack continues during a user pause, so resume at the next beat
	// on its existing grid rather than preserving time from a paused clock.
	p.alignNextBeatLocked(time.Now())
	p.running = true
	p.wakeLocked()
}

// Begin releases the first event immediately, then continues at the configured
// beat interval. It is for a newly created paused stream: its first visible
// action should land on the soundtrack's first beat, not one beat later.
func (p *Pacer) Begin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return
	}
	p.credits++
	p.running = true
	p.nextBeat = time.Now().Add(p.interval)
	p.wakeLocked()
}

// SetInterval changes the time between admissions, taking effect immediately
// even if a wait is already in progress.
func (p *Pacer) SetInterval(interval time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.interval = interval
	// A changed tempo starts a fresh beat immediately.
	p.nextBeat = time.Time{}
	p.waiting = false
	p.wakeLocked()
}

// Step admits exactly one event and leaves admission paused. The configured
// interval is untouched, so resuming afterwards runs at the speed it was set to.
func (p *Pacer) Step() {
	p.mu.Lock()
	defer p.mu.Unlock()
	// A click may race the sequencer consuming the previous click. Keep at most
	// one permission outstanding, so repeated clicks cannot bank a run of future
	// admissions while the same event is still waiting.
	if p.credits == 0 {
		p.credits = 1
	}
	p.running = false
	p.wakeLocked()
}

// State reports whether admission is running, and at what interval.
func (p *Pacer) State() (running bool, interval time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running, p.interval
}

// wakeLocked releases everyone waiting on the current generation and starts a
// new one. Closing a channel rather than signalling means a waiter cannot miss
// the wake-up between releasing the lock and selecting on it.
func (p *Pacer) wakeLocked() {
	p.waitGeneration++
	close(p.resumed)
	p.resumed = make(chan struct{})
}

// alignNextBeatLocked skips missed beats and leaves nextBeat at the first one
// strictly after now. The caller holds p.mu.
func (p *Pacer) alignNextBeatLocked(now time.Time) {
	if p.nextBeat.IsZero() || p.interval <= 0 || p.nextBeat.After(now) {
		return
	}
	missed := now.Sub(p.nextBeat)/p.interval + 1
	p.nextBeat = p.nextBeat.Add(missed * p.interval)
}
