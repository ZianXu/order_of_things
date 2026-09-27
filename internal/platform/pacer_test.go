package platform

import (
	"context"
	"testing"
	"time"
)

func TestPacerRunsFlatOutAtZeroInterval(t *testing.T) {
	pacer := NewPacer(0)
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if !pacer.Wait(ctx) {
			t.Fatalf("Wait returned false at %d", i)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("1000 unpaced admissions took %v", elapsed)
	}
}

func TestPacerThrottles(t *testing.T) {
	pacer := NewPacer(20 * time.Millisecond)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 3; i++ {
		if !pacer.Wait(ctx) {
			t.Fatalf("Wait returned false at %d", i)
		}
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("3 admissions at 20ms took only %v", elapsed)
	}
}

func TestPacerUsesAnAbsoluteBeatGrid(t *testing.T) {
	pacer := NewPacer(200 * time.Millisecond)
	ctx := context.Background()
	if !pacer.Wait(ctx) {
		t.Fatal("first beat was cancelled")
	}
	first := time.Now()

	// This is the time the sequencer spends delivering the first event. The next
	// beat must remain scheduled from the first beat, rather than from this work.
	time.Sleep(100 * time.Millisecond)
	if !pacer.Wait(ctx) {
		t.Fatal("second beat was cancelled")
	}
	if elapsed := time.Since(first); elapsed >= 260*time.Millisecond {
		t.Errorf("second beat arrived after %v; want an absolute 200ms beat grid", elapsed)
	}
}

func TestPacerRejoinsTheBeatGridAfterAnIdleGap(t *testing.T) {
	const interval = 20 * time.Millisecond
	pacer := NewPacer(interval)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if !pacer.Wait(ctx) {
		t.Fatal("first beat was cancelled")
	}
	// Model a stalled tournament: no event asks the pacer to wait for several
	// beats. Recovery waits for the next beat on the existing grid instead of
	// catching up in a burst or creating a new, out-of-phase grid.
	time.Sleep(5*interval + interval/2)
	started := time.Now()
	if !pacer.Wait(ctx) {
		t.Fatal("recovery beat was cancelled")
	}
	if elapsed := time.Since(started); elapsed < interval/5 {
		t.Fatalf("recovery beat arrived after %v, want the next grid beat", elapsed)
	}
	started = time.Now()
	if !pacer.Wait(ctx) {
		t.Fatal("post-recovery beat was cancelled")
	}
	if elapsed := time.Since(started); elapsed < interval/2 {
		t.Fatalf("post-recovery beat arrived after %v, want roughly one interval", elapsed)
	}
}

func TestPacerPauseBlocksUntilResume(t *testing.T) {
	pacer := NewPacer(0)
	ctx := context.Background()
	pacer.Pause()

	admitted := make(chan bool, 1)
	go func() { admitted <- pacer.Wait(ctx) }()

	select {
	case <-admitted:
		t.Fatal("Wait returned while paused")
	case <-time.After(50 * time.Millisecond):
	}

	pacer.Resume()
	select {
	case ok := <-admitted:
		if !ok {
			t.Error("Wait returned false after Resume")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after Resume")
	}
}

func TestPacerBeginReleasesTheFirstBeatImmediately(t *testing.T) {
	pacer := NewPacer(time.Hour)
	pacer.Pause()
	admitted := make(chan bool, 1)
	go func() { admitted <- pacer.Wait(context.Background()) }()

	pacer.Begin()
	select {
	case ok := <-admitted:
		if !ok {
			t.Error("Wait returned false after Begin")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Begin did not release the first beat immediately")
	}

	if running, interval := pacer.State(); !running || interval != time.Hour {
		t.Errorf("after Begin: running=%v interval=%v, want running at 1h", running, interval)
	}
}

func TestPacerResumeKeepsTheRemainderOfTheBeat(t *testing.T) {
	pacer := NewPacer(300 * time.Millisecond)
	ctx := context.Background()
	admitted := make(chan bool, 1)
	go func() { admitted <- pacer.Wait(ctx) }()

	// Let part of the beat pass, then freeze it. The admission must not slip
	// through while paused, and it must not restart a full beat on resume.
	time.Sleep(100 * time.Millisecond)
	pacer.Pause()
	select {
	case <-admitted:
		t.Fatal("Wait returned after pause")
	case <-time.After(250 * time.Millisecond):
	}

	resumed := time.Now()
	pacer.Resume()
	select {
	case ok := <-admitted:
		if !ok {
			t.Error("Wait returned false after Resume")
		}
		if elapsed := time.Since(resumed); elapsed >= 260*time.Millisecond {
			t.Errorf("resume waited %v, want the remaining beat rather than a fresh 300ms beat", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait did not return after Resume")
	}
}

func TestPacerStepAdmitsExactlyOne(t *testing.T) {
	pacer := NewPacer(time.Hour) // slow enough that only Step can release anything
	ctx := context.Background()
	pacer.Pause()

	admitted := make(chan bool, 4)
	for i := 0; i < 2; i++ {
		go func() { admitted <- pacer.Wait(ctx) }()
	}

	pacer.Step()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("Step did not admit an event")
	}
	select {
	case <-admitted:
		t.Fatal("Step admitted more than one event")
	case <-time.After(50 * time.Millisecond):
	}

	// Step must not have disturbed the configured speed.
	if running, interval := pacer.State(); running || interval != time.Hour {
		t.Errorf("after Step: running=%v interval=%v, want paused at 1h", running, interval)
	}
	pacer.Step()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("a second Step did not admit an event")
	}
}

func TestPacerSpeedChangeInterruptsAWaitInProgress(t *testing.T) {
	pacer := NewPacer(time.Hour)
	ctx := context.Background()

	admitted := make(chan bool, 1)
	go func() { admitted <- pacer.Wait(ctx) }()

	// Give the waiter time to enter its hour-long timer, then speed it up.
	time.Sleep(20 * time.Millisecond)
	pacer.SetInterval(time.Millisecond)

	select {
	case ok := <-admitted:
		if !ok {
			t.Error("Wait returned false after a speed change")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a speed change did not interrupt the wait in progress")
	}
}

func TestPacerWaitReportsCancellation(t *testing.T) {
	pacer := NewPacer(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())

	admitted := make(chan bool, 1)
	go func() { admitted <- pacer.Wait(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case ok := <-admitted:
		if ok {
			t.Error("Wait returned true after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancellation")
	}

	// And it must not block once cancelled, paused or not.
	pacer.Pause()
	if pacer.Wait(ctx) {
		t.Error("Wait returned true on a cancelled context")
	}
}

// Pacing changes when events are admitted, never which or in what order. Two
// runs of the same seed at different speeds must produce the same log.
func TestPacingDoesNotChangeTheLog(t *testing.T) {
	fast := NewPacer(0)
	slow := NewPacer(2 * time.Millisecond)

	logs := make([][]string, 0, 2)
	for _, pacer := range []*Pacer{fast, slow} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		s := NewSequencer()
		s.SetPacer(pacer)
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx)
		}()

		producer := NewSequencerClient("producer", "p1", s)
		join(t, ctx, s, producer, 0)
		for i := 0; i < 25; i++ {
			producer.Send(i)
			mustRead(t, ctx, producer)
		}
		cancel()
		<-done

		entries := make([]string, 0, 25)
		for _, e := range s.EventLog() {
			entries = append(entries, string(rune('a'+e.Header.Seq))+":"+string(rune('0'+e.Payload.(int)%10)))
		}
		logs = append(logs, entries)
	}
	if len(logs[0]) != 25 || len(logs[1]) != 25 {
		t.Fatalf("logs have %d and %d events, want 25 each", len(logs[0]), len(logs[1]))
	}
	for i := range logs[0] {
		if logs[0][i] != logs[1][i] {
			t.Fatalf("position %d: unpaced %q, paced %q", i, logs[0][i], logs[1][i])
		}
	}
}
