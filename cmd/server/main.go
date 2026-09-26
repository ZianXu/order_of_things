// Command server serves the tournament to a browser.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"order_of_things/internal/golden"
	"order_of_things/internal/session"
	"order_of_things/internal/web"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "address to listen on")
		goldPath = flag.String("golden", "testdata/golden.json", "golden outcome store; empty keeps references in memory only")
		games    = flag.Int("games", session.DefaultGames, "games per session")
		tempo    = flag.Float64("tempo", session.DefaultTempo, "soundtrack tempo in beats per minute")
		idle     = flag.Duration("idle", 2*time.Minute, "stop sessions with no viewer for this long")
		maxSess  = flag.Int("max-sessions", session.MaxSessions, "cap on concurrent sessions; 0 for no cap")
		maxAge   = flag.Duration("max-age", time.Hour, "stop sessions older than this")
		quiet    = flag.Bool("quiet", false, "suppress the platform's own logging")
	)
	flag.Parse()
	if *tempo <= 0 {
		fmt.Fprintln(os.Stderr, "tempo must be greater than zero")
		os.Exit(2)
	}

	if *quiet {
		log.SetOutput(io.Discard)
	}

	store, err := golden.Open(*goldPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	registry := session.NewRegistry(store)
	registry.SetGames(*games)
	registry.SetTempo(*tempo)
	registry.SetMaxSessions(*maxSess)
	defer registry.StopAll()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Generate the canonical state-checksum chain before serving, so a replica can be
	// checked against it from the first event of the first session.
	if err := registry.Prepare(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// A viewer who closes the tab leaves a session running at one event per
	// beat forever, so idle ones are reclaimed on a timer.
	go reclaim(ctx, registry, *idle, *maxAge)

	server := &http.Server{
		Addr:    *addr,
		Handler: web.NewServer(registry),
		// A whole request, headers and body, has to arrive promptly. Every request
		// this server accepts is tiny, so a slow one is either broken or hostile.
		// WriteTimeout stays unset on purpose: the event stream is a long-lived
		// response and any write deadline would cut it off.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 14,
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()

	fmt.Fprintf(os.Stderr, "the order of things: http://localhost%s  (%d games, %.0f BPM)\n",
		*addr, *games, *tempo)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func reclaim(ctx context.Context, registry *session.Registry, idle, maxAge time.Duration) {
	// Often enough that an abandoned session is not held for much longer than the
	// idle window it has already outlived.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if stopped := registry.Evict(idle, maxAge); len(stopped) > 0 {
				log.Printf("reclaimed idle sessions: %v", stopped)
			}
		}
	}
}
