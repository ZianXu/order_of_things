// Package web serves the tournament to a browser.
//
// Everyone plays the same tournament: the seed is fixed and is not exposed as a
// control. Reproducing a seeded sequence is a property of the generator, not of
// this system, and offering it as a knob would point the reader at the wrong
// claim. The claim is that the outcome survives what the viewer does to the
// system while it runs.
//
// The one thing this layer will not do is hand the browser a rendered view of
// the world and let it poll for a new one. Everything the page draws, it draws by
// folding an ordered feed of events, exactly as every component inside the system
// does. That makes the browser one more replica of the same state machine rather
// than a dashboard bolted onto it, which is the point the whole project is
// making. Anything that cannot be expressed as an event in that feed -- which
// replica is alive, whether the tournament is stalled -- is sent as a separate,
// clearly-labelled kind of frame, so the distinction stays visible instead of
// being quietly blurred.
package web

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"order_of_things/internal/session"
	"order_of_things/internal/tracker"
)

//go:embed static
var assets embed.FS

// pollInterval is how often the stream checks the tracker for new events. A
// frame-length interval keeps a beat-synced visual cue within one display frame
// of its admission, rather than letting transport timing rotate its phase across
// the soundtrack. What reaches the browser is still events in order, never a
// snapshot of aggregate state.
const pollInterval = 16 * time.Millisecond

// maxBody bounds a control request. Every request this server accepts is a few
// dozen bytes of JSON; without a bound a slow or oversized body can hold a
// handler goroutine open inside the decoder for as long as the sender likes.
const maxBody = 4 << 10

// Server exposes a session registry over HTTP.
type Server struct {
	registry *session.Registry
	mux      *http.ServeMux
}

func NewServer(registry *session.Registry) *Server {
	s := &Server{registry: registry, mux: http.NewServeMux()}

	static, err := fs.Sub(assets, "static")
	if err != nil {
		panic("web: embedded assets missing: " + err.Error())
	}
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	s.mux.HandleFunc("GET /", s.index(static))

	s.mux.HandleFunc("POST /api/sessions", s.createSession)
	s.mux.HandleFunc("GET /api/sessions/{id}/stream", s.stream)
	s.mux.HandleFunc("POST /api/sessions/{id}/control", s.control)
	s.mux.HandleFunc("POST /api/sessions/{id}/replicas/{component}/{replica}", s.replicaAction)
	s.mux.HandleFunc("DELETE /api/sessions/{id}", s.deleteSession)

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) index(static fs.FS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFileFS(w, r, static, "index.html")
	}
}

type sessionResponse struct {
	Id    string `json:"id"`
	Seed  int64  `json:"seed"`
	Games int    `json:"games"`
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	// No seed: every session is the canonical tournament.
	handle, err := s.registry.Create(r.Context(), 0)
	if errors.Is(err, session.ErrTooManySessions) {
		// Full, not broken. Say so with a status a client can act on, and one that
		// a load balancer will not mistake for a bug in the server.
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many sessions in progress, try again shortly", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, sessionResponse{
		Id: handle.Id, Seed: handle.Seed, Games: handle.Games,
	})
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	if err := s.registry.Stop(r.PathValue("id")); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type controlRequest struct {
	Action string `json:"action"`
}

func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	handle, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "no such session", http.StatusNotFound)
		return
	}
	var req controlRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	switch req.Action {
	case "start":
		handle.Session.Begin()
	case "resume":
		handle.Session.Resume()
	case "pause":
		handle.Session.Pause()
	case "step":
		handle.Session.Step()
	default:
		http.Error(w, fmt.Sprintf("unknown action %q", req.Action), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type replicaRequest struct {
	Action string `json:"action"`
	// Defect names which bug to inject on a buggy restart. The two are caught by
	// different mechanisms, so which one is chosen changes what the demo shows.
	Defect string `json:"defect"`
}

func (s *Server) replicaAction(w http.ResponseWriter, r *http.Request) {
	handle, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "no such session", http.StatusNotFound)
		return
	}
	var req replicaRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	component, replica := r.PathValue("component"), r.PathValue("replica")

	var err error
	switch req.Action {
	case "kill":
		err = handle.Session.Kill(component, replica)
	case "restart":
		err = handle.Session.Restart(component, replica)
	case "restart-with-bug":
		defect, defectErr := parseDefect(req.Defect)
		if defectErr != nil {
			http.Error(w, defectErr.Error(), http.StatusBadRequest)
			return
		}
		err = handle.Session.RestartWithBug(component, replica, defect)
	default:
		http.Error(w, fmt.Sprintf("unknown action %q", req.Action), http.StatusBadRequest)
		return
	}
	if err != nil {
		// A refused action is expected, not exceptional -- killing something that
		// is already stopped, restarting something already running -- and the UI
		// says so rather than pretending it worked.
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseDefect(name string) (session.Defect, error) {
	switch name {
	case "", "decision":
		// Deterministically wrong, so it is refused every time. An intermittent
		// defect can pass a check that runs once, which makes for a demonstration
		// that works most of the time -- the worst kind.
		return session.Defect{WrongDecision: true}, nil
	case "clock":
		return session.Defect{ImpureClock: true}, nil
	case "payoff":
		return session.Defect{CorruptPayoff: true}, nil
	default:
		return session.Defect{}, fmt.Errorf("unknown defect %q: want decision, clock or payoff", name)
	}
}

// replicaState is a supervisor fact, not a logical one, so it travels as its own
// kind of frame rather than being mixed into the event feed.
type replicaState struct {
	Component   string `json:"component"`
	Replica     string `json:"replica"`
	Running     bool   `json:"running"`
	Killed      bool   `json:"killed"`
	Quarantined bool   `json:"quarantined"`
	Wins        int    `json:"wins"`
}

type statusFrame struct {
	Replicas  []replicaState `json:"replicas"`
	Stalled   bool           `json:"stalled"`
	WaitingOn string         `json:"waitingOn"`
	Completed int            `json:"completed"`
	Games     int            `json:"games"`
	Running   bool           `json:"running"`
	Seed      int64          `json:"seed"`
	Done      bool           `json:"done"`
	StateHash string         `json:"stateHash"`
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	handle, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "no such session", http.StatusNotFound)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	from := 0
	if raw := r.URL.Query().Get("from"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			from = parsed
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies that buffer would defeat the whole point of streaming.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastStatus string
	for {
		// An open stream is a viewer. Without this a session is only ever touched
		// when a request arrives, so someone watching -- or paused part way through
		// explaining it -- would be reclaimed out from under themselves.
		if !s.registry.Touch(handle.Id) {
			return // reclaimed or stopped while we were streaming
		}
		snapshot := handle.Session.Tracker().Snapshot()

		// Events first, so the page never learns a game is over before it has
		// been told how it went.
		for _, event := range eventsFrom(snapshot.Feed, from) {
			if err := writeSSE(w, "feed", event); err != nil {
				return
			}
			from = event.Index + 1
		}

		status := s.status(handle, snapshot)
		if encoded, err := json.Marshal(status); err == nil && string(encoded) != lastStatus {
			lastStatus = string(encoded)
			if err := writeRawSSE(w, "status", encoded); err != nil {
				return
			}
		}
		flusher.Flush()

		// A finished tournament has nothing more to send, and holding the stream
		// open would keep touching the session for as long as the tab exists --
		// pinning a session that has already stopped everything it started. The
		// client has folded the whole feed by now and needs nothing further.
		if status.Done {
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func eventsFrom(feed []tracker.FeedEvent, from int) []tracker.FeedEvent {
	if from >= len(feed) {
		return nil
	}
	if from < 0 {
		from = 0
	}
	return feed[from:]
}

func (s *Server) status(handle *session.Handle, snapshot tracker.Snapshot) statusFrame {
	stalled, waitingOn := handle.Session.Stalled()
	running, _ := handle.Session.Pacing()

	replicas := make([]replicaState, 0)
	for _, st := range handle.Session.Status() {
		replicas = append(replicas, replicaState{
			Component:   st.Component,
			Replica:     st.Replica,
			Running:     st.Running,
			Killed:      st.Killed,
			Quarantined: st.Quarantined,
			Wins:        st.Wins,
		})
	}
	return statusFrame{
		Replicas:  replicas,
		Stalled:   stalled,
		WaitingOn: waitingOn,
		Completed: snapshot.Completed,
		Games:     handle.Games,
		Running:   running,
		Seed:      handle.Seed,
		Done:      snapshot.Completed >= handle.Games,
		StateHash: fmt.Sprintf("%016x", snapshot.StateHash),
	}
}

func writeSSE(w http.ResponseWriter, event string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return writeRawSSE(w, event, encoded)
}

func writeRawSSE(w http.ResponseWriter, event string, encoded []byte) error {
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, encoded)
	return err
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
