package web

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"order_of_things/internal/golden"
	"order_of_things/internal/session"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store, err := golden.Open("")
	if err != nil {
		t.Fatalf("golden: %v", err)
	}
	registry := session.NewRegistry(store)
	registry.SetGames(6)
	registry.SetInterval(0) // tests run flat out
	t.Cleanup(registry.StopAll)

	server := httptest.NewServer(NewServer(registry))
	t.Cleanup(server.Close)
	return server
}

func createSession(t *testing.T, server *httptest.Server) sessionResponse {
	t.Helper()
	res, err := server.Client().Post(server.URL+"/api/sessions", "application/json", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create returned %d", res.StatusCode)
	}
	var created sessionResponse
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	return created
}

func post(t *testing.T, server *httptest.Server, path, body string) *http.Response {
	t.Helper()
	res, err := server.Client().Post(server.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	return res
}

type frame struct {
	event string
	data  []byte
}

// readFrames consumes the SSE stream until stop says so, or the deadline passes.
func readFrames(t *testing.T, server *httptest.Server, path string, stop func([]frame) bool) []frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("stream returned %d", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("stream content type %q", got)
	}

	frames := make([]frame, 0)
	scanner := bufio.NewScanner(res.Body)
	var pending frame
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			pending.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			pending.data = []byte(strings.TrimPrefix(line, "data: "))
		case line == "" && pending.event != "":
			frames = append(frames, pending)
			pending = frame{}
			if stop(frames) {
				return frames
			}
		}
	}
	t.Fatalf("stream ended after %d frames without the expected content", len(frames))
	return nil
}

func feedEvents(frames []frame) []map[string]any {
	events := make([]map[string]any, 0)
	for _, f := range frames {
		if f.event != "feed" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(f.data, &event); err == nil {
			events = append(events, event)
		}
	}
	return events
}

func lastStatus(frames []frame) map[string]any {
	var status map[string]any
	for _, f := range frames {
		if f.event != "status" {
			continue
		}
		_ = json.Unmarshal(f.data, &status)
	}
	return status
}

func TestIndexIsServed(t *testing.T) {
	server := newTestServer(t)
	res, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET / returned %d", res.StatusCode)
	}
	// Credit is on the welcome page, which is the first and sometimes only thing
	// a visitor reads, not tucked into a footer they reach by playing.
	for _, want := range []string{
		"The Order of Things", "/static/app.js",
		"ncase.me/trust", "The Evolution of Trust",
		"suno.com/s/yjNvZutbRTCvK0ko",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("index does not mention %q", want)
		}
	}
	// And it must appear before the Play button, not only in the game view.
	welcome := string(body)[:strings.Index(string(body), `<main class="game">`)]
	if !strings.Contains(welcome, "ncase.me/trust") {
		t.Error("the Nicky Case credit is not on the welcome page")
	}
}

func TestStaticAssetsAreServed(t *testing.T) {
	server := newTestServer(t)
	for _, path := range []string{"/static/app.js", "/static/style.css", "/static/characters.js"} {
		res, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s returned %d", path, res.StatusCode)
		}
	}
	res, err := server.Client().Get(server.URL + "/nope")
	if err != nil {
		t.Fatalf("get /nope: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("GET /nope returned %d, want 404", res.StatusCode)
	}
}

// Every session is the same tournament. The seed is not a control: reproducing a
// seeded sequence is a property of the generator, not of this system, and the
// claim being made is about surviving what the viewer does to a running system.
func TestEverySessionPlaysTheCanonicalTournament(t *testing.T) {
	server := newTestServer(t)

	first := createSession(t, server)
	second := createSession(t, server)

	if first.Seed != session.CanonicalSeed || second.Seed != session.CanonicalSeed {
		t.Errorf("seeds were %d and %d, want the canonical %d",
			first.Seed, second.Seed, session.CanonicalSeed)
	}
	if first.Id == second.Id {
		t.Error("two sessions were given the same id")
	}
	if first.Games != 6 {
		t.Errorf("games = %d, want 6", first.Games)
	}

	// A seed in the request body is not a control and must not become one.
	res, err := server.Client().Post(server.URL+"/api/sessions", "application/json",
		strings.NewReader(`{"seed":999}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer res.Body.Close()
	var third sessionResponse
	if err := json.NewDecoder(res.Body).Decode(&third); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if third.Seed != session.CanonicalSeed {
		t.Errorf("a seed in the body was honoured: got %d, want %d", third.Seed, session.CanonicalSeed)
	}
}

// The whole page is built by folding this feed, so it has to arrive in order,
// with contiguous indices, and carry everything needed to draw a game through.
func TestStreamDeliversTheFeedInOrder(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"start"}`).Body.Close()

	frames := readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		status := lastStatus(f)
		return status != nil && status["done"] == true
	})

	events := feedEvents(frames)
	// Six games: a new-game, two decisions, and a separately paced completion each.
	if want := 6 * 4; len(events) != want {
		t.Fatalf("feed had %d events, want %d", len(events), want)
	}
	for i, event := range events {
		if int(event["index"].(float64)) != i {
			t.Fatalf("event %d has index %v; the fold depends on contiguity", i, event["index"])
		}
	}
	for game := 0; game < 6; game++ {
		kinds := make([]string, 0, 4)
		for _, event := range events[game*4 : game*4+4] {
			kinds = append(kinds, event["kind"].(string))
		}
		want := []string{"new-game", "decision", "decision", "game-completed"}
		for i := range want {
			if kinds[i] != want[i] {
				t.Fatalf("game %d event %d was %q, want %q", game, i, kinds[i], want[i])
			}
		}
	}

	// A new-game names its participants; a completion carries the scores.
	first := events[0]
	if first["strategyA"] == nil || first["strategyB"] == nil {
		t.Errorf("new-game did not name both participants: %v", first)
	}
	completion := events[3]
	if completion["leaderboard"] == nil {
		t.Errorf("completion carried no leaderboard: %v", completion)
	}
}

// A client that reconnects resumes from where its fold got to, exactly as a
// restarting replica resumes from the log.
func TestStreamResumesFromAnIndex(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"start"}`).Body.Close()

	full := feedEvents(readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		status := lastStatus(f)
		return status != nil && status["done"] == true
	}))

	resumed := feedEvents(readFrames(t, server, "/api/sessions/"+created.Id+"/stream?from=8", func(f []frame) bool {
		return len(feedEvents(f)) >= len(full)-8
	}))
	if len(resumed) < len(full)-8 {
		t.Fatalf("resumed stream gave %d events, want %d", len(resumed), len(full)-8)
	}
	if int(resumed[0]["index"].(float64)) != 8 {
		t.Errorf("resumed stream started at index %v, want 8", resumed[0]["index"])
	}
}

// The deployment facts the page needs -- which replicas are alive -- are a
// separate kind of frame, never folded into the feed.
func TestStatusFrameCarriesEveryReplica(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	frames := readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		return lastStatus(f) != nil
	})
	status := lastStatus(frames)

	replicas, ok := status["replicas"].([]any)
	if !ok {
		t.Fatalf("status carried no replicas: %v", status)
	}
	// Four strategies and an injector, two replicas each, plus one tracker.
	if want := 4*2 + 2 + 1; len(replicas) != want {
		t.Errorf("status listed %d replicas, want %d", len(replicas), want)
	}
	if int64(status["seed"].(float64)) != session.CanonicalSeed {
		t.Errorf("status seed = %v, want the canonical %d", status["seed"], session.CanonicalSeed)
	}
	if status["games"].(float64) != 6 {
		t.Errorf("status games = %v, want 6", status["games"])
	}
	if _, ok := status["stateHash"].(string); !ok {
		t.Errorf("status carried no state checksum: %v", status)
	}
}

// Restart and the bugged restart both act on a live replica directly: the
// operator should not have to kill it first to redeploy it.
func TestRestartActsOnALiveReplica(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	path := "/api/sessions/" + created.Id + "/replicas/flipper/r1"

	for _, action := range []string{`{"action":"restart"}`, `{"action":"restart-with-bug","defect":"decision"}`} {
		res := post(t, server, path, action)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			t.Errorf("%s on a live replica returned %d: %s", action, res.StatusCode, body)
		}
	}
}

func TestKillAndRestartAReplica(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	path := "/api/sessions/" + created.Id + "/replicas/flipper/r1"

	res := post(t, server, path, `{"action":"kill"}`)
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("kill returned %d", res.StatusCode)
	}

	frames := readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		status := lastStatus(f)
		if status == nil {
			return false
		}
		for _, entry := range status["replicas"].([]any) {
			replica := entry.(map[string]any)
			if replica["component"] == "flipper" && replica["replica"] == "r1" {
				return replica["killed"] == true && replica["running"] == false
			}
		}
		return false
	})
	if lastStatus(frames) == nil {
		t.Fatal("no status frame reported the kill")
	}

	res = post(t, server, path, `{"action":"restart"}`)
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("restart returned %d", res.StatusCode)
	}
}

// A refused action is expected, not exceptional: the page says so rather than
// pretending it worked.
func TestRefusedActionsReportWhy(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	// Killing something that is already stopped is refused, and says why.
	post(t, server, "/api/sessions/"+created.Id+"/replicas/flipper/r0", `{"action":"kill"}`).Body.Close()

	res := post(t, server, "/api/sessions/"+created.Id+"/replicas/flipper/r0", `{"action":"kill"}`)
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("killing a stopped replica returned %d, want 409", res.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if !strings.Contains(body["error"], "not running") {
		t.Errorf("error was %q", body["error"])
	}
}

// Once the tournament is over there is nothing to inject a fault into, and a
// control that silently did nothing would be worse than one that says so.
func TestControlsAreRefusedOnceFinished(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"start"}`).Body.Close()

	readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		status := lastStatus(f)
		return status != nil && status["done"] == true
	})

	for _, action := range []string{`{"action":"kill"}`, `{"action":"restart"}`} {
		res := post(t, server, "/api/sessions/"+created.Id+"/replicas/flipper/r0", action)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusConflict {
			t.Errorf("%s returned %d, want 409", action, res.StatusCode)
		}
		if !strings.Contains(string(body), "finished") {
			t.Errorf("%s said %q", action, body)
		}
	}
}

func TestBadRequestsAreRejected(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	tests := []struct {
		name string
		path string
		body string
		want int
	}{
		{"unknown control", "/api/sessions/" + created.Id + "/control", `{"action":"levitate"}`, http.StatusBadRequest},
		{"unknown replica action", "/api/sessions/" + created.Id + "/replicas/flipper/r0", `{"action":"levitate"}`, http.StatusBadRequest},
		{"unknown defect", "/api/sessions/" + created.Id + "/replicas/flipper/r0", `{"action":"restart-with-bug","defect":"nonsense"}`, http.StatusBadRequest},
		{"clock defect still available", "/api/sessions/" + created.Id + "/replicas/flipper/r0", `{"action":"restart-with-bug","defect":"clock"}`, http.StatusNoContent},
		{"malformed body", "/api/sessions/" + created.Id + "/control", `{`, http.StatusBadRequest},
		{"unknown session", "/api/sessions/nope/control", `{"action":"pause"}`, http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := post(t, server, tc.path, tc.body)
			res.Body.Close()
			if res.StatusCode != tc.want {
				t.Errorf("got %d, want %d", res.StatusCode, tc.want)
			}
		})
	}

	res, err := server.Client().Get(server.URL + "/api/sessions/nope/stream")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("streaming an unknown session returned %d, want 404", res.StatusCode)
	}
}

func TestPauseHoldsTheStream(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"pause"}`).Body.Close()

	frames := readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		return lastStatus(f) != nil
	})
	if lastStatus(frames)["running"] != false {
		t.Error("status reported running while paused")
	}

	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"resume"}`).Body.Close()
	frames = readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		status := lastStatus(f)
		return status != nil && status["running"] == true
	})
	if lastStatus(frames)["running"] != true {
		t.Error("status did not report running after resume")
	}
}

func TestDeleteSession(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/sessions/"+created.Id, nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("delete returned %d", res.StatusCode)
	}

	res, err = server.Client().Do(req)
	if err != nil {
		t.Fatalf("delete again: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("deleting twice returned %d, want 404", res.StatusCode)
	}
}

// The music is optional: the page has to work whether or not the audio file has
// been dropped into the static directory, so a missing one must 404 cleanly
// rather than breaking the build or the page.
func TestPageWorksWithoutTheAudioFile(t *testing.T) {
	server := newTestServer(t)

	res, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET / returned %d", res.StatusCode)
	}
	for _, want := range []string{`id="theme"`, `id="mute"`, "/static/background.m4a"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("index does not wire up %q", want)
		}
	}

	// Present or absent, the page is unaffected; only the button's visibility is.
	res, err = server.Client().Get(server.URL + "/static/background.m4a")
	if err != nil {
		t.Fatalf("get the music: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound {
		t.Errorf("GET /static/background.m4a returned %d, want 200 or 404", res.StatusCode)
	}
}

// A full registry is a capacity answer, not a fault. It has to come back as
// something a client can act on and a load balancer will not read as a broken
// server.
func TestCreateIsRefusedWhenFull(t *testing.T) {
	store, err := golden.Open("")
	if err != nil {
		t.Fatalf("golden: %v", err)
	}
	registry := session.NewRegistry(store)
	registry.SetGames(6)
	registry.SetInterval(0)
	registry.SetMaxSessions(2)
	t.Cleanup(registry.StopAll)

	server := httptest.NewServer(NewServer(registry))
	t.Cleanup(server.Close)

	for i := 0; i < 2; i++ {
		createSession(t, server)
	}

	res, err := server.Client().Post(server.URL+"/api/sessions", "application/json", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("creating past the cap returned %d, want 503", res.StatusCode)
	}
	if res.Header.Get("Retry-After") == "" {
		t.Error("a 503 with no Retry-After leaves the client guessing")
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "too many sessions") {
		t.Errorf("body was %q", body)
	}
}

// An open stream counts as activity, so a viewer is not reclaimed mid-watch.
func TestStreamingKeepsTheSessionAlive(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	// Reading the stream at all must refresh the session's last-used time.
	readFrames(t, server, "/api/sessions/"+created.Id+"/stream", func(f []frame) bool {
		return lastStatus(f) != nil
	})

	res := post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"pause"}`)
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("the session was gone after streaming: %d", res.StatusCode)
	}
}

// An oversized or slow body must not be read into memory or hold a handler open.
func TestControlBodiesAreBounded(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)

	huge := `{"action":"pause","pad":"` + strings.Repeat("x", 64<<10) + `"}`
	res := post(t, server, "/api/sessions/"+created.Id+"/control", huge)
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("a 64KB control body returned %d, want 400", res.StatusCode)
	}

	// And the session is untouched by the attempt.
	res = post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"pause"}`)
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Errorf("an ordinary control after an oversized one returned %d", res.StatusCode)
	}
}

// The stream ends when the tournament does. Holding it open would keep touching
// a session that has already stopped everything it started, pinning it for as
// long as the tab exists.
func TestStreamClosesWhenTheTournamentFinishes(t *testing.T) {
	server := newTestServer(t)
	created := createSession(t, server)
	post(t, server, "/api/sessions/"+created.Id+"/control", `{"action":"start"}`).Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		server.URL+"/api/sessions/"+created.Id+"/stream", nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer res.Body.Close()

	// Read to EOF. If the server kept the stream open this would block until the
	// test's own deadline instead.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading the stream to its end: %v", err)
	}
	if !strings.Contains(string(body), `"done":true`) {
		t.Error("the stream ended without ever reporting the tournament finished")
	}
}
