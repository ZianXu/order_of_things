import { characterSvg } from "/static/characters.js";

// Everything on this page is built by folding an ordered feed of events from the
// backend. Nothing is polled, nothing is recomputed from a rendered view of the
// world: a game row appears because a new-game event arrived, a decision fills in
// because a decision event arrived, the leaderboard moves because a game
// completed. The browser is one more replica of the same state machine.
//
// The one exception is deliberate and kept visibly separate: which replicas are
// alive is a fact about the deployment, not about the tournament, so it arrives
// as its own kind of frame and is never mixed into the feed.

const STRATEGIES = ["cooperator", "flipper", "retaliator", "copy-leader"];
const REPLICAS = ["r0", "r1"];

// What each player does, and -- the point of the whole thing -- how much of the
// world it has to look at to do it. The two are the same sentence: the wider the
// view, the more coordination that player costs to run.
const BLURBS = {
  cooperator: ["always cooperates", "(looks at nothing)"],
  flipper: ["flips its own last move", "(looks at itself)"],
  retaliator: ["mirrors this opponent", "(looks at the opponent)"],
  "copy-leader": ["copies whoever leads", "(looks at everyone)"],
};

const $ = id => document.getElementById(id);

const state = {
  id: null,
  seed: null,
  games: 0,
  nextIndex: 0,
  rows: new Map(),   // gameId -> <tr>
  current: null,     // { id, a, b } — the game in flight, or null between games
  status: null,
  source: null,
};

// ------------------------------------------------------------------- welcome

$("cast").innerHTML = STRATEGIES.map(s => characterSvg(s, "awake")).join("");

$("play").addEventListener("click", async () => {
  $("play").disabled = true;
  $("play").textContent = "Starting…";
  try {
    const res = await fetch("/api/sessions", { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    start(await res.json());
  } catch (err) {
    $("play").disabled = false;
    $("play").textContent = "Play";
    alert("Could not start a session: " + err.message);
  }
});

function start(session) {
  state.id = session.id;
  state.seed = session.seed;
  state.games = session.games;

  $("metaGames").textContent = session.games;
  document.body.classList.add("playing");

  buildPlayers();
  connect();
  // The server's first admission is immediate, so wait until audio playback
  // has begun before releasing it. That puts the four game actions on the four
  // beats of the opening measure.
  startMusic().finally(() => control("start"));
}

// --------------------------------------------------------------------- music

// Background music, if the file is there. Everything below degrades to nothing
// when it is not: the button stays hidden and the page is otherwise unchanged.
//
// Playback starts from the Play click rather than on load, because browsers only
// release audio on a user gesture -- and because music starting before anyone has
// asked for it is rude.
const MUTE_KEY = "oot.muted";
const theme = $("theme");

function readMuted() {
  try {
    return localStorage.getItem(MUTE_KEY) === "1";
  } catch {
    return false; // private windows and blocked storage
  }
}

function writeMuted(muted) {
  try {
    localStorage.setItem(MUTE_KEY, muted ? "1" : "0");
  } catch {
    /* a remembered preference is a convenience, not a requirement */
  }
}

let musicAvailable = true;
theme.addEventListener("error", () => {
  musicAvailable = false;
  $("mute").hidden = true;
});

function paintMute() {
  const button = $("mute");
  button.textContent = theme.muted ? "Sound Off" : "Sound On";
  button.setAttribute("aria-pressed", String(theme.muted));
  button.title = theme.muted ? "Unmute the music" : "Mute the music";
}

function startMusic() {
  if (!musicAvailable) return Promise.resolve();
  theme.volume = 0.32;
  theme.muted = readMuted();
  paintMute();
  return theme.play()
    .then(() => {
      $("mute").hidden = false;
    })
    .catch(() => {
      // Refused despite the gesture, or there is nothing to play.
      $("mute").hidden = true;
    });
}

$("mute").addEventListener("click", () => {
  theme.muted = !theme.muted;
  writeMuted(theme.muted);
  paintMute();
});

// ------------------------------------------------------------------ controls

async function control(action) {
  await send(`/api/sessions/${state.id}/control`, { action });
}

async function replicaAction(component, replica, action, defect) {
  await send(`/api/sessions/${state.id}/replicas/${component}/${replica}`, {
    action,
    defect: defect || "",
  });
}

// send posts a control action and makes sure the viewer learns what happened to
// it, whatever that was.
//
// Every one of these can fail, and a button that silently does nothing is the
// worst possible answer: it looks exactly like the system ignoring you. A 409 is
// an ordinary refusal with a reason. A 404 means this session no longer exists --
// the server has been restarted, or it was reclaimed for being idle -- and no
// amount of clicking will ever work again, so say so rather than letting the
// page sit there frozen on its last known state.
async function send(path, body) {
  let res;
  try {
    res = await fetch(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    notice(`could not reach the server: ${err.message}`);
    return false;
  }

  if (res.ok) {
    // Whatever the last refusal was about, the viewer has now done something
    // that worked, so it has stopped being the answer to anything.
    clearNotice();
    return true;
  }
  if (res.status === 404) {
    sessionLost();
    return false;
  }
  if (res.status === 409) {
    const failed = await res.json().catch(() => ({}));
    notice(failed.error || "that could not be done");
    return false;
  }
  notice(`the server refused that (${res.status})`);
  return false;
}

// sessionLost is terminal: nothing on the page will work again, so it stops
// pretending and says why.
let lost = false;
function sessionLost() {
  if (lost) return;
  lost = true;
  state.source?.close();
  notice("This session no longer exists — the server restarted, or it was idle too long. Reload to start a new one.");
  $("pause").disabled = true;
  $("step").disabled = true;
}

$("pause").addEventListener("click", () => {
  const running = state.status?.running ?? true;
  if (musicAvailable) {
    if (running) theme.pause();
    else theme.play().catch(() => { /* the game still works without audio */ });
  }
  control(running ? "pause" : "resume");
});
$("step").addEventListener("click", () => control("step"));

// ------------------------------------------------------------------- players

// One box per player, two replicas inside it. Four players, each run as an
// active-active pair -- not eight independent characters. Each replica is still
// individually killable, because that is the whole point of there being two.
function buildPlayers() {
  for (const strategy of STRATEGIES) {
    $(`pair-${strategy}`).innerHTML = `
      <div class="pair" id="pairbox-${strategy}">
        <div class="pair-head">
          <div class="name">${strategy}</div>
          <div class="does">${BLURBS[strategy][0]}</div>
          <div class="sub">${BLURBS[strategy][1]}</div>
        </div>
        <div class="pair-body">
          ${REPLICAS.map(replica => `
            <div class="replica" id="rep-${strategy}-${replica}">
              <div class="art" id="art-${strategy}-${replica}"></div>
              <div class="tag">${replica}</div>
              <div class="state" id="state-${strategy}-${replica}"></div>
              <div class="buttons">
                <button data-act="kill" data-c="${strategy}" data-r="${replica}" title="Stop this replica; its partner carries the player alone">K</button>
                <button data-act="restart" data-c="${strategy}" data-r="${replica}" title="Bring it back: it replays the log and rejoins">R</button>
                <button data-act="restart-with-bug" data-c="${strategy}" data-r="${replica}" title="Bring it back with a corrupted decision function">B</button>
              </div>
            </div>`).join("")}
        </div>
      </div>`;
  }

  document.querySelector(".nodes").addEventListener("click", e => {
    const button = e.target.closest("button");
    if (!button) return;
    replicaAction(button.dataset.c, button.dataset.r, button.dataset.act, "decision");
  });
  paintPlayers();
}

// Is this strategy one of the two playing right now?
function playing(strategy) {
  return Boolean(
    state.current && (state.current.a === strategy || state.current.b === strategy),
  );
}

function replicaStatus(strategy, replica) {
  return state.status?.replicas?.find(
    r => r.component === strategy && r.replica === replica,
  );
}

// A replica's drawn state is the combination of two independent things: whether
// it is alive (deployment) and whether its strategy is in the current game
// (tournament). Alive-but-not-playing is translucent; alive-and-playing is solid.
function faceState(strategy, replica) {
  const live = replicaStatus(strategy, replica);
  // Only an explicit kill or a quarantine is a death. Every replica also stops
  // when the tournament ends, and drawing that as eight corpses would say
  // something went wrong when nothing did.
  if (live && (live.killed || live.quarantined)) return "gone";
  return playing(strategy) ? "awake" : "idle";
}

function paintPlayers() {
  for (const strategy of STRATEGIES) {
    $(`pairbox-${strategy}`).classList.toggle("playing", playing(strategy));

    for (const replica of REPLICAS) {
      const drawn = faceState(strategy, replica);
      const art = $(`art-${strategy}-${replica}`);
      if (art.dataset.state !== drawn) {
        art.dataset.state = drawn;
        art.innerHTML = characterSvg(strategy, drawn);
      }
      $(`rep-${strategy}-${replica}`).classList.toggle("gone", drawn === "gone");

      const info = replicaStatus(strategy, replica);
      $(`state-${strategy}-${replica}`).textContent =
        info?.quarantined ? "quarantined" : info?.killed ? "killed" : "";
    }
  }
}

// -------------------------------------------------------------------- stream

let retries = 0;

function connect() {
  if (lost) return;
  state.source?.close();
  const source = new EventSource(`/api/sessions/${state.id}/stream?from=${state.nextIndex}`);
  state.source = source;

  source.addEventListener("feed", e => {
    const event = JSON.parse(e.data);
    if (event.index < state.nextIndex) return; // already folded
    state.nextIndex = event.index + 1;
    apply(event);
    paintPlayers();
  });
  source.addEventListener("status", e => {
    state.status = JSON.parse(e.data);
    paintStatus();
    paintPlayers();
  });
  source.addEventListener("open", () => {
    retries = 0;
  });
  // Losing the connection is recoverable: reconnect and resume from where the
  // fold got to, exactly as a restarting replica resumes from the log. But not
  // forever -- a stream that will not come back means the session is gone, and
  // silently retrying behind a frozen page is worse than saying so.
  source.onerror = () => {
    source.close();
    if (lost) return;
    // The server closes the stream once the tournament is over: there is nothing
    // further to send, and holding it open would keep a finished session alive.
    // That is a normal ending, not a lost connection.
    if (state.status?.done) return;
    if (++retries > 5) {
      sessionLost();
      return;
    }
    setTimeout(connect, 800);
  };
}

// apply folds one event into the page. This is the whole renderer.
function apply(event) {
  logLine(event);
  pulse(event.seq);

  switch (event.kind) {
    case "new-game":
      state.current = { id: event.gameId, a: event.strategyA, b: event.strategyB };
      addGameRow(event);
      break;

    case "decision":
      fillDecision(event);
      break;

    case "game-completed":
      completeGame(event);
      paintBoard(event.leaderboard);
      state.current = null;
      break;
  }
}

// --------------------------------------------------------------------- games

function addGameRow(event) {
  const row = document.createElement("tr");
  row.className = "live fresh";
  row.innerHTML = `
    <td>${event.gameId + 1}</td>
    <td>${event.strategyA}</td><td class="pending" data-cell="a">&hellip;</td>
    <td>${event.strategyB}</td><td class="pending" data-cell="b">&hellip;</td>
    <td class="pending" data-cell="score">&hellip;</td>`;
  state.rows.set(event.gameId, row);
  $("games").prepend(row);
  setTimeout(() => row.classList.remove("fresh"), 400);

  // Keep the table from growing without bound over a long session.
  while ($("games").children.length > 60) $("games").lastElementChild.remove();
}

function fillDecision(event) {
  const row = state.rows.get(event.gameId);
  if (!row) return;
  const which = event.strategy === row.children[1].textContent ? "a" : "b";
  const cell = row.querySelector(`[data-cell="${which}"]`);
  cell.textContent = event.decision;
  cell.className = event.decision;
  showDecision(event);
}

// Decisions are durable in the game table, but this is the immediate cue for
// who just acted. The feed names the winning replica, so the label appears over
// the specific icon that earned the event rather than over both halves.
function showDecision(event) {
  if (!event.replica) return;
  const replica = $(`rep-${event.strategy}-${event.replica}`);
  if (!replica) return;

  const label = document.createElement("div");
  label.className = `decision-pop ${event.decision}`;
  label.textContent = event.decision;
  label.addEventListener("animationend", () => label.remove(), { once: true });
  replica.append(label);
}

function completeGame(event) {
  const row = state.rows.get(event.gameId);
  if (!row) return;
  row.classList.remove("live");
  const cell = row.querySelector('[data-cell="score"]');
  cell.className = "delta";
  cell.textContent = `${signed(event.payoffA)} / ${signed(event.payoffB)}`;
}

const signed = n => (n > 0 ? `+${n}` : `${n}`);

// --------------------------------------------------------------- leaderboard

function paintBoard(leaderboard) {
  if (!leaderboard?.length) return;
  const top = Math.max(1, ...leaderboard.map(e => Math.abs(e.score)));
  $("board").innerHTML = leaderboard.map(entry => `
    <tr>
      <td>${entry.strategy}</td>
      <td class="bar"><div style="width:${Math.max(3, (Math.abs(entry.score) / top) * 100)}%"></div></td>
      <td class="score">${entry.score}</td>
    </tr>`).join("");
}

// ----------------------------------------------------------------- log + hud

function logLine(event) {
  const line = document.createElement("div");
  const detail =
    event.kind === "new-game"
      ? `game ${event.gameId + 1}: ${event.strategyA} vs ${event.strategyB}`
      : event.kind === "decision"
        ? `${event.strategy} ${event.decision}s`
        : `game ${event.gameId + 1} scored ${signed(event.payoffA)}/${signed(event.payoffB)}`;
  line.innerHTML = `
    <span class="s">${event.kind === "game-completed" ? "" : event.seq}</span>
    <span class="r">${event.replica ? `${event.component.slice(0, 4)}/${event.replica}` : "derived"}</span>
    <span>${detail}</span>`;
  $("log").prepend(line);
  while ($("log").children.length > 120) $("log").lastElementChild.remove();
}

let pulseTimer = null;
function pulse(seq) {
  // Sequence IDs are zero-based for the protocol; the diagram is counting
  // admitted events, so its human-facing value starts at one.
  $("seqNum").textContent = seq + 1;
  const box = $("sequencer");
  box.classList.add("tick");
  clearTimeout(pulseTimer);
  pulseTimer = setTimeout(() => box.classList.remove("tick"), 220);
}

function paintStatus() {
  const s = state.status;
  if (!s) return;
  $("metaGame").textContent = s.completed;
  $("metaHash").textContent = s.stateHash;
  $("pause").textContent = s.running ? "Pause" : "Resume";
  $("pause").disabled = s.done;
  $("step").disabled = s.done;

  if (s.done) {
    banner(`Tournament complete — ${s.completed} games, state checksum ${s.stateHash}. Same result, whatever you did to it.`, true);
  } else if (s.stalled) {
    banner(`Stalled: no live replica of ${s.waitingOn}. Nothing can happen until one comes back — restart either half.`, false);
  } else {
    $("banner").classList.remove("show");
  }
}

// banner carries the state of the session: finished, or stalled and waiting on
// somebody. It is owned by the status frames.
function banner(text, done) {
  const el = $("banner");
  el.textContent = text;
  el.classList.add("show");
  el.classList.toggle("done", Boolean(done));
}

// notice carries the answer to something the viewer just tried and could not do.
// It gets its own line rather than overwriting the banner: the two say different
// things, and "the tournament has finished" is the explanation for the refusal
// rather than a replacement for it.
//
// It also has to be its own line for a duller reason. Status frames are only sent
// when the status changes, and a finished session does not change again -- so a
// refusal written into the banner would have stayed there for good, with nothing
// left to put the result back.
//
// It stays up until something resolves it: another refusal replaces it, and an
// action that succeeds clears it. Timing it out would mean the explanation for
// what just happened could vanish before it had been read, which is the one thing
// a message like this must not do.
function notice(text) {
  const el = $("notice");
  el.textContent = text.replace(/^session:\s*/, "");
  el.classList.add("show");
}

function clearNotice() {
  $("notice").classList.remove("show");
}
