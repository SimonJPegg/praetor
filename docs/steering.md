# Praetor — Project Steering

This is the engineering standard for this project. Every commit, every review, every decision
is measured against this. If something contradicts this doc, this doc wins.

---

## Technology Stack

- **Language:** Python 3.14
- **Concurrency:** asyncio. Actors are async tasks; mailboxes are `asyncio.Queue`. Cooperative,
  single-threaded — no threads, no locks in v1.
- **Packaging:** uv (lockfile committed)
- **Testing:** pytest + pytest-asyncio
- **Linting:** ruff (C901 on)
- **Typing:** mypy strict
- **Network:** in-process simulated transport in v1, behind a message interface. Real sockets are
  a later, optional transport — never a v1 dependency.

Minimal, boring dependencies. pydantic for the core data models — frozen, validated; validation
is where option-1 invariants live (log index matches position, non-negative terms). The standard
library for everything else. "Pure core" means pure *logic* — pure functions over immutable state,
no I/O or side effects in the transition path — not the absence of a data-modelling library.
Question anything beyond well-established data/validation libraries, and keep any dependency out
of the transition logic itself.

---

## Coding Style

- snake_case. Type hints everywhere. Single-line docstring on every function and class saying
  *why* it exists.
- Functional core. The Raft logic is pure functions over immutable state — build new state, don't
  mutate in place. Side effects (timers, sends, logging) live in the shell.
- Roles and messages as explicit types. `Follower`/`Candidate`/`Leader` and each RPC are distinct
  types, not string tags. Make the state machine readable from the type signatures.
- Model state with frozen dataclasses (or equivalent). Immutable by default.
- Avoid classes where a function does. Actors are the one place a class earns its keep (they hold
  a mailbox and a loop). The core prefers functions and data.
- Explicit exceptions per failure mode. Follow TRY003 — no long messages in the raise.
- Imports at the top. No module-level mutable state.

---

## Functions & Complexity

- Function > 10 lines → probably doing two things. Decompose. Exception: the event-dispatch
  function that routes by (role, event) — but each branch is an extracted handler.
- File > 200 lines → question it.
- Cyclomatic complexity > 10 → ruff C901 flags it. The Raft transition logic is where this bites;
  split by role or by message type before it does.

---

## Error Handling

- A Raft node never crashes on a message it doesn't like. Unexpected term, stale leader, malformed
  RPC — these are *handled transitions*, not exceptions. The paper defines what to do; do it.
- Reserve exceptions for programmer errors (illegal state that the type system should have made
  impossible) and shell-level I/O failures.
- Never swallow silently. Log and handle, or raise.
- Timeouts are events, not errors — an election timeout firing is normal control flow.

---

## Testing

- pytest. Isolated. No project-wide fixtures. Parametrise when it earns it.
- **Test the pure core, not the async shell.** Feed a state and an event, assert the new state and
  the outgoing messages. Deterministic, no `sleep`, no flakiness. This is the whole point of the
  seam.
- Sad paths first. Split votes, higher-term step-down, stale AppendEntries, log conflicts, minority
  that can't commit.
- Safety properties over the cluster (Phase 3): assert the invariants — one leader per term,
  committed entries never lost or overwritten — under injected partitions, delays, and reordering.
- Reproducible failures: seed the simulated schedule so a failing interleaving replays exactly.
- Coverage proportional to blast radius. The transition logic and commit rules need exhaustive
  tests. The demo runner does not.
- "I touched it, I tested it."

---

## Design Principles

- **The paper is the spec.** Correctness means "matches Raft." When in doubt, cite the section.
  Don't invent semantics — implement the defined ones.
- **Make the state machine visible.** Roles and transitions are explicit types read from the
  signatures, not buried in `if`/`else`.
- **Pure core, imperative shell.** The defining boundary. Guard it.
- **Deterministic by construction.** Time, network, and scheduling are inputs the shell controls,
  so behaviour is reproducible. No hidden wall-clock or real randomness in the core.
- **Constrain toward correctness.** Strong types, explicit transitions, exhaustive handling. Make
  illegal states hard to represent.
- **No magic.** A grad who understands Python and has read the Raft paper can trace a request from
  the mailbox to a state transition without framework archaeology.
- **Standalone, not a library.** A demonstration you run, not a package others depend on. No public
  API to keep stable, no adoption to court.

---

## Documentation

- Every public function/class: a single-line docstring saying *why*.
- Comment *why* when it isn't obvious. Never comment *what*.
- ADRs in `docs/decisions/` if a non-obvious call needs explaining in six months (e.g. asyncio over
  threads, simulated network over sockets, the pure/shell split). Context, Decision, Consequences.
- README leads with what it is and how to run the demo. Start thin, grow it as milestones land.

---

## Git & Commits

- Branch: `feature/<short-description>` or `fix/<short-description>`. Public repo, no tracker.
- Commit: imperative mood, 50-char subject, body explains what and why not how, wrap at 72.
- One logical change per commit. Don't mix refactor with feature.
- No WIP on main. Squash or rewrite before merge.
- Tag releases with semver.

---

## Self-Review Checklist

Before pushing:

1. Is the Raft logic in the pure core, or did it leak into the actor/timer shell?
2. Can this behaviour be tested without async and without `sleep`? If not, it's in the wrong layer.
3. Are roles and messages explicit types, or string tags?
4. Are the sad paths tested — split vote, higher term, log conflict, minority commit?
5. Specific exceptions, not swallowed errors? Handled transitions rather than crashes on bad input?
6. Does my addition meet this standard, even if surrounding code doesn't?
7. Would someone debug this at 2am without me? Docstrings, logging, clear transitions.
8. Linters clean? `ruff check` (C901 on), `mypy` strict.
9. Does it still match the paper? Can I cite the section?

---

## How I Work On This

This project is for learning, and I pair-program with an AI (Kiro, Copilot, whatever). I'm not
hiding that — but the AI does not write the code.

- **Explain** concepts, patterns, tradeoffs. Point me at docs and the Raft paper.
- **Help** me debug when I'm stuck. Ask me what I've tried first.
- **Review** what I've written. Hold me to the standard in this doc.
- **Do not write code for me.** Not functions, not tests, not "here's a starting point."
- If I ask "how do I do X?" — explain the approach, show the relevant API signature, link the
  docs. I type it.
- If I paste code and ask "is this right?" — review it, critique it, suggest improvements. Don't
  rewrite it.
- Scaffolding (build config, CI YAML, project skeleton) is an exception — that's plumbing, not
  learning.

The point is that I can explain every line because I wrote every line.

---

## What This Project Is Not

- Not a production consensus system. It doesn't persist, snapshot, or reconfigure membership in v1.
  It's a correct, demonstrable Raft core, run standalone.
- Not a library. Nobody installs it; there's no public API to keep stable.
- Not distributed over real networks in v1. Simulated, in-process, deterministic. Real sockets are
  a later flourish, not the point.
- Not an excuse to learn everything at once. Python only — no Scala/Cats port. One language, one
  hard thing.
- Not AI-generated slop. Every line understood, explainable, defensible.

---

## Quality Bar

Held to the Sluice standard. Sluice went to a chaos-tested 1.0.0. This stands next to it without
apology.

Two things follow:

- **This finishes.** v1 is election + replication + correctness-under-partition, tested. The
  bottomless parts (persistence, snapshots, membership, real sockets) are explicitly out and
  opt-in — not a gate v1 waits behind.
- **Correct, not just working.** "It elected a leader once" isn't the bar. The bar is the safety
  properties holding under injected partitions, reproducibly. Build the tests that prove it.
