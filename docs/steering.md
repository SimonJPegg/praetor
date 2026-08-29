# Praetor — Project Steering

This is the engineering standard for this project. Every commit, every review, every decision
is measured against it. If something contradicts this doc, this doc wins.

---

## Technology Stack

- Python 3.14.
- asyncio for concurrency. Actors are async tasks, mailboxes are `asyncio.Queue`. Cooperative and
  single-threaded. No threads, no locks in v1.
- uv for packaging, lockfile committed.
- pytest + pytest-asyncio.
- ruff, with C901 on.
- mypy strict.
- Network is an in-process simulated transport in v1, sitting behind a message interface. Real
  sockets come later as an optional transport, never a v1 dependency.

Keep dependencies minimal and boring. pydantic for the core data models, frozen and validated. The
model validators enforce the invariants: log index matches position, terms are non-negative. The
standard library does everything else. "Pure core" means pure logic: pure functions over immutable
state, with no I/O or side effects in the transition path. It doesn't mean banning a data-modelling
library. Question anything beyond well-established data and validation libraries, and keep every
dependency out of the transition logic itself.

---

## Coding Style

- snake_case. Type hints everywhere. Single-line docstring on every function and class saying
  *why* it exists.
- Functional core. The Raft logic is pure functions over immutable state. Build new state, don't
  mutate in place. Side effects (timers, sends, logging) live in the shell.
- Roles and messages are explicit types. `Follower`/`Candidate`/`Leader` and each RPC are distinct
  types, not string tags. You should be able to read the state machine off the type signatures.
- Model state with frozen dataclasses or equivalent. Immutable by default.
- Avoid classes where a function does. Actors are the exception, since they hold a mailbox and a
  loop. The core is functions and data.
- Explicit exceptions per failure mode. Follow TRY003, no long messages in the raise.
- Imports at the top. No module-level mutable state.

---

## Functions & Complexity

- Function over 10 lines is probably doing two things. Decompose. Exception: the event-dispatch
  function that routes by (role, event), and even there each branch is an extracted handler.
- File over 200 lines, question it.
- Cyclomatic complexity over 10 gets flagged by ruff C901. The transition logic is where this
  bites, so split it by role or by message type early.

---

## Error Handling

- A Raft node never crashes on a message it doesn't like. Unexpected term, stale leader, malformed
  RPC are all handled transitions rather than exceptions. Follow what the paper says to do.
- Reserve exceptions for programmer errors (illegal state the type system should have made
  impossible) and shell-level I/O failures.
- Never swallow silently. Log and handle, or raise.
- Timeouts are events, not errors. An election timeout firing is normal control flow.

---

## Testing

- pytest. Isolated. No project-wide fixtures. Parametrise when it earns it.
- Test the pure core, not the async shell. Feed a state and an event, assert the new state and the
  outgoing messages. Deterministic, no `sleep`, no flakiness. That's why the seam exists.
- Sad paths first. Split votes, higher-term step-down, stale AppendEntries, log conflicts, a
  minority that can't commit.
- Safety properties over the cluster (Phase 3): assert the invariants under injected partitions,
  delays, and reordering. One leader per term, committed entries never lost or overwritten.
- Reproducible failures. Seed the simulated schedule so a failing interleaving replays exactly.
- Coverage proportional to blast radius. Transition logic and commit rules need exhaustive tests.
  The demo runner does not.
- "I touched it, I tested it."

---

## Design Principles

- The paper is the spec. Correctness means it matches Raft. When in doubt, cite the section rather
  than invent semantics.
- Roles and transitions should be explicit types you read off the signatures. Keep them out of
  nested `if`/`else`.
- Pure core, imperative shell. Everything else hangs off that boundary, so guard it.
- Deterministic by construction. Time, network, and scheduling are inputs the shell controls, so
  behaviour replays. No hidden wall-clock, no real randomness in the core.
- Strong types, explicit transitions, exhaustive handling. Make illegal states hard to represent.
- No magic. A grad who knows Python and has read the paper should be able to trace a request from
  the mailbox to a state transition without digging through framework internals.
- Standalone, not a library. It's a demonstration you run. There's no public API to keep stable and
  no consumers to support.

---

## Documentation

- Every public function and class: a single-line docstring saying *why*.
- Comment *why* when it isn't obvious. Don't narrate *what* the code already says.
- ADRs in `docs/decisions/` when a non-obvious call needs explaining in six months. asyncio over
  threads, simulated network over sockets, the pure/shell split. Context, Decision, Consequences.
- README leads with what it is and how to run the demo. Start thin, grow it as milestones land.

---

## Git & Commits

- Commit: imperative mood, 50-char message.
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
8. Linters clean? `ruff check` with C901 on, `mypy` strict.
9. Does it still match the paper? Can I cite the section?

---

## How I Work On This

This project is for learning. AI (Kiro, Copilot, whatever) does not write the code.

- Explain concepts, patterns, tradeoffs. Point me at the docs and the Raft paper.
- Help me debug when I'm stuck. Ask what I've tried first.
- Review what I've written. Hold me to the standard in this doc.
- Do not write code for me. Not functions, not tests, not "here's a starting point."
- If I ask "how do I do X?", explain the approach, show the relevant API signature, link the docs.
  I type it.
- If I paste code and ask "is this right?", review it and critique it. Don't rewrite it.
- Scaffolding — build config, CI YAML, project skeleton — is the exception. That's plumbing.

I want to be able to explain every line, because I wrote every line.

---

## What This Project Is Not

- Not a production consensus system. It doesn't persist, snapshot, or reconfigure membership in v1.
  It's a correct, demonstrable Raft core, run standalone.
- Not a library. Nobody installs it, there's no public API to keep stable.
- Not distributed over real networks in v1. Simulated, in-process, deterministic. Real sockets come
  later, if at all.

---

## Quality Bar

> "Would a senior engineer at Monzo/Wise look at this repo and say: that person knows what they're doing?"

If the answer isn't yes, it's not done.

- This finishes. v1 is election + replication + correctness-under-partition, tested. The deeper
  work — persistence, snapshots, membership, real sockets — is explicitly out and opt-in. v1
  doesn't wait behind it.
- Working isn't the bar, correct is. "It elected a leader once" doesn't count. The bar is the
  safety properties holding under injected partitions, reproducibly. Build the tests that prove it.
