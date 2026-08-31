# 001 — Addressing outside the message

## Context

The core is pure: `(state, event) -> (new_state, outgoing)`. A reply has to get back to the
candidate that asked, so something needs the recipient. Considered putting a `to` field on the
message given that we already have `candidate` and `sender` as fields.

However `candidate` and `sender` are part of the Raft spec and nothing in core reads `to`.
It's an envelope address, and every test would need to construct a routing field the transition ignores.

## Decision

`handle` returns `list[tuple[frozenset[Node], Message]]`. Recipient beside the message, not
inside it. The shell peels it off and routes. Message types stay pure Raft payload.

## Consequences

The actors forward messages without inspecting the message type. Adding a new message type doesn't
change the wrapping function.

But, `handle`'s return type is busier than `list[Message]` :(
