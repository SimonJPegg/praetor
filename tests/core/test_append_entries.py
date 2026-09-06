from praetor.core import AppendEntries, AppendEntriesReply, Log, Node, NodeState, Role, handle
from praetor.core.node_state import LogEntry


def test_stale_terms_are_rejected() -> None:
    sender = Node(name="steve", uri="2")
    state = NodeState(
        term=2,
        role=Role.Follower,
        voted_for=None,
        log=Log[int](),
        peers=frozenset({sender}),
        current_node=Node(name="dave", uri="1"),
    )
    event = AppendEntries[int](entries=[], term=1, sender=sender, prev_log_index=0, prev_log_term=0)
    new_state, messages = handle(state, event)
    assert new_state.term == state.term
    assert new_state.role == state.role
    assert new_state.log == state.log

    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert not reply.success
    assert reply.sender == state.current_node
    assert reply.term == state.term


def test_leader_steps_down_on_higher_term() -> None:
    sender = Node(name="steve", uri="2")
    state = NodeState(
        term=1,
        role=Role.Follower,
        voted_for=None,
        log=Log[int](),
        peers=frozenset({sender}),
        current_node=Node(name="dave", uri="1"),
    )
    event = AppendEntries[int](entries=[], term=2, sender=sender, prev_log_index=0, prev_log_term=0)
    new_state, messages = handle(state, event)
    assert new_state.term == event.term
    assert new_state.role == Role.Follower
    assert new_state.log == state.log

    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success
    assert reply.sender == state.current_node
    assert reply.term == new_state.term


def test_valid_request_is_accepted() -> None:
    sender = Node(name="steve", uri="2")
    state = NodeState(
        term=4,
        role=Role.Follower,
        voted_for=None,
        log=Log[int](),
        peers=frozenset({sender}),
        current_node=Node(name="dave", uri="1"),
    )
    event = AppendEntries[int](entries=[], term=4, sender=sender, prev_log_index=0, prev_log_term=0)
    new_state, messages = handle(state, event)
    assert new_state.term == event.term
    assert new_state.role == state.role
    assert new_state.log == state.log

    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success
    assert reply.sender == state.current_node
    assert reply.term == new_state.term


def test_candidate_steps_down_at_equal_term() -> None:
    sender = Node(name="steve", uri="2")
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=4,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=frozenset({sender}),
        current_node=node,
    )
    event = AppendEntries[int](entries=[], term=4, sender=sender, prev_log_index=0, prev_log_term=0)
    new_state, messages = handle(state, event)
    assert new_state.role == Role.Follower
    assert new_state.term == 4

    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success


def test_index_and_term_agree() -> None:
    entries = [LogEntry[int](term=(index // 2) + 1, index=index, command=index) for index in range(1, 11)]
    log = Log[int]()
    for entry in entries:
        log = log.append(entry.command, entry.term)

    sender = Node(name="steve", uri="2")
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=6,
        role=Role.Follower,
        voted_for=None,
        log=log,
        peers=frozenset({sender}),
        current_node=node,
    )
    event = AppendEntries[int](
        entries=entries, term=6, sender=sender, prev_log_index=entries[9].index, prev_log_term=entries[9].term
    )
    _, messages = handle(state, event)
    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success


def test_no_entry_at_index() -> None:
    entries = [LogEntry[int](term=(index // 2) + 1, index=index, command=index) for index in range(1, 11)]
    log = Log[int]()
    for entry in entries[:-3]:
        log = log.append(entry.command, entry.term)

    sender = Node(name="steve", uri="2")
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=6,
        role=Role.Follower,
        voted_for=None,
        log=log,
        peers=frozenset({sender}),
        current_node=node,
    )
    event = AppendEntries[int](
        entries=entries[-3:], term=6, sender=sender, prev_log_index=entries[7].index, prev_log_term=entries[7].term
    )
    _, messages = handle(state, event)
    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert not reply.success


def test_last_term_mismatch() -> None:
    entries = [LogEntry[int](term=(index // 2) + 1, index=index, command=index) for index in range(1, 11)]
    log = Log[int]()
    for entry in entries:
        log = log.append(entry.command, entry.term)

    sender = Node(name="steve", uri="2")
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=6,
        role=Role.Follower,
        voted_for=None,
        log=log,
        peers=frozenset({sender}),
        current_node=node,
    )

    event = AppendEntries[int](
        entries=entries, term=6, sender=sender, prev_log_index=entries[9].index - 1, prev_log_term=entries[9].term
    )
    _, messages = handle(state, event)
    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert not reply.success


def test_append_to_new_empty_log() -> None:
    entries = [LogEntry[int](term=(index // 2) + 1, index=index, command=index) for index in range(1, 11)]
    log = Log[int]()

    sender = Node(name="steve", uri="2")
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=6,
        role=Role.Follower,
        voted_for=None,
        log=log,
        peers=frozenset({sender}),
        current_node=node,
    )
    event = AppendEntries[int](entries=entries, term=6, sender=sender, prev_log_index=0, prev_log_term=0)
    _, messages = handle(state, event)
    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success
