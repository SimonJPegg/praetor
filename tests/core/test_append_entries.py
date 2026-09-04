from praetor.core import AppendEntries, AppendEntriesReply, Log, Node, NodeState, Role, handle


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
    event = AppendEntries[int](entries=[], term=1, sender=sender)
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
    event = AppendEntries[int](entries=[], term=2, sender=sender)
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
    event = AppendEntries[int](entries=[], term=4, sender=sender)
    new_state, messages = handle(state, event)
    assert new_state.term == event.term
    assert new_state.role == state.role
    assert new_state.log == state.log

    _, reply = messages[0]
    assert isinstance(reply, AppendEntriesReply)
    assert reply.success
    assert reply.sender == state.current_node
    assert reply.term == new_state.term
