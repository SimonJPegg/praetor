from praetor.core import ElectionTimeout, Log, Node, NodeState, RequestVote, Role, handle


def test_election_timeout_with_log_entries() -> None:
    state = NodeState(
        term=1,
        role=Role.Leader,
        voted_for=None,
        log=Log[int]().append(1, 1),
        peers=frozenset(),
        current_node=Node(name="dave", uri="1"),
    )
    new_state, messages = handle(state, ElectionTimeout())
    assert new_state.term == 2
    assert new_state.role == Role.Candidate
    assert new_state.voted_for == state.current_node

    _, request = messages[0]
    assert isinstance(request, RequestVote)
    assert request.term == state.term + 1
    assert request.candidate == state.current_node
    assert request.last_log_term == state.log.last_term
    assert request.last_log_index == state.log.last_index


def test_election_timeout_with_no_log() -> None:
    state = NodeState(
        term=1,
        role=Role.Leader,
        voted_for=None,
        log=Log[int](),
        peers=frozenset(),
        current_node=Node(name="dave", uri="1"),
    )
    new_state, messages = handle(state, ElectionTimeout())
    assert new_state.term == 2
    assert new_state.role == Role.Candidate
    assert new_state.voted_for == state.current_node

    _, request_vote = messages[0]
    assert isinstance(request_vote, RequestVote)
    assert request_vote.term == state.term + 1
    assert request_vote.candidate == state.current_node
    assert request_vote.last_log_term == 0
    assert request_vote.last_log_index == 0
