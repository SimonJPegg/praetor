from praetor.core import Log, Node, NodeState, RequestVote, RequestVoteReply, Role, handle


def test_a_node_wont_vote_twice() -> None:
    state = NodeState(
        term=1,
        role=Role.Leader,
        voted_for=None,
        log=Log[int](),
        peers=frozenset(),
        current_node=Node(name="dave", uri="1"),
    )
    request_one = RequestVote(term=2, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)
    request_two = RequestVote(term=2, candidate=Node(name="rachael", uri="3"), last_log_index=1, last_log_term=1)

    state, replies_one = handle(state, request_one)
    state, replies_two = handle(state, request_two)

    reply_one = replies_one[0]
    reply_two = replies_two[0]
    assert isinstance(reply_one, RequestVoteReply)
    assert isinstance(reply_two, RequestVoteReply)
    assert reply_one.vote_granted
    assert not reply_two.vote_granted
    assert reply_one.sender == state.current_node
    assert reply_two.sender == state.current_node
    assert state.voted_for == request_one.candidate


def test_higher_term_causes_step_down() -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=2,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=frozenset(),
        current_node=node,
    )
    request = RequestVote(term=5, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)
    state, messages = handle(state, request)
    reply = messages[0]
    assert isinstance(reply, RequestVoteReply)
    assert reply.vote_granted
    assert reply.sender == node
    assert reply.term == 5
    assert reply.term == state.term
    assert state.role == Role.Follower


def test_votes_idempotent_in_term() -> None:
    state = NodeState(
        term=1,
        role=Role.Leader,
        voted_for=None,
        log=Log[int](),
        peers=frozenset(),
        current_node=Node(name="dave", uri="1"),
    )
    request_one = RequestVote(term=2, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)
    request_two = RequestVote(term=2, candidate=Node(name="rachael", uri="3"), last_log_index=1, last_log_term=1)
    request_three = RequestVote(term=2, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)

    state, replies_one = handle(state, request_one)
    state, replies_two = handle(state, request_two)
    state, replies_three = handle(state, request_three)

    reply_one = replies_one[0]
    reply_two = replies_two[0]
    reply_three = replies_three[0]
    assert isinstance(reply_one, RequestVoteReply)
    assert isinstance(reply_two, RequestVoteReply)
    assert isinstance(reply_three, RequestVoteReply)
    assert reply_one.vote_granted
    assert not reply_two.vote_granted
    assert reply_three.vote_granted
    assert reply_one.sender == state.current_node
    assert reply_two.sender == state.current_node
    assert reply_three.sender == state.current_node
    assert state.voted_for == request_one.candidate
    assert state.voted_for == request_three.candidate


def test_deny_stale_log() -> None:
    state = NodeState(
        term=3,
        role=Role.Leader,
        voted_for=None,
        log=Log[int]().append(1, 1).append(2, 2),
        peers=frozenset(),
        current_node=Node(name="dave", uri="1"),
    )
    request = RequestVote(term=4, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)
    state, messages = handle(state, request)

    reply = messages[0]
    assert isinstance(reply, RequestVoteReply)
    assert not reply.vote_granted
    assert state.voted_for is None
    assert state.term == 4


def test_stale_request_votes_are_denied() -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=7,
        role=Role.Follower,
        voted_for=node,
        log=Log[int](),
        peers=frozenset({Node(name="steve", uri="2"), Node(name="jim", uri="4")}),
        has_votes_from=frozenset(),
        current_node=node,
    )
    request = RequestVote(term=6, candidate=Node(name="steve", uri="2"), last_log_index=1, last_log_term=1)
    state, messages = handle(state, request)

    reply = messages[0]
    assert state.role == Role.Follower
    assert isinstance(reply, RequestVoteReply)
    assert not reply.vote_granted
    assert reply.term == 7
