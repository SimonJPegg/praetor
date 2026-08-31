import pytest

from praetor.core import ElectionTimeout, Log, Node, NodeState, RequestVoteReply, Role, handle


@pytest.mark.parametrize(
    "peers,votes_from,messages,expected",
    [
        (
            frozenset({Node(name="steve", uri="2"), Node(name="jim", uri="4")}),
            frozenset({Node(name="dave", uri="1")}),
            [RequestVoteReply(term=2, vote_granted=True, sender=Node(name="steve", uri="2"))],
            Role.Leader,
        ),
        (
            frozenset(
                {
                    Node(name="steve", uri="2"),
                    Node(name="rachael", uri="3"),
                    Node(name="jim", uri="4"),
                    Node(name="bart", uri="5"),
                }
            ),
            frozenset({Node(name="dave", uri="1")}),
            [
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="steve", uri="2")),
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="jim", uri="4")),
            ],
            Role.Leader,
        ),
        (
            frozenset(
                {
                    Node(name="steve", uri="2"),
                    Node(name="rachael", uri="3"),
                    Node(name="jim", uri="4"),
                    Node(name="bart", uri="5"),
                }
            ),
            frozenset({Node(name="dave", uri="1")}),
            [
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="jim", uri="4")),
                RequestVoteReply(term=2, vote_granted=False, sender=Node(name="rachael", uri="3")),
            ],
            Role.Candidate,
        ),
    ],
)
def test_majority_win(
    peers: frozenset[Node], votes_from: frozenset[Node], messages: list[RequestVoteReply], expected: Role
) -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=2,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=peers,
        has_votes_from=votes_from,
        current_node=node,
    )

    for message in messages:
        state, _ = handle(state, message)
    assert state.role == expected


def test_replies_from_old_term_are_ignored() -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=7,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=frozenset({Node(name="steve", uri="2"), Node(name="jim", uri="4")}),
        has_votes_from=frozenset({node}),
        current_node=node,
    )
    reply = RequestVoteReply(term=6, vote_granted=True, sender=Node(name="jim", uri="4"))
    state, _ = handle(state, reply)

    assert state.role == Role.Candidate
    assert Node(name="jim", uri="4") not in state.has_votes_from


def test_replies_when_not_a_candidate_are_ignored() -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=7,
        role=Role.Follower,
        voted_for=node,
        log=Log[int](),
        peers=frozenset({Node(name="steve", uri="2"), Node(name="jim", uri="4")}),
        has_votes_from=frozenset({node}),
        current_node=node,
    )
    reply = RequestVoteReply(term=7, vote_granted=True, sender=Node(name="jim", uri="4"))
    state, _ = handle(state, reply)
    assert state.role == Role.Follower
    assert Node(name="jim", uri="4") not in state.has_votes_from


def test_duplicate_votes_from_peer_do_not_trigger_premature_leader() -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=1,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=frozenset(
            {
                Node(name="steve", uri="2"),
                Node(name="rachael", uri="3"),
                Node(name="jim", uri="4"),
                Node(name="bart", uri="5"),
            }
        ),
        has_votes_from=frozenset({node}),
        current_node=node,
    )

    for vote in [
        RequestVoteReply(term=1, vote_granted=True, sender=Node(name="rachael", uri="3")),
        RequestVoteReply(term=1, vote_granted=True, sender=Node(name="rachael", uri="3")),
    ]:
        state, _ = handle(state, vote)

    assert state.role == Role.Candidate
    assert state.has_votes_from == frozenset({node, Node(name="rachael", uri="3")})


@pytest.mark.parametrize(
    "peers,votes_from,messages,expected",
    [
        (
            frozenset({Node(name="steve", uri="2"), Node(name="jim", uri="4")}),
            frozenset({Node(name="dave", uri="1")}),
            [
                RequestVoteReply(term=1, vote_granted=False, sender=Node(name="steve", uri="2")),
                ElectionTimeout(),
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="steve", uri="2")),
            ],
            Role.Leader,
        ),
        (
            frozenset(
                {
                    Node(name="steve", uri="2"),
                    Node(name="rachael", uri="3"),
                    Node(name="jim", uri="4"),
                    Node(name="bart", uri="5"),
                }
            ),
            frozenset({Node(name="dave", uri="1")}),
            [
                RequestVoteReply(term=1, vote_granted=True, sender=Node(name="steve", uri="2")),
                RequestVoteReply(term=1, vote_granted=False, sender=Node(name="jim", uri="4")),
                ElectionTimeout(),
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="steve", uri="2")),
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="jim", uri="4")),
            ],
            Role.Leader,
        ),
        (
            frozenset(
                {
                    Node(name="steve", uri="2"),
                    Node(name="rachael", uri="3"),
                    Node(name="jim", uri="4"),
                    Node(name="bart", uri="5"),
                }
            ),
            frozenset({Node(name="dave", uri="1")}),
            [
                RequestVoteReply(term=1, vote_granted=True, sender=Node(name="jim", uri="4")),
                RequestVoteReply(term=1, vote_granted=False, sender=Node(name="rachael", uri="3")),
                ElectionTimeout(),
                RequestVoteReply(term=2, vote_granted=True, sender=Node(name="jim", uri="4")),
                RequestVoteReply(term=2, vote_granted=False, sender=Node(name="rachael", uri="3")),
            ],
            Role.Candidate,
        ),
    ],
)
def test_election_recovers_or_persists_after_split(
    peers: frozenset[Node], votes_from: frozenset[Node], messages: list[RequestVoteReply], expected: Role
) -> None:
    node = Node(name="dave", uri="1")
    state = NodeState(
        term=1,
        role=Role.Candidate,
        voted_for=node,
        log=Log[int](),
        peers=peers,
        has_votes_from=votes_from,
        current_node=node,
    )

    for message in messages:
        state, _ = handle(state, message)
        if isinstance(message, ElectionTimeout):
            assert state.role == Role.Candidate
            assert state.has_votes_from == frozenset({node})
            assert state.term == 2
    assert state.role == expected
    assert state.term == 2
