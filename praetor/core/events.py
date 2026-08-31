from pydantic import BaseModel, ConfigDict

from praetor.core.node_state import Node, NodeState, Role


class RequestVote(BaseModel):
    """A request for a vote in a leadership election"""

    model_config = ConfigDict(frozen=True)
    term: int
    candidate: Node
    last_log_index: int
    last_log_term: int


class RequestVoteReply(BaseModel):
    """A response to a request for a vote"""

    model_config = ConfigDict(frozen=True)
    term: int
    vote_granted: bool
    sender: Node


class ElectionTimeout(BaseModel):
    """Signals that a leadership election has timed out"""

    model_config = ConfigDict(frozen=True)


type Event[Command] = RequestVote | RequestVoteReply | ElectionTimeout
type Message[Command] = tuple[frozenset[Node], RequestVote | RequestVoteReply]


def _step_down[Command](state: NodeState[Command], term: int) -> NodeState[Command]:
    """Step down to follower status"""
    return state.model_copy(
        update={
            "role": Role.Follower,
            "voted_for": None,
            "term": term,
            "has_votes_from": frozenset({}),
        }
    )


def _handle_election_timeout[Command](
    state: NodeState[Command], _: ElectionTimeout
) -> tuple[NodeState[Command], list[Message[Command]]]:
    """converts the node to a candidate and creates a request to become the leader"""

    return state.model_copy(
        update={
            "role": Role.Candidate,
            "term": state.term + 1,
            "voted_for": state.current_node,
            "has_votes_from": frozenset({state.current_node}),
        }
    ), [
        (
            state.peers,
            RequestVote(
                term=state.term + 1,
                candidate=state.current_node,
                last_log_index=state.log.last_index,
                last_log_term=state.log.last_term,
            ),
        )
    ]


def _handle_request_vote[Command](
    state: NodeState[Command], request: RequestVote
) -> tuple[NodeState[Command], list[Message[Command]]]:
    """Handle a request from a peer to become leader"""
    if request.term < state.term:
        return state, [
            (
                frozenset({request.candidate}),
                RequestVoteReply(term=state.term, vote_granted=False, sender=state.current_node),
            )
        ]

    if request.term > state.term:
        state = _step_down(state, request.term)

    vote_granted = (
        (state.voted_for is None or state.voted_for == request.candidate)
        and state.log.is_current(request.last_log_term, request.last_log_index)
        and request.term >= state.term
    )
    if vote_granted:
        state = state.model_copy(
            update={
                "voted_for": request.candidate,
            }
        )

    return state, [
        (
            frozenset({request.candidate}),
            RequestVoteReply(term=state.term, vote_granted=vote_granted, sender=state.current_node),
        )
    ]


def _handle_request_vote_reply[Command](
    state: NodeState[Command], reply: RequestVoteReply
) -> tuple[NodeState[Command], list[Message[Command]]]:
    """Count check replies to a vote request and become leader when a quorum is reached"""
    if not reply.vote_granted or reply.term < state.term or state.role != Role.Candidate:
        return state, []

    if reply.term > state.term:
        state = _step_down(state, reply.term)
        return state, []

    state = state.model_copy(update={"has_votes_from": frozenset(state.has_votes_from | {reply.sender})})

    cluster = len(state.peers) + 1
    majority = (cluster // 2) + 1

    if len(state.has_votes_from) >= majority:
        state = state.model_copy(update={"role": Role.Leader})

    return state, []


def handle[Command](
    state: NodeState[Command], event: Event[Command]
) -> tuple[NodeState[Command], list[Message[Command]]]:
    """handle requests from peers"""
    match event:
        case ElectionTimeout():
            return _handle_election_timeout(state, event)
        case RequestVote():
            return _handle_request_vote(state, event)
        case RequestVoteReply():
            return _handle_request_vote_reply(state, event)
