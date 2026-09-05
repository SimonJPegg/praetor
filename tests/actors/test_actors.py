import asyncio

import pytest
from _pytest.monkeypatch import MonkeyPatch

from praetor.actors.message_dispatcher import InMemoryDispatcher
from praetor.actors.raft_actor import RaftActor
from praetor.actors.traits import AddressedEvent
from praetor.core import ElectionTimeout, Node, RequestVoteReply, Role


async def test_actors_transition_state() -> None:

    dispatcher = InMemoryDispatcher[int]()
    peers: set[Node] = set()
    node = Node(name="jim", uri="1")
    actor = RaftActor(node, peers=peers, dispatcher=dispatcher, max_election_timeout_seconds=10)
    actor.start()
    initial_term = actor._state.term
    actor.tell(AddressedEvent(to=frozenset(), event=ElectionTimeout()))
    await actor.mailbox.join()
    assert actor._state.term == initial_term + 1
    assert actor._state.role == Role.Candidate
    assert actor._state.voted_for == node
    await actor.stop()


async def test_exceptions_on_mailbox_do_not_terminate_actor(monkeypatch: MonkeyPatch) -> None:
    dispatcher = InMemoryDispatcher[int]()
    peers: set[Node] = set()
    node = Node(name="jim", uri="1")
    actor = RaftActor(node, peers=peers, dispatcher=dispatcher, max_election_timeout_seconds=10)
    actor.start()
    monkeypatch.setattr("praetor.actors.raft_actor.handle", lambda _, __: exec("raise RuntimeError('error')"))
    with pytest.raises(RuntimeError):
        _ = await actor.ask(AddressedEvent(to=frozenset({}), event=ElectionTimeout()))

    monkeypatch.undo()
    state = await actor.ask(AddressedEvent(to=frozenset({}), event=ElectionTimeout()))
    assert state.role == Role.Candidate
    await actor.stop()


async def test_leader_stops_heartbeating_when_dispatch_fails(monkeypatch: MonkeyPatch) -> None:

    test_node = Node(name="jim", uri="1")
    peer_node = Node(name="Ted Danson!!!", uri="100")
    dispatcher = InMemoryDispatcher[int]()
    actor = RaftActor(test_node, peers={peer_node}, dispatcher=dispatcher, max_election_timeout_seconds=2)
    peer_actor = RaftActor(peer_node, peers={test_node}, dispatcher=dispatcher, max_election_timeout_seconds=2)
    dispatcher.register(test_node, actor)
    dispatcher.register(peer_node, peer_actor)
    actor.start()

    node_state = await actor.ask(AddressedEvent(to=frozenset(), event=ElectionTimeout()))
    assert node_state.role == Role.Candidate
    node_state = await actor.ask(
        AddressedEvent(
            to=frozenset(),
            event=RequestVoteReply(term=actor._state.term, vote_granted=True, sender=peer_node),
        )
    )
    assert node_state.role == Role.Leader

    monkeypatch.setattr(dispatcher, "enqueue_messages", lambda _: exec("raise RuntimeError('error')"))

    assert actor._heartbeat_task is not None
    async with asyncio.timeout(1):
        with pytest.raises(RuntimeError):
            await actor._heartbeat_task
    await actor.stop()


async def test_cancellation_does_not_trigger_failure_handling() -> None:
    """stop() must cancel loops cleanly, not route through failure handling."""
    test_node = Node(name="jim", uri="1")
    actor = RaftActor(test_node, peers=set(), dispatcher=InMemoryDispatcher[int](), max_election_timeout_seconds=2)
    actor.start()
    await actor.stop()
    assert actor._election_task is not None
    assert actor._election_task.cancelled()
