from praetor.actors.message_dispatcher import InMemoryDispatcher
from praetor.actors.raft_actor import RaftActor
from praetor.actors.traits import AddressedEvent
from praetor.core import ElectionTimeout, Node, Role


async def test_actors_transition_state() -> None:

    dispatcher = InMemoryDispatcher[int]()
    peers: set[Node] = set()
    node = Node(name="jim", uri="1")
    actor = RaftActor(node, peers=peers, dispatcher=dispatcher)
    actor.start()
    initial_term = actor._state.term
    actor.tell(AddressedEvent(to=frozenset(), event=ElectionTimeout()))
    await actor.mailbox.join()
    assert actor._state.term == initial_term + 1
    assert actor._state.role == Role.Candidate
    assert actor._state.voted_for == node
    await actor.stop()
