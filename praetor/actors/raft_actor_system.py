from praetor.actors.raft_actor import RaftActor
from praetor.actors.traits import Actor, ActorSystem, AddressedEvent, MessageDispatcher
from praetor.core import Node


class RaftActorSystem[T](ActorSystem):
    """Builds a cluster of actors."""

    def __init__(self, actor_count: int, dispatcher: MessageDispatcher[T]):
        """Spin up actor_count nodes, with peers and a shared dispatcher"""
        self._dispatcher = dispatcher
        self._actors: list[Actor[AddressedEvent[T]]] = []
        pool = {Node(name=f"actor{x}", uri=f"actor://local/actor{x}") for x in range(1, actor_count + 1)}
        for n in pool:
            actor = RaftActor(node=n, peers=pool - {n}, dispatcher=self._dispatcher)
            dispatcher.register(n, actor)
            self._actors.append(actor)

    def start(self) -> None:
        """Start every actor's loop."""
        for a in self._actors:
            a.start()

    async def stop(self) -> None:
        """Stop every actor."""
        for a in self._actors:
            await a.stop()
