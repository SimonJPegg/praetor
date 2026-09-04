import asyncio

from praetor.actors.message_dispatcher import InMemoryDispatcher
from praetor.actors.raft_actor_system import RaftActorSystem


async def main() -> None:
    system = RaftActorSystem[str](actor_count=51, max_election_timeout_seconds=10, dispatcher=InMemoryDispatcher())
    system.start()
    await asyncio.sleep(40)
    await system.stop()


if __name__ == "__main__":
    asyncio.run(main())
