from asyncio import Future, Queue, Task, create_task, gather, get_running_loop

from praetor.actors.traits import Actor, AddressedEvent, MessageDispatcher
from praetor.core import Log, Node, NodeState, Role, handle


class RaftActor[Command](Actor[AddressedEvent[Command]]):
    """An actor that implements the RAFT protocol"""

    def __init__(self, node: Node, peers: set[Node], dispatcher: MessageDispatcher[Command]):
        """initialize the actor with an empty mailbox and no tasks"""
        self.mailbox: Queue[tuple[AddressedEvent[Command], Future[NodeState[Command]] | None]] = Queue()
        self._task: Task[None] | None = None
        self._dispatcher = dispatcher
        self._state: NodeState[Command] = NodeState(
            term=1,
            role=Role.Candidate,
            log=Log(),
            peers=frozenset(peers),
            current_node=node,
            voted_for=None,
            has_votes_from=frozenset(),
        )

    def start(self) -> None:
        """Start the actor's message processing loop."""
        self._task = create_task(self._run_loop())

    async def _run_loop(self) -> None:
        """Pull one event at a time and process it. Single consumer"""
        while True:
            message, future = await self.mailbox.get()
            try:
                result = await self.handle_message(message)
                if future and not future.done():
                    future.set_result(result)
            except Exception as e:
                if future and not future.done():
                    future.set_exception(e)
            finally:
                self.mailbox.task_done()

    async def handle_message(self, message: AddressedEvent[Command]) -> NodeState[Command]:
        """Handle raft messages"""
        self._state, messages = handle(self._state, message.event)
        self._dispatcher.enqueue_messages([AddressedEvent(to=to, event=event) for to, event in messages])
        return self._state

    async def send(self, message: AddressedEvent[Command]) -> NodeState[Command]:
        """Send a message to the actor and wait for a response."""
        future: Future[NodeState[Command]] = get_running_loop().create_future()
        await self.mailbox.put((message, future))
        return await future

    def tell(self, message: AddressedEvent[Command]) -> None:
        """Fire-and-forget messages to the actor"""
        self.mailbox.put_nowait((message, None))

    async def stop(self) -> None:
        """Stop the actor loop."""
        if self._task:
            self._task.cancel()
            await gather(self._task, return_exceptions=True)
