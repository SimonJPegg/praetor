import asyncio
import random
from asyncio import CancelledError, Future, Queue, Task, create_task, gather, get_running_loop

from praetor.actors.traits import Actor, AddressedEvent, MessageDispatcher
from praetor.core import AppendEntries, ElectionTimeout, Event, Log, Node, NodeState, Role, handle
from praetor.logging import logger


class RaftActor[Command](Actor[AddressedEvent[Command]]):
    """An actor that implements the RAFT protocol"""

    def __init__(
        self, node: Node, peers: set[Node], max_election_timeout_seconds: int, dispatcher: MessageDispatcher[Command]
    ):
        """initialize the actor with an empty mailbox and no tasks"""
        self.mailbox: Queue[tuple[AddressedEvent[Command], Future[NodeState[Command]] | None]] = Queue()
        self._mailbox_task: Task[None] | None = None
        self._election_task: Task[None] | None = None
        self._heartbeat_task: Task[None] | None = None
        self._dispatcher = dispatcher
        self._state: NodeState[Command] = NodeState(
            term=0,
            role=Role.Follower,
            log=Log(),
            peers=frozenset(peers),
            current_node=node,
            voted_for=None,
            has_votes_from=frozenset(),
        )
        self._max_election_timeout_seconds = max_election_timeout_seconds

    def start(self) -> None:
        """Start the actor's message processing loop."""
        self._mailbox_task = create_task(self._run_loop())
        self._election_task = create_task(self._election_loop())

    async def _election_loop(self) -> None:
        """Followers and candidates run this."""
        while True:
            try:
                next_election_timeout = random.uniform(
                    self._max_election_timeout_seconds / 2, self._max_election_timeout_seconds
                )
                logger.debug(
                    f"will trigger an election in {next_election_timeout} without a heartbeat",
                    node=self._state.current_node.uri,
                    term=self._state.term,
                    role=self._state.role,
                )
                await asyncio.sleep(next_election_timeout)
                self.tell(AddressedEvent(to=frozenset(), event=ElectionTimeout()))
            except CancelledError:
                logger.debug(
                    "election loop cancelled",
                    node=self._state.current_node.uri,
                    term=self._state.term,
                    role=self._state.role,
                )
                raise

    async def _heartbeat_loop(self) -> None:
        """Only the leader runs this."""
        while True:
            try:
                next_heartbeat = self._max_election_timeout_seconds / 4
                logger.debug(
                    f"will heartbeat in {next_heartbeat}",
                    node=self._state.current_node.uri,
                    term=self._state.term,
                    role=self._state.role,
                )
                await asyncio.sleep(next_heartbeat)
                logger.debug(self._state.peers)
                self._dispatcher.enqueue_messages(
                    [
                        AddressedEvent(
                            to=frozenset({to}),
                            event=AppendEntries(entries=[], sender=self._state.current_node, term=self._state.term),
                        )
                        for to in self._state.peers
                    ]
                )
            except CancelledError:
                logger.debug(
                    "heartbeat loop cancelled",
                    node=self._state.current_node.uri,
                    term=self._state.term,
                    role=self._state.role,
                )
                raise

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
                logger.exception(e)
            finally:
                self.mailbox.task_done()

    async def handle_message(self, message: AddressedEvent[Command]) -> NodeState[Command]:
        """Handle raft messages"""
        pre_state = self._state.model_copy(deep=True)
        self._state, messages = handle(self._state, message.event)
        self._dispatcher.enqueue_messages([AddressedEvent(to=to, event=event) for to, event in messages])
        logger.debug(
            f"handled message: {type(message.event).__name__} ",
            node=self._state.current_node.uri,
            term=self._state.term,
            role=self._state.role,
        )

        await self._update_election_loops(pre_state, self._state, message.event)
        return self._state

    async def _update_election_loops(
        self, pre_state: NodeState[Command], post_state: NodeState[Command], event: Event[Command]
    ) -> None:
        """A node runs at most one timer at a time. Swap on role changes."""

        async def cancel_task(task: asyncio.Task[None] | None) -> None:
            """cancel the passed in task"""
            if task:
                task.cancel()
                await gather(task, return_exceptions=True)

        match (pre_state, post_state, event):
            case (NodeState(role=Role.Candidate), NodeState(role=Role.Leader), _):
                await cancel_task(self._election_task)
                self._heartbeat_task = create_task(self._heartbeat_loop())
            case (NodeState(role=Role.Leader), NodeState(role=Role.Follower) | NodeState(role=Role.Candidate), _):
                await cancel_task(self._heartbeat_task)
                self._election_task = create_task(self._election_loop())
            case (NodeState(role=Role.Follower) | NodeState(role=Role.Candidate), _, AppendEntries() as ae) if (
                ae.term >= pre_state.term
            ):
                await cancel_task(self._election_task)
                self._election_task = create_task(self._election_loop())
            case _ if pre_state.voted_for != post_state.voted_for:
                await cancel_task(self._election_task)
                self._election_task = create_task(self._election_loop())

    async def ask(self, message: AddressedEvent[Command]) -> NodeState[Command]:
        """Send a message to the actor and wait for a response."""
        future: Future[NodeState[Command]] = get_running_loop().create_future()
        await self.mailbox.put((message, future))
        return await future

    def tell(self, message: AddressedEvent[Command]) -> None:
        """Fire-and-forget messages to the actor"""
        self.mailbox.put_nowait((message, None))

    async def stop(self) -> None:
        """Stop the actor loop."""
        for task in [self._election_task, self._heartbeat_task, self._mailbox_task]:
            if task:
                task.cancel()
                await gather(task, return_exceptions=True)
