from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class Role(StrEnum):
    """A node is exactly one of these at a time."""

    Candidate = 'Candidate'
    Follower = 'Follower'
    Leader = 'Leader'


class Node(BaseModel):
    """A cluster member. Stable key for peers and vote tracking."""

    model_config = ConfigDict(frozen=True)
    name: str
    uri: str


class LogEntry[Command](BaseModel):
    """A command, with the term and index it was created at."""

    model_config = ConfigDict(frozen=True)
    term: int
    command: Command
    index: int


class Log[Command](BaseModel):
    """An immutable log"""

    model_config = ConfigDict(frozen=True)
    entries: tuple[LogEntry[Command], ...] = ()

    @property
    def last_entry(self) -> LogEntry[Command] | None:
        """return the last entry in the log"""
        return self.entries[-1] if self.entries else None

    def truncate(self, conflict_index: int) -> Log[Command]:
        """remove any entry in the log prior to the given index"""
        return self.model_copy(
            update={
                "entries": tuple(entry for entry in self.entries if entry.index < conflict_index )
            }
        )

    def append(self, command: Command, term: int) -> Log[Command]:
        """the log is immutable, so copy and write"""
        index =  self.entries[-1].index + 1 if self.entries else 1
        entry = LogEntry(command=command, term=term, index=index)
        return self.model_copy(update={"entries": (*self.entries, entry)})



class NodeState[Command](BaseModel):
    """An immutable Node state"""

    model_config = ConfigDict(frozen=True)
    term: int
    role: Role
    voted_for: Node | None = None
    log: Log[Command]
    peers: list[Node]


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

class ElectionTimeout(BaseModel):
    """Signals that a leadership election has timed out"""
    model_config = ConfigDict(frozen=True)

type Event[Command] = RequestVote | RequestVoteReply | ElectionTimeout
type Message[Command] = RequestVote | RequestVoteReply



def step[Command](
        state: NodeState[Command],
        event: Event[Command]) -> tuple[NodeState[Command], list[Message[Command]]]:
    return state, []

