from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class Role(StrEnum):
    """A node is exactly one of these at a time."""

    Candidate = "Candidate"
    Follower = "Follower"
    Leader = "Leader"


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

    def is_current(self, their_term: int, their_index: int) -> bool:
        """allows comparing if another log instance is up to date with this one"""
        return their_term > self.last_term or (their_term == self.last_term and their_index >= self.last_index)

    @property
    def last_entry(self) -> LogEntry[Command] | None:
        """return the last entry in the log"""
        return self.entries[-1] if self.entries else None

    @property
    def last_index(self) -> int:
        """return the index of the last entry in the log"""
        return self.entries[-1].index if self.entries else 0

    @property
    def last_term(self) -> int:
        """return the term of the last entry in the log"""
        return self.entries[-1].term if self.entries else 0

    def truncate(self, conflict_index: int) -> Log[Command]:
        """remove any entry in the log prior to the given index"""
        return self.model_copy(
            update={"entries": tuple(entry for entry in self.entries if entry.index < conflict_index)}
        )

    def append(self, command: Command, term: int) -> Log[Command]:
        """the log is immutable, so copy and write"""
        index = self.entries[-1].index + 1 if self.entries else 1
        entry = LogEntry(command=command, term=term, index=index)
        return self.model_copy(update={"entries": (*self.entries, entry)})


class NodeState[Command](BaseModel):
    """An immutable Node state"""

    model_config = ConfigDict(frozen=True)
    term: int
    role: Role
    log: Log[Command]
    peers: frozenset[Node]
    current_node: Node
    voted_for: Node | None = None
    has_votes_from: frozenset[Node] = frozenset()
