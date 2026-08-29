from praetor.core.model import ElectionTimeout, Log, NodeState, Role, step


def test_state_is_unchanged_on_election_timeout() -> None:
    state = NodeState(
        term = 5,
        role = Role.Leader,
        voted_for = None,
        log = Log[int](),
        peers = []
    )

    new_state, _ = step(state, ElectionTimeout())
    assert state == new_state
