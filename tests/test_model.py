from praetor.core.model import Log


def test_logs_are_appended_correctly() -> None:
    log = Log[int]()
    for x in range(1, 11):
        log = log.append(x, term=1)

    assert len(log.entries) == 10
    assert log.last_entry is not None
    assert log.last_entry.command == 10


def test_logs_are_truncated_correctly() -> None:
    log = Log[str]()
    for x in range(1, 11):
        log = log.append(f"command{x}", term=1)

    log = log.truncate(5)
    assert log.last_entry is not None
    assert log.last_entry.command == "command4"
    assert log.last_entry.index == 4
    assert log.last_entry.term == 1
