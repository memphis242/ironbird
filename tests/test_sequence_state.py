"""Sequence counts that outlive the process: ``sequence-state.json``, ``ib send --sequence``.

Real ``ib`` processes against a real ``ib respond``; the store is also driven directly for the
cases a command line cannot reach (a lost lock, a burst, a crash). No assertion is made on the
wording of a message, only on its code.
"""

from __future__ import annotations

import errno
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS, EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
from ironbird.errors import Code, IronbirdError
from ironbird.header import load_profile
from ironbird.seqstate import (
    BLOCK,
    LOCK_NAME,
    MAX_BYTES,
    MAX_ENTRIES,
    MAX_KEY_LEN,
    STATE_NAME,
    SequenceStore,
    key_of,
    node_stem,
    parse_key,
    parse_state,
    render_state,
)
from support.faultfs import FaultFs

KEY = "fc/command/0x1890"


class _Reports:
    def __init__(self) -> None:
        self.errors: list[IronbirdError] = []
        self.events: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, error: IronbirdError) -> None:
        self.errors.append(error)

    def record(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))

    @property
    def codes(self) -> list[Code]:
        return [e.code for e in self.errors]


def _store(
    directory: Path, reports: _Reports | None = None, *, block: int = BLOCK, fs: Any = None
) -> SequenceStore:
    seen = reports or _Reports()
    extra = {"fs": fs} if fs is not None else {}
    return SequenceStore(directory, report=seen, record=seen.record, block=block, **extra)


def _marks(directory: Path) -> dict[str, int]:
    marks = parse_state((directory / STATE_NAME).read_bytes())
    assert marks is not None
    return marks


# -- the store ---------------------------------------------------------------------------


def test_counts_continue_across_stores_when_closed_cleanly(tmp_path: Path) -> None:
    first = _store(tmp_path)
    assert [first.take(KEY) for _ in range(3)] == [0, 1, 2]
    first.close()
    second = _store(tmp_path)
    assert second.take(KEY) == 3
    second.close()
    assert _marks(tmp_path)[KEY] == 4


def test_a_crash_skips_counts_but_never_reuses_one(tmp_path: Path) -> None:
    crashed = _store(tmp_path)
    used = [crashed.take(KEY) for _ in range(3)]  # never closed: the unused rest stays reserved
    after = _store(tmp_path)
    nxt = after.take(KEY)
    assert nxt not in used and nxt > max(used)
    assert nxt == BLOCK


def test_the_mark_is_durable_before_the_first_count_of_a_block_is_used(tmp_path: Path) -> None:
    store = _store(tmp_path, block=8)
    store.take(KEY)
    assert _marks(tmp_path)[KEY] == 8  # the whole block is already on disk


def test_a_burst_costs_one_sync_per_block_not_per_packet(tmp_path: Path) -> None:
    fs = FaultFs()
    store = _store(tmp_path, fs=fs)
    taken = [store.take(KEY) for _ in range(10 * BLOCK)]
    assert taken == list(range(10 * BLOCK))
    assert fs.count("fdatasync") == 10  # the temporary's data, once per reservation


def test_a_reservation_that_follows_another_process_leaves_a_gap_not_a_clash(
    tmp_path: Path,
) -> None:
    one, two = _store(tmp_path, block=4), _store(tmp_path, block=4)
    assert one.take(KEY) == 0
    assert two.take(KEY) == 4  # reserved after one's block
    one.close()  # not the newest reservation: its unused counts stay reserved
    two.close()  # the newest: gives back 5..7
    assert _store(tmp_path, block=4).take(KEY) == 5


def test_seat_makes_the_next_count_one_more(tmp_path: Path) -> None:
    store = _store(tmp_path)
    assert store.take(KEY) == 0
    store.seat(KEY, 100)
    assert store.take(KEY) == 101
    store.close()
    assert _store(tmp_path).take(KEY) == 102


def test_keys_are_independent_and_bounded(tmp_path: Path) -> None:
    store = _store(tmp_path, block=1)
    for i in range(MAX_ENTRIES + 40):
        store.take(f"n{i}/command/0x0001")
    raw = (tmp_path / STATE_NAME).read_bytes()
    marks = parse_state(raw)
    assert marks is not None and len(marks) == MAX_ENTRIES and len(raw) <= MAX_BYTES
    assert f"n{MAX_ENTRIES + 39}/command/0x0001" in marks  # the newest stay
    assert "n0/command/0x0001" not in marks


def test_long_and_odd_node_names_get_distinct_bounded_keys() -> None:
    a, b = key_of("x" * 300, "command", 5), key_of("x" * 299 + "y", "command", 5)
    assert a != b and len(a) <= MAX_KEY_LEN and len(b) <= MAX_KEY_LEN
    assert key_of("fc", "command", 5) != key_of("fc", "telemetry", 5)
    # a name of characters that JSON escapes long still fits the file
    marks = {key_of("\U0001f680" * 200 + str(i), "command", 1): i for i in range(MAX_ENTRIES)}
    assert len(render_state(marks)) <= MAX_BYTES


_WORKER = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from ironbird.seqstate import SequenceStore
    def refuse(error):
        raise SystemExit(f"unexpected warning: {error}")
    store = SequenceStore(Path(sys.argv[1]), report=refuse, block=int(sys.argv[3]))
    taken = [store.take("fc/command/0x1890") for _ in range(int(sys.argv[2]))]
    if sys.argv[4] == "close":
        store.close()
    print(" ".join(map(str, taken)))
    """
)


@pytest.mark.parametrize("ending", ["close", "crash"])
def test_parallel_processes_never_hand_out_the_same_count(tmp_path: Path, ending: str) -> None:
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, str(tmp_path), "60", "4", ending],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(8)
    ]
    taken: list[int] = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        series = [int(n) for n in out.split()]
        assert series == sorted(series)  # one process counts upward
        taken += series
    assert len(taken) == 8 * 60 and len(set(taken)) == len(taken)


def test_unusable_state_directory_warns_once_and_counts_in_memory(tmp_path: Path) -> None:
    fs = FaultFs()
    fs.fail("open_at", errno.EROFS, when=lambda _fd, name, *_a: name == LOCK_NAME)
    reports = _Reports()
    store = _store(tmp_path, reports, fs=fs)
    assert [store.take(KEY) for _ in range(200)] == list(range(200))
    store.seat(KEY, 5)
    assert store.take(KEY) == 6
    store.close()
    assert reports.codes == [Code.SEQ_UNSAVED]
    assert reports.codes[0].is_warning
    assert [e for e, _ in reports.events if e == "sequence.memory_only"]
    assert not (tmp_path / STATE_NAME).exists()


def test_a_lock_held_too_long_falls_back_instead_of_hanging(tmp_path: Path) -> None:
    import fcntl

    holder = open(tmp_path / LOCK_NAME, "wb")  # noqa: SIM115 - held for the whole test
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        reports = _Reports()
        store = SequenceStore(tmp_path, report=reports, sleep=lambda _s: None)
        assert store.take(KEY) == 0
        assert reports.codes == [Code.SEQ_UNSAVED]
    finally:
        holder.close()


def test_no_file_locks_never_means_unlocked_counting(tmp_path: Path) -> None:
    fs = FaultFs()
    fs.fail("flock", errno.ENOLCK)
    reports = _Reports()
    store = _store(tmp_path, reports, fs=fs)
    assert store.take(KEY) == 0
    assert reports.codes == [Code.SEQ_UNSAVED] and not store.persistent


def test_failure_while_writing_falls_back_and_keeps_the_old_file(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.take(KEY)
    before = (tmp_path / STATE_NAME).read_bytes()
    fs = FaultFs()
    fs.fail("fdatasync", errno.EIO)
    reports = _Reports()
    other = _store(tmp_path, reports, fs=fs, block=8)
    assert other.take("other/command/0x0002") == 0  # in memory from here
    assert reports.codes == [Code.SEQ_UNSAVED]
    assert (tmp_path / STATE_NAME).read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([STATE_NAME, LOCK_NAME])


# -- the file is untrusted ---------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(CORPUS.glob("seqstate-*.json")), ids=lambda p: p.stem)
def test_corpus_state_files_are_refused_by_the_parser(path: Path) -> None:
    assert parse_state(path.read_bytes()) is None


@pytest.mark.parametrize("path", sorted(CORPUS.glob("seqstate-*.json")), ids=lambda p: p.stem)
def test_corpus_state_files_warn_and_reset_without_crashing(path: Path, tmp_path: Path) -> None:
    (tmp_path / STATE_NAME).write_bytes(path.read_bytes())
    reports = _Reports()
    store = _store(tmp_path, reports)
    assert [store.take(KEY) for _ in range(3)] == [0, 1, 2]
    store.close()
    assert reports.codes == [Code.SEQ_RESET]
    assert all("\n" not in e.user_line() for e in reports.errors)
    assert _marks(tmp_path)[KEY] == 3  # replaced by a valid file


def test_a_forged_file_that_is_a_symbolic_link_or_a_directory_is_not_followed(
    tmp_path: Path,
) -> None:
    target = tmp_path / "elsewhere"
    target.write_text('{"v":1,"next":{"fc/command/0x1890":7}}\n')
    (tmp_path / STATE_NAME).symlink_to(target)
    reports = _Reports()
    store = _store(tmp_path, reports)
    assert store.take(KEY) == 0  # the link's content is not read
    assert Code.SEQ_RESET in reports.codes
    assert target.read_text() == '{"v":1,"next":{"fc/command/0x1890":7}}\n'  # and not written


@settings(max_examples=200, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(st.binary(max_size=2000))
def test_parse_state_never_raises_and_what_it_accepts_round_trips(data: bytes) -> None:
    marks = parse_state(data)
    if marks is not None:
        assert parse_state(render_state(marks)) == marks


@settings(max_examples=100, deadline=1000)
@given(
    st.dictionaries(
        st.text(min_size=1, max_size=MAX_KEY_LEN), st.integers(0, 2**64 - 1), max_size=MAX_ENTRIES
    )
)
def test_any_valid_marks_render_within_the_size_cap(marks: dict[str, int]) -> None:
    raw = render_state(marks)
    assert len(raw) <= MAX_BYTES
    kept = parse_state(raw)
    assert kept is not None and all(marks[k] == v for k, v in kept.items())


# -- the codec ---------------------------------------------------------------------------


def _codec(tmp_path: Path, store: SequenceStore | None = None) -> Codec:
    codec = Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )
    if store is not None:
        codec.use_sequence_source(store.for_node("fc"))
    return codec


def test_counts_wrap_at_the_width_of_the_sequence_field(tmp_path: Path) -> None:
    store = _store(tmp_path)
    codec = _codec(tmp_path, store)
    assert codec.encode_command("MOCK_NOOP", sequence=16382).sequence == 16382
    assert [codec.encode_command("MOCK_NOOP").sequence for _ in range(3)] == [16383, 0, 1]
    store.close()
    again = _codec(tmp_path, _store(tmp_path))  # a later process goes on past the wrap
    assert again.encode_command("MOCK_NOOP").sequence == 2


def test_set_sequence_is_validated_against_the_field_width(tmp_path: Path) -> None:
    codec = _codec(tmp_path, _store(tmp_path))
    for bad in (-1, 16384, 2**70):
        with pytest.raises(IronbirdError) as info:
            codec.encode_command("MOCK_NOOP", sequence=bad)
        assert info.value.code is Code.ENC_ARG
    assert not (tmp_path / STATE_NAME).exists()  # a refusal takes no count
    assert codec.encode_command("MOCK_NOOP", sequence=16383).sequence == 16383


def test_set_sequence_is_refused_when_the_profile_has_no_sequence_field(tmp_path: Path) -> None:
    text = (EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml").read_text()
    assert 'name = "sequence"\nbits = 14\ncomputed = "sequence"' in text
    plain = tmp_path / "nosequence.toml"
    plain.write_text(
        text.replace(
            'name = "sequence"\nbits = 14\ncomputed = "sequence"',
            'name = "sequence"\nbits = 14\nvalue = 0',
        )
    )
    codec = Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(plain),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    )
    codec.use_sequence_source(_store(tmp_path).for_node("fc"))
    assert codec.encode_command("MOCK_NOOP").sequence is None
    with pytest.raises(IronbirdError) as info:
        codec.encode_command("MOCK_NOOP", sequence=1)
    assert info.value.code is Code.ENC_ARG
    assert not (tmp_path / STATE_NAME).exists()  # no sequence field, no count kept


def test_without_a_source_counting_is_in_memory_from_zero(tmp_path: Path) -> None:
    codec = _codec(tmp_path)
    assert [codec.encode_command("MOCK_NOOP").sequence for _ in range(2)] == [0, 1]
    assert codec.encode_command("MOCK_NOOP", sequence=9).sequence == 9
    assert codec.encode_command("MOCK_NOOP").sequence == 10


# -- real processes ----------------------------------------------------------------------


def _ib(config: Path, *args: str, timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(config), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _seq(done: subprocess.CompletedProcess[str]) -> int:
    assert done.returncode == 0, done.stderr
    return int(done.stdout.split(" seq ")[1].split(",")[0].split(")")[0].split()[0])


@pytest.fixture
def responder(mock_config_path: Path) -> Any:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ironbird.cli.main",
            "-c",
            str(mock_config_path),
            "respond",
            "--as",
            "fc",
            "--timeout",
            "60",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stderr is not None
    for _ in range(200):
        line = proc.stderr.readline()
        if "responding" in line:
            break
        assert line or proc.poll() is None, "respond exited"
    yield proc
    proc.kill()
    proc.communicate()


def _tx_sequences(state: Path) -> list[int]:
    out: list[int] = []
    for path in sorted(state.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event.get("event") == "tx.command":
                out.append(event["sequence"])
    return out


def test_two_consecutive_sends_count_zero_then_one(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    """Each `ib send` is its own process; the count used to restart at 0 in every one."""
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 1
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_SET_MODE", "mode=SAFE", "pad=0")) == 2
    state = tmp_path / "state"
    assert sorted(_tx_sequences(state)) == [0, 1, 2]  # the ledger has the count used
    assert _marks(state)["fc/command/0x1890"] == 3  # nothing reserved is left over


def test_send_sequence_sets_one_packet_and_counts_on_from_it(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", "100")) == 100
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 101
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", "0x3fff")) == 16383
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0  # wrapped at 14 bits
    assert sorted(_tx_sequences(tmp_path / "state")) == [0, 100, 101, 16383]


@pytest.mark.parametrize("value", ["16384", "-1", "abc", "1.5", "\N{ARABIC-INDIC DIGIT ONE}"])
def test_send_sequence_refuses_what_does_not_fit_and_sends_nothing(
    mock_config_path: Path, responder: Any, tmp_path: Path, value: str
) -> None:
    done = _ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", value)
    assert done.returncode == 2 and not done.stdout
    assert "error IB-" in done.stderr
    assert _tx_sequences(tmp_path / "state") == []


def test_parallel_sends_get_distinct_counts(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ironbird.cli.main",
                "-c",
                str(mock_config_path),
                "send",
                "fc",
                "MOCK_NOOP",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(8)
    ]
    counts: list[int] = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        counts.append(int(out.split(" seq ")[1].split(",")[0].split()[0]))
    assert len(set(counts)) == 8
    assert sorted(_tx_sequences(tmp_path / "state")) == sorted(counts)


@pytest.mark.parametrize("path", sorted(CORPUS.glob("seqstate-*.json")), ids=lambda p: p.stem)
def test_a_damaged_state_file_warns_with_a_code_and_the_send_goes_out(
    mock_config_path: Path, responder: Any, tmp_path: Path, path: Path
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    (state / STATE_NAME).write_bytes(path.read_bytes())
    first = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(first) == 0
    assert f"warning {Code.SEQ_RESET.value}" in first.stderr
    assert len([ln for ln in first.stderr.splitlines() if Code.SEQ_RESET.value in ln]) == 1
    second = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(second) == 1 and Code.SEQ_RESET.value not in second.stderr  # repaired
    reset = [
        json.loads(line)
        for p in state.glob("*.jsonl")
        for line in p.read_text().splitlines()
        if '"sequence.reset"' in line
    ]
    assert len(reset) == 1  # in the ledger too


def test_an_unusable_state_directory_warns_once_and_counts_in_memory(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    (state / LOCK_NAME).mkdir()  # the lock cannot be opened, as in a read-only directory
    first = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(first) == 0 and f"warning {Code.SEQ_UNSAVED.value}" in first.stderr
    assert first.stderr.count(Code.SEQ_UNSAVED.value) == 1
    second = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(second) == 0  # restarts at 0 in every process, as warned
    assert not (state / STATE_NAME).exists()
    assert any(
        '"sequence.memory_only"' in line
        for p in state.glob("*.jsonl")
        for line in p.read_text().splitlines()
    )


# -- ib seq show / ib seq reset ----------------------------------------------------------


def _seed(state: Path) -> None:
    """Stored counts for two nodes, as sends and a responder would leave them."""
    store = _store(state, block=1)
    for node, kind, mid in (
        ("fc", "command", 0x1890),
        ("fc", "command", 0x1891),
        ("fc", "telemetry", 0x0890),
        ("ground", "command", 0x1890),
    ):
        store.take(key_of(node, kind, mid))
        store.take(key_of(node, kind, mid))


def _stored_keys(state: Path) -> set[str]:
    return set(_marks(state))


def test_parse_key_reads_what_key_of_writes() -> None:
    for node in ("fc", "a/b", "x" * 300):
        assert parse_key(key_of(node, "telemetry", 0x890)) == (node_stem(node), "telemetry", 0x890)
    for foreign in ("", "fc", "fc/command", "fc/other/0x1", "/command/0x1", "fc/command/0xzz"):
        assert parse_key(foreign) is None


def test_forget_selects_by_node_and_mid_and_everything_wipes_foreign_keys(tmp_path: Path) -> None:
    _seed(tmp_path)
    store = _store(tmp_path)
    assert [r[:3] for r in store.forget("fc", 0x1891)] == [("fc", "command", 0x1891)]
    assert len(store.forget("fc")) == 2
    assert _stored_keys(tmp_path) == {key_of("ground", "command", 0x1890)}
    (tmp_path / STATE_NAME).write_text('{"v":1,"next":{"foreign":5,"ground/command/0x1890":2}}\n')
    assert len(store.forget("ground")) == 1 and _stored_keys(tmp_path) == {"foreign"}
    assert store.forget() == []  # nothing of ours left; the foreign key goes too
    assert _stored_keys(tmp_path) == set()


def test_forgetting_heals_an_invalid_file(tmp_path: Path) -> None:
    (tmp_path / STATE_NAME).write_text("garbage")
    reports = _Reports()
    assert _store(tmp_path, reports).forget() == []
    assert reports.codes == [Code.SEQ_RESET] and _marks(tmp_path) == {}


def test_seq_reset_needs_to_be_told_what_to_reset(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    _seed(state)
    before = _marks(state)
    for args in (
        ["seq", "reset"],
        ["seq", "reset", "--all", "fc"],
        ["seq", "reset", "--all", "fc", "1"],
    ):
        done = _ib(mock_config_path, *args)
        assert done.returncode == 2 and not done.stdout
    assert _marks(state) == before


def test_seq_reset_forms(mock_config_path: Path, responder: Any, tmp_path: Path) -> None:
    state = tmp_path / "state"
    _seed(state)
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 2  # counts on from the seed
    one = _ib(mock_config_path, "seq", "reset", "fc", "0x1891")
    assert one.returncode == 0 and _stored_keys(state) >= {key_of("fc", "command", 0x1890)}
    assert key_of("fc", "command", 0x1891) not in _stored_keys(state)
    named = _ib(mock_config_path, "seq", "reset", "fc", "MOCK_NOOP")  # a message name
    assert named.returncode == 0 and key_of("fc", "command", 0x1890) not in _stored_keys(state)
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    node = _ib(mock_config_path, "seq", "reset", "fc")
    assert node.returncode == 0
    assert not [k for k in _stored_keys(state) if k.startswith("fc/")]
    assert key_of("ground", "command", 0x1890) in _stored_keys(state)  # the other node stays
    everything = _ib(mock_config_path, "seq", "reset", "--all")
    assert everything.returncode == 0 and _stored_keys(state) == set()
    again = _ib(mock_config_path, "seq", "reset", "--all")  # nothing left: not an error
    assert again.returncode == 0
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    ledgered = [
        e
        for p in state.glob("*.jsonl")
        for e in map(json.loads, p.read_text().splitlines())
        if e.get("event") == "seq.reset" and e.get("kind") == "action"
    ]
    assert len(ledgered) == 5
    assert any(r["removed"] and r["removed"][0]["kind"] == "command" for r in ledgered)


def test_seq_reset_refuses_an_unknown_node_or_mid(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    _seed(state)
    before = _marks(state)
    unknown = _ib(mock_config_path, "seq", "reset", "nowhere")
    assert unknown.returncode == 2 and Code.CFG_REF.value in unknown.stderr
    bad_mid = _ib(mock_config_path, "seq", "reset", "fc", "NO_SUCH_MESSAGE")
    assert bad_mid.returncode == 2 and Code.CLI_USAGE.value in bad_mid.stderr
    assert _marks(state) == before


def test_seq_show_lists_the_stored_counts(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    assert "no stored" in _ib(mock_config_path, "seq", "show").stdout
    _seed(state)
    everything = _ib(mock_config_path, "seq", "show")
    assert everything.returncode == 0 and len(everything.stdout.splitlines()) == 1 + 4
    only_fc = _ib(mock_config_path, "seq", "show", "fc")
    assert len(only_fc.stdout.splitlines()) == 1 + 3
    assert _ib(mock_config_path, "seq", "show", "nowhere").returncode == 2
