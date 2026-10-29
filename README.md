# ironbird

Terminal integrated/system test tool for cFS flight software vehicles. Stage 1: CLI send and
receive over UDP. See `docs/plan.md` for the design and `CLAUDE.md` for project rules.

## Setup

```bash
pyenv virtualenv 3.11 ironbird      # or: python3.11 -m venv <dir>
pyenv activate ironbird
pip install -e '.[dev]'
```

## Try it without hardware (ironbird as both ends)

```bash
ib -c examples/mock.toml nodes
ib -c examples/mock.toml respond --as fc          # shell 1: act as the flight computer
ib -c examples/mock.toml listen fc                # shell 2: decode telemetry
ib -c examples/mock.toml send fc MOCK_NOOP        # shell 3
ib -c examples/mock.toml send fc MOCK_SET_MODE mode=SAFE pad=0 --verbose
ib -c examples/mock.toml find mode
```

Events (shell 2 instead of `listen`; the mock reports whatever event it is asked to raise):

```bash
ib -c examples/mock.toml events --min-type info
ib -c examples/mock.toml send fc MOCK_RAISE_EVENT app=DEMO event_id=7 event_type=ERROR "message=bus voltage low"
```

`ib` is a short alias of `ironbird`. Global options: `--config/-c PATH` (or `$IRONBIRD_CONFIG`),
`--as NODE`, `--verbose/-v`. Errors are one line, `error IB-XXX-NNN: summary`, on stderr; add
`--verbose` for detail. Every action and error is recorded as JSON Lines in
`$XDG_STATE_HOME/ironbird/` (override with `IRONBIRD_STATE_DIR`).

## Events and telemetry categories

cFS events are the most important telemetry. `ib events [--node X ...] [--min-type error]
[--format line|json]` listens on every telemetry link of all (or the selected) nodes and
prints only events: time, node, app, type, id, message. They also show in `ib listen`. The
ledger records every received event in full.

Telemetry messages carry a `category` in the CCDD, in priority order: `event`,
`command_response`, `housekeeping`, `status`, `commanded`, `other` (default). An event message
also has an `event` mapping naming its app/id/type/message fields; see `docs/plan.md`
("Telemetry categories and events") and `examples/ccdd/cfs.json`.

## Configuration

- `examples/ironbird.toml` - ground plus a real cFS flight computer (CI_LAB 1234, TO_LAB 2234).
- `examples/mock.toml` - ironbird-vs-ironbird on distinct ports, with response rules.
- `examples/headers/*.toml` - header profiles (bit widths, fixed/computed fields, checksum).
- `examples/ccdd/*.json` - placeholder CCDD JSON (`ironbird-ccdd/0`).

## Development

```bash
pytest -n auto
ruff check . && ruff format --check .
pyright
```

Real-cFS acceptance tests (opt-in, serial, need `tools/cfs/build.sh` first):

```bash
IRONBIRD_CFS=1 pytest tests/cfs -n0      # or: pytest tests/cfs -m cfs -n0
```

A plain `pytest` skips them. They start and stop cFS themselves and need UDP 1234 and 2234
and `/dev/shm/osal:RAM` free (one instance per host).

Test corpus of malformed inputs: `tests/corpus/malformed/`. Add a file there for every parser bug.
