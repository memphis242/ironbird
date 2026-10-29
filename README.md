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

`ib` is a short alias of `ironbird`. Global options: `--config/-c PATH` (or `$IRONBIRD_CONFIG`),
`--as NODE`, `--verbose/-v`. Errors are one line, `error IB-XXX-NNN: summary`, on stderr; add
`--verbose` for detail. Every action and error is recorded as JSON Lines in
`$XDG_STATE_HOME/ironbird/` (override with `IRONBIRD_STATE_DIR`).

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
