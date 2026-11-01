"""Stage 2k: the ``serial`` and ``framing`` tables of a link."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from conftest import CORPUS
from ironbird.config import (
    FramingSpec,
    SerialLinkSpec,
    SystemConfig,
    parse_config,
)
from ironbird.errors import Code, IronbirdError

BASE: dict[str, Any] = {
    "nodes": {
        "ground": {},
        "fc": {"ccdd": "c.json", "command_header": "c.toml", "telemetry_header": "t.toml"},
    },
    "links": [
        {
            "name": "uart",
            "from": "ground",
            "to": "fc",
            "transport": "serial",
            "serial": {"device": "/dev/ttyUSB0", "baud": 115200},
        }
    ],
}


def parse(edit: Any = None) -> SystemConfig:
    data = copy.deepcopy(BASE)
    if edit is not None:
        edit(data)
    return parse_config(data, Path("sys.toml"))


def test_a_serial_link_takes_defaults_for_the_line_and_the_framing() -> None:
    link = parse().links[0]
    assert link.transport == "serial" and link.udp is None
    serial, framing = link.serial, link.framing
    assert serial == SerialLinkSpec("/dev/ttyUSB0", 115200) and framing == FramingSpec()
    assert serial is not None and framing is not None
    assert (serial.data_bits, serial.parity, serial.stop_bits) == (8, "none", 1.0)
    assert framing.sync == b"" and framing.max_idle_ms == 50 and framing.require_known_mid
    assert link.command_target == link.telemetry_target == "/dev/ttyUSB0"


def test_every_setting_is_read() -> None:
    def edit(data: dict[str, Any]) -> None:
        data["links"][0]["serial"] = {
            "device": "/dev/serial/by-id/usb-FTDI_x-if00-port0",
            "baud": 12_000_000,
            "data_bits": 7,
            "parity": "space",
            "stop_bits": 1.5,
            "flow": "rtscts",
        }
        data["links"][0]["framing"] = {
            "kind": "header_length",
            "sync": "1ACFfc1D",
            "max_idle_ms": 10_000,
            "require_known_mid": False,
        }

    link = parse(edit).links[0]
    assert link.serial == SerialLinkSpec(
        "/dev/serial/by-id/usb-FTDI_x-if00-port0", 12_000_000, 7, "space", 1.5, "rtscts"
    )
    assert link.framing == FramingSpec("header_length", bytes.fromhex("1acffc1d"), 10_000, False)


def test_stop_bits_may_be_written_as_a_whole_number_or_a_float() -> None:
    for written, kept in ((1, 1.0), (2, 2.0), (1.0, 1.0), (2.0, 2.0), (1.5, 1.5)):
        link = parse(lambda d, w=written: d["links"][0]["serial"].update(stop_bits=w)).links[0]
        assert link.serial is not None and link.serial.stop_bits == kept


def test_bits_per_character_counts_start_data_parity_and_stop_bits() -> None:
    assert SerialLinkSpec("/d", 9600).bits_per_char == 10
    assert SerialLinkSpec("/d", 9600, 7, "even", 2.0).bits_per_char == 11


def test_a_serial_link_needs_no_udp_endpoints_and_a_udp_link_no_serial_table() -> None:
    assert parse().links[0].udp is None


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda d: d["links"][0]["serial"].update(device="ttyUSB0"), "absolute device path"),
        (lambda d: d["links"][0]["serial"].update(device="/dev/tty\nX"), "control characters"),
        (lambda d: d["links"][0]["serial"].update(device="/" + "d" * 1030), "absolute device path"),
        (lambda d: d["links"][0]["serial"].pop("device"), "serial.device: required"),
        (lambda d: d["links"][0]["serial"].pop("baud"), "serial.baud: required"),
        (lambda d: d["links"][0]["serial"].update(baud=49), "integer in 50..12000000"),
        (lambda d: d["links"][0]["serial"].update(baud=12_000_001), "integer in 50..12000000"),
        (lambda d: d["links"][0]["serial"].update(baud=True), "integer in 50..12000000"),
        (lambda d: d["links"][0]["serial"].update(baud=9600.0), "integer in 50..12000000"),
        (lambda d: d["links"][0]["serial"].update(data_bits=4), "one of 5, 6, 7, 8"),
        (lambda d: d["links"][0]["serial"].update(data_bits=True), "one of 5, 6, 7, 8"),
        (lambda d: d["links"][0]["serial"].update(parity="mixed"), "one of none, even, odd"),
        (lambda d: d["links"][0]["serial"].update(parity=1), "one of none, even, odd"),
        (lambda d: d["links"][0]["serial"].update(stop_bits=3), "one of 1, 1.5, 2"),
        (lambda d: d["links"][0]["serial"].update(stop_bits=True), "one of 1, 1.5, 2"),
        (lambda d: d["links"][0]["serial"].update(stop_bits="1"), "one of 1, 1.5, 2"),
        (lambda d: d["links"][0]["serial"].update(flow="dtrdsr"), "one of none, rtscts, xonxoff"),
        (lambda d: d["links"][0]["serial"].update(parity_bit="n"), "serial.parity_bit: unknown"),
        (lambda d: d["links"][0].update(serial="/dev/ttyS0"), "serial: must be a table"),
        (lambda d: d["links"][0].pop("serial"), "serial: required key is missing"),
        (lambda d: d["links"][0].update(framing={"sync": "1AC"}), "framing.sync: must be hex"),
        (lambda d: d["links"][0].update(framing={"sync": "0x1ACF"}), "framing.sync: must be hex"),
        (lambda d: d["links"][0].update(framing={"sync": "zz"}), "framing.sync: must be hex"),
        (lambda d: d["links"][0].update(framing={"sync": "00" * 9}), "framing.sync: must be hex"),
        (lambda d: d["links"][0].update(framing={"sync": 5}), "framing.sync: must be hex"),
        (lambda d: d["links"][0].update(framing={"max_idle_ms": 0}), "integer in 1..10000"),
        (lambda d: d["links"][0].update(framing={"max_idle_ms": 10_001}), "integer in 1..10000"),
        (lambda d: d["links"][0].update(framing={"kind": "slip"}), "framing.kind: must be one of"),
        (lambda d: d["links"][0].update(framing={"require_known_mid": 1}), "true or false"),
        (lambda d: d["links"][0].update(framing={"window": 3}), "framing.window: unknown"),
        (lambda d: d["links"][0].update(framing="header_length"), "framing: must be a table"),
        (lambda d: d["links"][0].update(command_endpoint="h:1"), "command_endpoint: unknown"),
    ],
)
def test_serial_settings_outside_what_the_transport_can_apply_are_refused(
    edit: Any, needle: str
) -> None:
    with pytest.raises(IronbirdError) as info:
        parse(edit)
    assert info.value.code is Code.CFG_SCHEMA
    assert info.value.summary.startswith("sys.toml: links[0].")
    assert needle in info.value.summary


def test_a_udp_link_cannot_carry_a_serial_table() -> None:
    def edit(data: dict[str, Any]) -> None:
        data["links"][0] = {
            "from": "ground",
            "to": "fc",
            "command_endpoint": "h:1",
            "telemetry_endpoint": "h:2",
            "serial": {"device": "/dev/ttyS0", "baud": 9600},
        }

    with pytest.raises(IronbirdError) as info:
        parse(edit)
    assert info.value.code is Code.CFG_SCHEMA and "serial: unknown key" in info.value.summary


def test_one_device_carries_one_link() -> None:
    def edit(data: dict[str, Any]) -> None:
        data["nodes"]["ground"] = BASE["nodes"]["fc"]
        data["links"].append({**data["links"][0], "name": "again", "from": "fc", "to": "ground"})

    with pytest.raises(IronbirdError) as info:
        parse(edit)
    assert info.value.code is Code.CFG_SCHEMA
    assert "already the device of link 'uart'" in info.value.summary


@pytest.mark.parametrize(
    "spelling",
    ["/dev//ttyUSB0", "/dev/./ttyUSB0", "/dev/ttyUSB0/", "//dev/ttyUSB0", "/dev/x/../ttyUSB0"],
)
def test_two_spellings_of_one_device_path_are_one_device(spelling: str) -> None:
    def edit(data: dict[str, Any]) -> None:
        data["nodes"]["ground"] = BASE["nodes"]["fc"]
        other = copy.deepcopy(data["links"][0])
        other.update(name="again", **{"from": "fc", "to": "ground"})
        other["serial"]["device"] = spelling
        data["links"].append(other)

    with pytest.raises(IronbirdError) as info:
        parse(edit)
    assert info.value.code is Code.CFG_SCHEMA
    assert "already the device of link 'uart'" in info.value.summary


def test_different_devices_are_not_taken_for_one() -> None:
    def edit(data: dict[str, Any]) -> None:
        data["nodes"]["ground"] = BASE["nodes"]["fc"]
        other = copy.deepcopy(data["links"][0])
        other.update(name="again", **{"from": "fc", "to": "ground"})
        other["serial"]["device"] = "/dev/ttyUSB10"
        data["links"].append(other)

    assert len(parse(edit).links) == 2


def idle_edit(baud: int, framing: dict[str, Any] | None, **line: Any) -> Any:
    def edit(data: dict[str, Any]) -> None:
        data["links"][0]["serial"].update(baud=baud, **line)
        if framing is not None:
            data["links"][0]["framing"] = framing

    return edit


@pytest.mark.parametrize(
    ("baud", "line", "floor"),
    [
        (110, {}, 364),  # 4 x 10 bits at 110 baud
        (300, {}, 134),
        (9600, {}, 5),  # 4.17 ms rounded up
        (115_200, {}, 1),
        (9600, {"data_bits": 7, "parity": "even", "stop_bits": 2.0}, 5),  # 11 bits a character
        (50, {"data_bits": 5, "parity": "odd", "stop_bits": 2.0}, 720),
    ],
)
def test_the_least_safe_idle_time_is_four_character_times(
    baud: int, line: dict[str, Any], floor: int
) -> None:
    serial = SerialLinkSpec("/dev/ttyUSB0", baud, **line)
    assert serial.min_idle_ms == floor
    parse(idle_edit(baud, {"max_idle_ms": floor}, **line))  # exactly the floor is accepted
    if floor > 1:
        with pytest.raises(IronbirdError):
            parse(idle_edit(baud, {"max_idle_ms": floor - 1}, **line))


def test_an_idle_time_below_four_character_times_is_refused_naming_both_keys() -> None:
    with pytest.raises(IronbirdError) as info:
        parse(idle_edit(9600, {"max_idle_ms": 1}))
    error = info.value
    assert error.code is Code.CFG_SCHEMA
    assert "links[0].framing.max_idle_ms" in error.summary
    assert "links[0].serial.baud" in error.summary and "at least 5 ms" in error.summary
    with pytest.raises(IronbirdError) as info:
        parse(idle_edit(110, {"max_idle_ms": 50}))  # the default value, set explicitly
    assert "at least 364 ms" in info.value.summary


@pytest.mark.parametrize("framing", [None, {}, {"sync": "1ACF"}, {"require_known_mid": False}])
def test_an_idle_time_left_out_follows_the_baud_rate_when_the_default_is_too_short(
    framing: dict[str, Any] | None,
) -> None:
    slow = parse(idle_edit(110, framing)).links[0].framing
    assert slow is not None and slow.max_idle_ms == 364
    fast = parse(idle_edit(115_200, framing)).links[0].framing
    assert fast is not None and fast.max_idle_ms == 50  # the default stands when it is safe
    assert parse(idle_edit(300, framing)).links[0].framing.max_idle_ms == 134  # type: ignore[union-attr]


# Each checked-in serial config is refused for the reason its name says (tests/test_malformed_input
# only checks that the code is a configuration one).
CORPUS_REASONS = {
    "cfg-serial-device-relative": "absolute device path",
    "cfg-serial-device-newline": "control characters",
    "cfg-serial-device-missing": "serial.device: required",
    "cfg-serial-baud-zero": "integer in 50..12000000",
    "cfg-serial-baud-above-range": "integer in 50..12000000",
    "cfg-serial-baud-string": "integer in 50..12000000",
    "cfg-serial-baud-bool": "integer in 50..12000000",
    "cfg-serial-parity-unknown": "one of none, even, odd",
    "cfg-serial-stop-bits-three": "one of 1, 1.5, 2",
    "cfg-serial-data-bits-nine": "one of 5, 6, 7, 8",
    "cfg-serial-flow-unknown": "one of none, rtscts, xonxoff",
    "cfg-serial-unknown-key": "serial.parity_bit: unknown",
    "cfg-serial-not-a-table": "serial: must be a table",
    "cfg-serial-framing-sync-odd": "framing.sync: must be hex",
    "cfg-serial-framing-sync-prefix": "framing.sync: must be hex",
    "cfg-serial-framing-sync-too-long": "framing.sync: must be hex",
    "cfg-serial-framing-idle-zero": "integer in 1..10000",
    "cfg-serial-framing-idle-huge": "integer in 1..10000",
    "cfg-serial-framing-kind-unknown": "framing.kind: must be one of",
    "cfg-serial-framing-known-mid-string": "true or false",
    "cfg-serial-with-udp-endpoint": "command_endpoint: unknown",
    "cfg-udp-with-serial-table": "serial: unknown key",
    "cfg-serial-device-twice": "already the device of link",
    "cfg-serial-device-twice-spelled": "already the device of link",
    "cfg-serial-framing-idle-below-baud": "4 character times",
}


@pytest.mark.parametrize("name", sorted(CORPUS_REASONS))
def test_corpus_serial_configs_are_refused_for_their_reason(name: str) -> None:
    from ironbird.config import load_config

    with pytest.raises(IronbirdError) as info:
        load_config(CORPUS / f"{name}.toml")
    assert info.value.code is Code.CFG_SCHEMA
    assert CORPUS_REASONS[name] in info.value.summary


def test_every_serial_corpus_config_has_a_reason_here() -> None:
    names = {p.stem for p in CORPUS.glob("cfg-serial-*.toml")} | {
        p.stem for p in CORPUS.glob("cfg-udp-with-serial-*.toml")
    }
    assert names == set(CORPUS_REASONS)
