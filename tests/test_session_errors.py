"""Routing and responder-rule errors a hand-edited config can produce, each a coded error that
says what to change, found before anything is opened."""

from __future__ import annotations

import asyncio
import copy
import tomllib
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird.config import SystemConfig, parse_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger
from ironbird.session import Session, Side, _validate_rules


def config_from(edit: Any = None) -> SystemConfig:
    data = tomllib.loads((EXAMPLES / "mock.toml").read_text())
    if edit is not None:
        edit(data)
    return parse_config(data, EXAMPLES / "mock.toml")


def session_of(config: SystemConfig, tmp_path: Path, role: str = "ground") -> Session:
    return Session(
        config, role=role, ledger=Ledger(tmp_path / role, session_id=role), notify=lambda _: None
    )


def add_second_link(data: dict[str, Any], *, same_telemetry_port: bool = False) -> None:
    second = copy.deepcopy(data["links"][0])
    second["name"] = "ground-fc-b"
    second["command_endpoint"] = "127.0.0.1:41240"
    second["telemetry_endpoint"] = (
        data["links"][0]["telemetry_endpoint"] if same_telemetry_port else "127.0.0.1:41241"
    )
    data["links"].append(second)


# -- routing --


def test_a_peer_cannot_be_the_node_ironbird_acts_as(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        session_of(config_from(), tmp_path).route("ground")
    assert info.value.code is Code.CFG_LINK and "name the other end" in info.value.summary


def test_a_named_link_that_does_not_exist_lists_the_ones_that_do(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        session_of(config_from(), tmp_path).route("fc", link_name="nope")
    assert info.value.code is Code.CFG_LINK and "named 'nope'" in info.value.summary
    assert "ground-fc" in info.value.details


def test_the_wrong_side_of_a_link_says_which_side_this_node_is(tmp_path: Path) -> None:
    ground = session_of(config_from(), tmp_path)
    with pytest.raises(IronbirdError) as info:
        ground.route("fc", require=Side.RESPONDER)
    assert info.value.code is Code.CFG_LINK and "--as fc" in info.value.summary
    with pytest.raises(IronbirdError) as info:
        session_of(config_from(), tmp_path, role="fc").route("ground", require=Side.COMMANDER)
    assert info.value.code is Code.CFG_LINK


def test_two_links_to_one_peer_need_a_choice_and_a_name_makes_it(tmp_path: Path) -> None:
    session = session_of(config_from(add_second_link), tmp_path)
    with pytest.raises(IronbirdError) as info:
        session.route("fc")
    assert info.value.code is Code.CFG_LINK and "--link" in info.value.summary
    assert session.route("fc", link_name="ground-fc-b").link.name == "ground-fc-b"


def test_a_node_without_definitions_cannot_be_decoded(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        session_of(config_from(), tmp_path).codec_for("ground")
    assert info.value.code is Code.CFG_REF and "ground" in info.value.summary


def test_recording_with_nothing_to_record_is_a_link_error(tmp_path: Path) -> None:
    session = session_of(config_from(), tmp_path)
    with pytest.raises(IronbirdError) as info:
        session.record_routes(link_name="nope")
    assert info.value.code is Code.CFG_LINK and "named 'nope'" in info.value.summary
    assert [r.link.name for r in session.record_routes()] == ["ground-fc"]
    assert [r.link.name for r in session.record_routes(["fc"], link_name="ground-fc")] == [
        "ground-fc"
    ]


def test_recording_two_links_that_receive_on_one_address_asks_for_one(tmp_path: Path) -> None:
    config = config_from(lambda d: add_second_link(d, same_telemetry_port=True))
    with pytest.raises(IronbirdError) as info:
        session_of(config, tmp_path).record_routes()
    assert info.value.code is Code.CFG_LINK and "select one with --link" in info.value.summary


# -- responder rules --


def with_rule(edit: Any) -> Any:
    def apply(data: dict[str, Any]) -> None:
        edit(data["nodes"]["fc"]["responses"])

    return apply


RULE_DEFECTS = [
    pytest.param(
        lambda rules: rules.append(copy.deepcopy(rules[0])),
        Code.CFG_SCHEMA,
        "duplicate rule",
        id="duplicate-rule",
    ),
    pytest.param(
        lambda rules: rules[0].update(on="NO_SUCH_COMMAND"),
        Code.CFG_REF,
        "is not a command",
        id="unknown-command",
    ),
    pytest.param(
        lambda rules: rules[0].update(on="MOCK_HK"),
        Code.CFG_REF,
        "is not a command",
        id="telemetry-named-as-command",
    ),
    pytest.param(
        lambda rules: rules[0].update(reply="NO_SUCH_REPLY"),
        Code.CFG_REF,
        "is not telemetry",
        id="unknown-reply",
    ),
    pytest.param(
        lambda rules: rules[0].update(reply="MOCK_NOOP"),
        Code.CFG_REF,
        "is not telemetry",
        id="command-named-as-reply",
    ),
    pytest.param(
        lambda rules: rules[0].update(accepted_counter="label"),
        Code.CFG_SCHEMA,
        "must be an integer field",
        id="counter-not-integer",
    ),
    pytest.param(
        lambda rules: rules[0]["echo"].update(ghost="mode"),
        Code.CFG_REF,
        "has no field 'ghost'",
        id="echo-into-missing-reply-field",
    ),
    pytest.param(
        lambda rules: rules[0]["echo"].update(mode="no_such_arg"),
        Code.CFG_REF,
        "has no argument",
        id="echo-from-missing-argument",
    ),
    pytest.param(
        lambda rules: rules[-1].update(echo={"mode": "mode"}),
        Code.CFG_SCHEMA,
        "not allowed on a '*' rule",
        id="echo-on-wildcard",
    ),
]


@pytest.mark.parametrize(("edit", "code", "needle"), RULE_DEFECTS)
def test_a_response_rule_that_does_not_fit_the_definitions_is_refused_up_front(
    tmp_path: Path, edit: Any, code: Code, needle: str
) -> None:
    try:
        config = config_from(with_rule(edit))
    except IronbirdError as exc:  # the config schema already caught it: still coded
        assert exc.code in (Code.CFG_SCHEMA, Code.CFG_REF)
        return
    session = session_of(config, tmp_path, role="fc")
    node = config.nodes["fc"]
    with pytest.raises(IronbirdError) as info:
        _validate_rules(node, session.codec_for("fc"), config)
    assert info.value.code is code and needle in info.value.summary


def test_the_shipped_rules_validate(tmp_path: Path) -> None:
    config = config_from()
    session = session_of(config, tmp_path, role="fc")
    rules = _validate_rules(config.nodes["fc"], session.codec_for("fc"), config)
    assert "*" in rules and "MOCK_SET_MODE" in rules


def test_responding_with_a_bad_rule_fails_before_a_socket_is_opened(tmp_path: Path) -> None:
    config = config_from(with_rule(lambda rules: rules[0].update(reply="NO_SUCH_REPLY")))

    async def main() -> None:
        async with session_of(config, tmp_path, role="fc") as fc:
            with pytest.raises(IronbirdError) as info:
                await fc.respond()
            assert info.value.code is Code.CFG_REF

    asyncio.run(asyncio.wait_for(main(), 20))


# -- events --


def test_events_from_a_node_whose_definitions_have_none_is_a_coded_error(tmp_path: Path) -> None:
    import json

    ccdd = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    ccdd["messages"] = [m for m in ccdd["messages"] if m.get("category") != "event"]
    ccdd_path = tmp_path / "no_events.json"
    ccdd_path.write_text(json.dumps(ccdd))

    def point_at_it(data: dict[str, Any]) -> None:
        data["nodes"]["fc"]["ccdd"] = str(ccdd_path)

    config = config_from(point_at_it)
    session = session_of(config, tmp_path)
    with pytest.raises(IronbirdError) as info:
        session.plan_events()
    assert info.value.code is Code.EVT_NONE
    assert session.event_routes() == []
