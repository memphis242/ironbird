"""A test model whose enum fields take any value of their type, for tests that draw or zero-fill
command arguments and so need to send values no enum names (a CCDD field says so with
``"allow_raw": true``)."""

from __future__ import annotations

from dataclasses import replace

from ironbird.ccdd.model import CcddModel


def allow_raw_enums(model: CcddModel) -> CcddModel:
    messages = tuple(
        replace(
            m,
            fields=tuple(replace(f, allow_raw=True) if f.enum is not None else f for f in m.fields),
        )
        for m in model.messages
    )
    return CcddModel.build(model.target, model.source, messages)
