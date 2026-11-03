"""``Frame`` identity: equal when the packet fields are equal, never equal to a non-frame, and
rebuilt from fields through the same checks as the constructor."""

from __future__ import annotations

import operator

import pytest

from ironbird.frame import Frame
from recutil import frame


def test_a_frame_equals_only_a_frame_with_the_same_packet_fields() -> None:
    a, same, other = frame(1), frame(1), frame(2)
    assert operator.eq(a, same) and not operator.ne(a, same)
    assert operator.ne(a, other) and not operator.eq(a, other)
    assert hash(a) == hash(same)
    for stranger in ("frame", 3, (1, 2), None, object()):
        assert operator.ne(a, stranger) and not operator.eq(a, stranger)


@pytest.mark.parametrize("count", [0, 1, 7, 10, 11])
def test_make_refuses_any_other_number_of_fields(count: int) -> None:
    fields = tuple(frame(5))[:8]
    with pytest.raises(TypeError):
        Frame._make((fields + (None,) * 3)[:count])


def test_make_accepts_both_legal_lengths() -> None:
    fields = tuple(frame(5))[:8]
    assert Frame._make(fields) == frame(5)
    assert Frame._make((*fields, None)) == frame(5)
