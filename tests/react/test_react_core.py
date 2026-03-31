import pytest

from react.actions import ReactionAction
from react.react import React
from react.utils import build_types_embeds


class DummyEmoji:
    def __init__(self, id=None, name=None, raw=None):
        self.id = id
        self.name = name
        self._raw = raw

    def __str__(self):
        if self._raw is not None:
            return self._raw
        if self.id is not None:
            return f"<:{self.name}:{self.id}>"
        return self.name or ""


def test_emoji_key_matches_exact():
    em = DummyEmoji(None, "👍", "👍")
    assert React._emoji_key_matches(None, "👍", em)


def test_emoji_key_matches_custom_id():
    em = DummyEmoji(123456789012345678, "foo", "<:foo:123456789012345678>")
    key = "<:foo:123456789012345678>"
    assert React._emoji_key_matches(None, key, em)
    # also match when the stored key merely contains the id
    key2 = "prefix-123456789012345678-suffix"
    assert React._emoji_key_matches(None, key2, em)


def test_emoji_key_matches_name():
    em = DummyEmoji(None, "rocket", "🚀")
    assert React._emoji_key_matches(None, "rocket", em)


def test_emoji_key_matches_no_match():
    em = DummyEmoji(111, "a", "<:a:111>")
    assert not React._emoji_key_matches(None, "b", em)


def test_parse_bool_variants():
    assert ReactionAction._parse_bool(True) is True
    assert ReactionAction._parse_bool(False) is False
    assert ReactionAction._parse_bool("true") is True
    assert ReactionAction._parse_bool("False") is False
    assert ReactionAction._parse_bool("1") is True
    assert ReactionAction._parse_bool("0") is False
    assert ReactionAction._parse_bool(None, default=False) is False
    assert ReactionAction._parse_bool("unknown", default=False) is False


def test_normalize_role_ids_various_shapes():
    # None -> []
    a = ReactionAction.create("standard", {"roles": None})
    assert a._normalize_role_ids() == []

    # scalar int
    b = ReactionAction.create("standard", {"roles": 123})
    assert b._normalize_role_ids() == ["123"]

    # list/tuple/set
    c = ReactionAction.create("standard", {"roles": [1, "2", None]})
    assert c._normalize_role_ids() == ["1", "2"]
    d = ReactionAction.create("standard", {"roles": (3, 4)})
    assert d._normalize_role_ids() == ["3", "4"]
    e = ReactionAction.create("standard", {"roles": set([5, 6])})
    assert set(e._normalize_role_ids()) == {"5", "6"}

    # dict keys
    f = ReactionAction.create("standard", {"roles": {"7": "x", 8: "y"}})
    assert set(f._normalize_role_ids()) == {"7", "8"}


def test_validate_action_data_permutations():
    # not a mapping
    ok, reason = React._validate_action_data(None, "not-a-dict")
    assert not ok and "action_data is not a mapping" in reason

    # unknown action type
    ok, reason = React._validate_action_data(None, {"type": "nope"})
    assert not ok and "unknown action type 'nope'" in reason

    # scalar roles invalid
    ok, reason = React._validate_action_data(None, {"roles": "abc"})
    assert not ok and "roles scalar is not int-like" in reason

    # list roles invalid
    ok, reason = React._validate_action_data(None, {"roles": ["1", "two"]})
    assert not ok and "roles contain non-int-like values" in reason

    # dict role keys invalid
    ok, reason = React._validate_action_data(None, {"roles": {"one": "x"}})
    assert not ok and "roles dict keys are not int-like" in reason

    # duration invalid
    ok, reason = React._validate_action_data(None, {"duration": "3600x"})
    assert not ok and "duration is not int-like" in reason

    # group invalid
    ok, reason = React._validate_action_data(None, {"group": 1})
    assert not ok and "group must be a string" in reason

    # valid example
    ok, reason = React._validate_action_data(None, {"type": "standard", "roles": ["1", "2"], "duration": 60, "group": "g"})
    assert ok and reason == "ok"


def test_reactionaction_create_and_unknown():
    a = ReactionAction.create("standard", {"roles": [1]})
    assert isinstance(a, ReactionAction)

    with pytest.raises(KeyError):
        ReactionAction.create("no-such-type", {})


def test_build_types_embeds_basic():
    class A:
        description = "alpha"
        options = ["roles", "duration"]

    class B:
        __doc__ = "bravo docstring"
        options = []

    registry = {"a": A, "b": B}
    embeds = build_types_embeds(registry, title="Test Types")
    assert isinstance(embeds, list) and len(embeds) >= 1
    desc = "".join(e.description for e in embeds)
    assert "`a`" in desc and "`b`" in desc
    assert "Options" in desc
    assert "•" in desc
