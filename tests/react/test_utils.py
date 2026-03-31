import asyncio

from react import utils


class DummyEmoji:
    def __init__(self, id: int):
        self.id = id


class DummyRole:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name


class DummyGuild:
    def __init__(self, id: int, emojis: list = None, roles: list = None):
        self.id = id
        self._emojis = {e.id: e for e in (emojis or [])}
        self.roles = roles or []

    def get_emoji(self, eid: int):
        return self._emojis.get(eid)

    def get_role(self, rid: int):
        for r in self.roles:
            if r.id == rid:
                return r
        return None


class DummyBot:
    def __init__(self, emojis: list = None):
        self._emojis = {e.id: e for e in (emojis or [])}

    def get_emoji(self, eid: int):
        return self._emojis.get(eid)


def _run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_validate_emoji_unicode_accepts():
    ok, reason = _run_async(utils.validate_emoji(None, None, "👍"))
    assert ok and reason is None


def test_validate_emoji_empty_rejects():
    ok, reason = _run_async(utils.validate_emoji(None, None, ""))
    assert not ok and reason == "emoji is empty"


def test_validate_emoji_custom_found_in_guild():
    eid = 123456789012345678
    token = f"<:foo:{eid}>"
    e = DummyEmoji(eid)
    guild = DummyGuild(1, emojis=[e])
    ok, reason = _run_async(utils.validate_emoji(None, guild, token))
    assert ok and reason is None


def test_validate_emoji_custom_found_in_bot():
    eid = 222222222222222222
    token = f"<:bar:{eid}>"
    e = DummyEmoji(eid)
    bot = DummyBot(emojis=[e])
    guild = DummyGuild(1, emojis=[])
    ok, reason = _run_async(utils.validate_emoji(bot, guild, token))
    assert ok and reason is None


def test_resolve_roles_from_guild_by_id_and_name():
    r1 = DummyRole(777777777777777777, "Admin")
    r2 = DummyRole(888888888888888888, "Mod")
    guild = DummyGuild(1, roles=[r1, r2])
    tokens = [f"<@&{r1.id}>", "Mod", "unknown", "999999999999999999"]
    resolved, unresolved = utils.resolve_roles_from_guild(guild, tokens)
    assert str(r1.id) in resolved
    assert str(r2.id) in resolved
    assert "unknown" in unresolved
    assert "999999999999999999" in unresolved
