import asyncio
import time
import re

import pytest

import discord
from react.react import React


class DummyRole:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name
        self.managed = False
        self.position = 0

    @property
    def mention(self):
        return f"<@&{self.id}>"


class DummyChannel:
    def __init__(self, id: int, messages: list[int]):
        self.id = id
        self._messages = set(messages)

    async def fetch_message(self, message_id: int):
        if int(message_id) in self._messages:
            return object()
        raise discord.NotFound(None, "not found")


class DummyGuild:
    def __init__(self, id: int, roles: list[DummyRole], channels: list[DummyChannel] | None = None):
        self.id = id
        self.roles = roles
        self.me = None
        self.text_channels = channels or []

    def get_role(self, rid: int):
        for r in self.roles:
            if r.id == rid:
                return r
        return None


class DummyCtx:
    def __init__(self, guild, prefix="!"):
        self.guild = guild
        self.prefix = prefix
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append((content, kwargs))
        return content


class _Value:
    def __init__(self, parent, attr_name):
        self._parent = parent
        self._attr_name = attr_name

    async def __call__(self):
        return getattr(self._parent, self._attr_name)

    async def set(self, val):
        setattr(self._parent, self._attr_name, val)


class _ConfigGuildStub:
    def __init__(self, parent):
        self.parent = parent
        self.reactions = _Value(parent, "_reactions_store")
        self.timed_roles = _Value(parent, "_timed_store")


class _ConfigStub:
    def __init__(self):
        self._reactions_store = {}
        self._timed_store = {}
        self._guild = _ConfigGuildStub(self)

    def guild(self, guild):
        return self._guild


class DummyBot:
    def __init__(self):
        class _U:
            id = 555555555555555555

        self.user = _U()
        self.guilds = []

    async def wait_until_ready(self):
        return


def _make_emoji_key(style: str, eid: int = 333333333333333333):
    if style == "unicode":
        return "👍"
    if style == "custom":
        return f"<:foo:{eid}>"
    return str(eid)


@pytest.mark.parametrize("msg_style", ["id", "link"])
@pytest.mark.parametrize("emoji_style", ["unicode", "custom", "id"])
def test_reactrole_remove_specific(msg_style, emoji_style):
    async def run_case():
        guild_id = 111111111111111111
        channel_id = 222222222222222222
        message_id = 999999999999999999
        role_id = 444444444444444444

        msg_token = str(message_id) if msg_style == "id" else f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

        emoji_key = _make_emoji_key(emoji_style)
        emoji_token = emoji_key

        cfg = _ConfigStub()
        cfg._reactions_store[str(message_id)] = {emoji_key: {"type": "standard", "roles": [str(role_id)]}}

        cog = React(bot=DummyBot())
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        roles = [DummyRole(role_id, "r")]
        guild = DummyGuild(guild_id, roles)
        ctx = DummyCtx(guild, prefix="?")

        await cog.reactrole_remove.callback(cog, ctx, msg_token, emoji_token)

        # ensure mapping removed
        store = cfg._reactions_store
        msg_map = store.get(str(message_id), {})
        assert emoji_key not in msg_map
        assert any((isinstance(m[0], str) and "Mapping removed" in m[0]) or (m[1].get("embed") is not None) for m in ctx.sent)

    asyncio.get_event_loop().run_until_complete(run_case())


def test_reactrole_remove_all():
    async def run_case():
        guild_id = 1
        # use realistic snowflake-length id so command regex matches
        message_id = 999999999999999999
        cfg = _ConfigStub()
        cfg._reactions_store[str(message_id)] = {"a": {"type": "standard"}, "b": {"type": "standard"}}

        cog = React(bot=DummyBot())
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        guild = DummyGuild(guild_id, [])
        ctx = DummyCtx(guild)

        await cog.reactrole_remove.callback(cog, ctx, str(message_id))

        assert str(message_id) not in cfg._reactions_store
        assert any(isinstance(m[0], str) and "Mapping removed for message" in m[0] or isinstance(m[0], str) and "No mappings found" in m[0] for m in ctx.sent)

    asyncio.get_event_loop().run_until_complete(run_case())


def test_reactrole_list_and_embed_fields():
    async def run_case():
        guild_id = 10
        channel_id = 20
        # realistic message id
        message_id = 999999999999999998
        role_id = 40

        cfg = _ConfigStub()
        cfg._reactions_store[str(message_id)] = {"👍": {"type": "standard", "roles": [str(role_id)]}}

        cog = React(bot=DummyBot())
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        roles = [DummyRole(role_id, "r")]
        channel = DummyChannel(channel_id, [message_id])
        guild = DummyGuild(guild_id, roles, channels=[channel])
        ctx = DummyCtx(guild)

        await cog.reactrole_list.callback(cog, ctx)

        # there should be at least one embed sent
        found = False
        for content, kw in ctx.sent:
            embed = kw.get("embed")
            if embed is not None:
                found = True
                # embed should have a field for the mapping
                assert any("Roles:" in f.value for f in embed.fields)
        assert found

    asyncio.get_event_loop().run_until_complete(run_case())


def test_schedule_and_unschedule_timed_role():
    async def run_case():
        guild_id = 99
        user_id = 123
        role_id = 456

        cfg = _ConfigStub()
        cog = React(bot=DummyBot())
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        guild = DummyGuild(guild_id, [])

        await cog.schedule_timed_role(guild, user_id, role_id, 1)

        # verify timed store contains an entry for the user/role
        timed = cfg._timed_store
        assert str(user_id) in timed
        assert str(role_id) in timed[str(user_id)]

        # unschedule
        await cog.unschedule_timed_role(guild, user_id, role_id)
        timed2 = cfg._timed_store
        assert str(user_id) not in timed2 or str(role_id) not in timed2.get(str(user_id), {})

    asyncio.get_event_loop().run_until_complete(run_case())
