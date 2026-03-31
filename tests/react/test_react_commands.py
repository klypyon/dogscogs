import asyncio
import re
from itertools import product

import pytest

from react.actions import ReactionAction
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


class DummyGuild:
    def __init__(self, id: int, roles: list):
        self.id = id
        self.roles = roles

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


class _ReactionsValue:
    def __init__(self, parent):
        self._parent = parent

    async def __call__(self):
        return self._parent._store

    async def set(self, val):
        self._parent._store = val


class _ConfigGuildStub:
    def __init__(self, parent):
        self.reactions = _ReactionsValue(parent)


class _ConfigStub:
    def __init__(self):
        self._store = {}
        self._guild = _ConfigGuildStub(self)

    def guild(self, guild):
        return self._guild


def _make_emoji_token(style: str, eid: int = 333333333333333333):
    if style == "unicode":
        return "👍"
    if style == "custom":
        return f"<:foo:{eid}>"
    return str(eid)


def _make_role_token(style: str, rid: int, name: str):
    if style == "id":
        return str(rid)
    if style == "mention":
        return f"<@&{rid}>"
    return name


def _expected_role_list(style: str, rid: int):
    return [str(rid)]


def _run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class DummyBot:
    def __init__(self):
        class _U:
            id = 555555555555555555

        self.user = _U()
        self.guilds = []

    async def wait_until_ready(self):
        return


@pytest.mark.parametrize("msg_style", ["id", "link"])
@pytest.mark.parametrize("emoji_style", ["unicode", "custom", "id"])
@pytest.mark.parametrize("role_style", ["id", "mention", "name"])
@pytest.mark.parametrize("action_type", list(ReactionAction.registry.keys()))
def test_reactrole_add_permutations(msg_style, emoji_style, role_style, action_type):
    async def run_case():
        # IDs used in tests
        guild_id = 111111111111111111
        channel_id = 222222222222222222
        message_id = 999999999999999999
        role_id = 444444444444444444

        # build message token
        if msg_style == "id":
            msg_token = str(message_id)
        else:
            msg_token = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

        emoji_token = _make_emoji_token(emoji_style)

        role_name = "somerole"
        role_token = _make_role_token(role_style, role_id, role_name)

        # prepare cog and config stub
        cog = React(bot=DummyBot())
        cfg = _ConfigStub()
        cog.config = cfg

        # guild roles: include a named role for name-resolution tests
        roles = [DummyRole(role_id, role_name)]
        guild = DummyGuild(guild_id, roles)

        ctx = DummyCtx(guild, prefix="?")

        # prepare args: include option for timed/grouped types
        args = [role_token]
        if action_type == "timed":
            args.append("duration=60")
        if action_type == "grouped":
            args.append("group=color")

        # call command (invoke the underlying callback to avoid the
        # command wrapper re-ordering parameters)
        await cog.reactrole_add.callback(cog, ctx, msg_token, emoji_token, action_type, *args)

        # verify config stored
        store = cfg._store
        msg_key = str(message_id)
        assert msg_key in store, f"message {msg_key} missing in store for case {msg_style}/{emoji_style}/{role_style}/{action_type}"
        msg_map = store[msg_key]
        assert isinstance(msg_map, dict) and len(msg_map) == 1
        action_data = next(iter(msg_map.values()))
        assert action_data.get("type") == action_type

        # roles stored should match expected
        expected_roles = _expected_role_list(role_style, role_id)
        assert action_data.get("roles") == expected_roles

        # options checks
        if action_type == "timed":
            assert int(action_data.get("duration")) == 60
        if action_type == "grouped":
            assert action_data.get("group") == "color"

        # ensure a confirmation was sent
        assert any(isinstance(m[0], str) and "Mapping added successfully" in m[0] for m in ctx.sent)

    _run_async(run_case())
