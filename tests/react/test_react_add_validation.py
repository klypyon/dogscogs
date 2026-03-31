import asyncio
import re

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


class DummyBot:
    def __init__(self):
        class _U:
            id = 555555555555555555

        self.user = _U()
        self.guilds = []

    async def wait_until_ready(self):
        return


def _run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_reactrole_add_positional_duration():
    async def run_case():
        guild_id = 111111111111111111
        message_id = 222222222222222222
        role_id = 333333333333333333

        msg_token = str(message_id)
        emoji_token = "👍"
        role_token = str(role_id)

        cog = React(bot=DummyBot())
        cfg = _ConfigStub()
        cog.config = cfg

        roles = [DummyRole(role_id, "r")]
        guild = DummyGuild(guild_id, roles)
        ctx = DummyCtx(guild, prefix="?")

        # positional: role_id then duration -> duration should be mapped
        await cog.reactrole_add.callback(cog, ctx, msg_token, emoji_token, "timed", role_token, "60")

        store = cfg._store
        assert str(message_id) in store
        msg_map = store[str(message_id)]
        action_data = next(iter(msg_map.values()))
        assert action_data.get("type") == "timed"
        assert int(action_data.get("duration")) == 60
        assert action_data.get("roles") == [str(role_id)]

    _run_async(run_case())


def test_reactrole_add_flag_style_duration():
    async def run_case():
        guild_id = 111111111111111111
        message_id = 444444444444444444
        role_id = 555555555555555555

        msg_token = str(message_id)
        emoji_token = "🔥"
        role_token = str(role_id)

        cog = React(bot=DummyBot())
        cfg = _ConfigStub()
        cog.config = cfg

        roles = [DummyRole(role_id, "r")]
        guild = DummyGuild(guild_id, roles)
        ctx = DummyCtx(guild, prefix="?")

        # flag-style: --duration 120 should be accepted
        await cog.reactrole_add.callback(cog, ctx, msg_token, emoji_token, "timed", role_token, "--duration", "120")

        store = cfg._store
        assert str(message_id) in store
        msg_map = store[str(message_id)]
        action_data = next(iter(msg_map.values()))
        assert action_data.get("type") == "timed"
        assert int(action_data.get("duration")) == 120
        assert action_data.get("roles") == [str(role_id)]

    _run_async(run_case())


def test_reactrole_add_missing_required_option_grouped():
    async def run_case():
        guild_id = 666666666666666666
        message_id = 777777777777777777
        role_id = 888888888888888888

        msg_token = str(message_id)
        emoji_token = "<:foo:333333333333333333>"
        role_token = str(role_id)

        cog = React(bot=DummyBot())
        cfg = _ConfigStub()
        cog.config = cfg

        roles = [DummyRole(role_id, "r")]
        guild = DummyGuild(guild_id, roles)
        ctx = DummyCtx(guild, prefix="?")

        # grouped requires `group` option; omitting it should fail
        await cog.reactrole_add.callback(cog, ctx, msg_token, emoji_token, "grouped", role_token)

        # config should not contain the mapping
        assert str(message_id) not in cfg._store

        # user should be informed about missing options
        assert any(isinstance(m[0], str) and "Missing required options" in m[0] for m in ctx.sent)

    _run_async(run_case())
