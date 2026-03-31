import asyncio
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from react.react import React
from react.actions import ReactionAction

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
        print("SENT:", content)
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

async def run_case():
    guild_id = 111111111111111111
    channel_id = 222222222222222222
    message_id = 999999999999999999
    role_id = 444444444444444444

    msg_token = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    emoji_token = str(333333333333333333)
    role_name = "somerole"
    role_token = role_name

    cog = React(bot=DummyBot())
    cfg = _ConfigStub()
    cog.config = cfg

    roles = [DummyRole(role_id, role_name)]
    guild = DummyGuild(guild_id, roles)
    ctx = DummyCtx(guild, prefix="?")

    args = [role_token]
    args.append("group=color")

    print("Before call, store:", cfg._store)
    await cog.reactrole_add.callback(cog, ctx, msg_token, emoji_token, 'grouped', *args)
    print("After call, store:", cfg._store)
    print("Ctx.sent:")
    for m in ctx.sent:
        print(m)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(run_case())
