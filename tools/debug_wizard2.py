import asyncio
import sys
from pathlib import Path
repo_root = str(Path(__file__).resolve().parents[1])
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from react.views import run_reactrole_wizard
from react.actions import ReactionAction
from react.react import React

# Dummy test helpers (copied minimal versions)
class DummyUser:
    def __init__(self, id: int):
        self.id = id

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
    def __init__(self, id: int):
        self.id = id

class DummyMessageFromUser:
    def __init__(self, content: str, author: DummyUser, channel: DummyChannel):
        self.content = content
        self.author = author
        self.channel = channel

class DummyCtx:
    def __init__(self, guild, author, channel, prefix='!'):
        self.guild = guild
        self.author = author
        self.channel = channel
        self.prefix = prefix
        self.sent = []
        self._last_view = None
    async def send(self, content=None, **kwargs):
        if 'view' in kwargs and kwargs.get('view') is not None:
            view = kwargs.get('view')
            self._last_view = view
            self.sent.append((content, kwargs))
            return DummyBotMessage(self.guild)
        self.sent.append((content, kwargs))
        return DummyBotMessage(self.guild)

class DummyBotMessage:
    def __init__(self, guild):
        self.guild = guild
    async def edit(self, **kwargs):
        return self
    async def delete(self):
        return

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
    def __init__(self, messages_queue):
        class _U:
            id = 999999999999999999
        self.user = _U()
        self._messages = messages_queue
    async def wait_for(self, event, check=None, timeout=None):
        while self._messages:
            m = self._messages.pop(0)
            if check is None or check(m):
                return m
        raise asyncio.TimeoutError()
    def get_emoji(self, eid: int):
        return None

async def main():
    guild_id = 111111111111111111
    message_id = 888888888888888888
    role_id = 777777777777777777

    cfg = _ConfigStub()

    class DummyGuild:
        def __init__(self, id, roles):
            self.id = id
            self.roles = roles
            self.text_channels = []
            self.me = None
        def get_role(self, rid):
            for r in self.roles:
                if r.id == rid:
                    return r
            return None

    roles = [DummyRole(role_id, "r")]
    guild = DummyGuild(guild_id, roles)

    author = DummyUser(555555555555555555)
    channel = DummyChannel(222222222222222222)
    ctx = DummyCtx(guild, author, channel, prefix='?')

    msgs = []
    msgs.append(DummyMessageFromUser(str(message_id), author, channel))
    msgs.append(DummyMessageFromUser("👍", author, channel))
    msgs.append(DummyMessageFromUser("standard", author, channel))
    msgs.append(DummyMessageFromUser(str(role_id), author, channel))

    bot = DummyBot(msgs)

    # Quick check of emoji validator
    from react.utils import validate_emoji
    ok, reason = await validate_emoji(bot, guild, "👍")
    print('validate_emoji ->', ok, reason)

    # Create a lightweight cog instance without running React.__init__
    cog = React.__new__(React)
    cog.bot = bot
    cog.config = cfg
    import types
    cog._validate_action_data = types.MethodType(React._validate_action_data, cog)
    cog._get_reaction_lock = lambda gid: asyncio.Lock()

    # Monitor to set confirmed when view is ready
    async def monitor_confirm():
        while ctx._last_view is None:
            await asyncio.sleep(0.01)
        view = ctx._last_view
        while not view.ready:
            await asyncio.sleep(0.01)
        print('DEBUG: monitor setting confirmed')
        view.confirmed = True
        view.stop()

    monitor = asyncio.create_task(monitor_confirm())
    result = await run_reactrole_wizard(cog, ctx)
    monitor.cancel()

    print('RESULT', result)
    print('CTX.SENT:')
    for s in ctx.sent:
        print(s)
    print('CONFIG STORE:', cfg._store)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
