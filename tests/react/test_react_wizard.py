import asyncio
import re

from typing import Any

import pytest

from react.react import React
from react.views import run_reactrole_wizard, ReactRoleWizardView, WizardState
from react.actions import ReactionAction


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


class DummyFetchedMessage:
    def __init__(self, content: str = ""):
        self.content = content
        self.embeds = []


class DummyCtx:
    def __init__(self, guild: Any, author: DummyUser, channel: Any, prefix: str = "!"):
        self.guild = guild
        self.author = author
        self.channel = channel
        self.prefix = prefix
        self.sent = []
        self._last_view = None

    async def send(self, content=None, **kwargs):
        # If an embed + view were provided, capture the view so tests can
        # interact with it. Return a dummy message that the view will use
        # as `message_ref`.
        if "view" in kwargs and kwargs.get("view") is not None:
            view = kwargs.get("view")
            self._last_view = view
            msg = DummyBotMessage(self.guild)
            self.sent.append((content, kwargs))
            return msg

        # Generic sends: record and return a simple marker
        self.sent.append((content, kwargs))
        return DummyBotMessage(self.guild)


class DummyBotMessage:
    def __init__(self, guild: Any):
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
        # Return next queued message that satisfies `check`
        while self._messages:
            m = self._messages.pop(0)
            if check is None or check(m):
                return m
        raise asyncio.TimeoutError()

    def get_emoji(self, eid: int):
        return None
        return None


class DummyInteraction:
    class DummyResponse:
        def __init__(self):
            self.sent = []

        async def send_message(self, *args, **kwargs):
            self.sent.append((args, kwargs))

        async def edit_message(self, *args, **kwargs):
            self.sent.append(("edit", args, kwargs))

    class DummyFollowup:
        def __init__(self):
            self.sent = []

        async def send(self, *args, **kwargs):
            self.sent.append((args, kwargs))
            return DummyBotMessage(None)

    async def original_response(self):
        return DummyBotMessage(None)

    def __init__(self, user_id, ctx):
        self.user = DummyUser(user_id)
        self._ctx = ctx
        self.response = DummyInteraction.DummyResponse()
        self.followup = DummyInteraction.DummyFollowup()


class DummyMessageFromUser:
    def __init__(self, content: str, author: DummyUser, channel: Any):
        self.content = content
        self.author = author
        self.channel = channel


@pytest.mark.parametrize("action_type", ["standard", "timed", "grouped"])
def test_wizard_flow(action_type):
    async def run_case():
        # ids
        guild_id = 111111111111111111
        message_id = 888888888888888888
        role_id = 777777777777777777

        # prepare config and cog
        cfg = _ConfigStub()
        # ensure registry has the tested action
        assert action_type in ReactionAction.registry

        # Build guild with a role that can be resolved
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

        # prepare ctx
        author = DummyUser(555555555555555555)
        channel = DummyChannel(222222222222222222)
        ctx = DummyCtx(guild, author, channel, prefix="?")

        # prepare messages queue for the bot.wait_for
        msgs = []
        # Step 1: message id
        msgs.append(DummyMessageFromUser(str(message_id), author, channel))
        # Step 2: emoji (unicode for simplicity)
        msgs.append(DummyMessageFromUser("👍", author, channel))
        # Step 3: action type selection
        msgs.append(DummyMessageFromUser(action_type, author, channel))

        # Additional prompts depending on action type
        if action_type == "timed":
            # roles prompt (roles come first now)
            msgs.append(DummyMessageFromUser(str(role_id), author, channel))
            # duration prompt
            msgs.append(DummyMessageFromUser("60", author, channel))
            # remove_on_unreact prompt (skip)
            msgs.append(DummyMessageFromUser("none", author, channel))
        elif action_type == "grouped":
            # roles prompt (roles first)
            msgs.append(DummyMessageFromUser(str(role_id), author, channel))
            # group prompt
            msgs.append(DummyMessageFromUser("color", author, channel))
        else:
            # standard: roles prompt
            msgs.append(DummyMessageFromUser(str(role_id), author, channel))

        # create the dummy bot with the queued messages
        bot = DummyBot(msgs)

        cog = React(bot=bot)
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        # start monitor task: waits for view to become ready then confirms
        async def monitor_confirm():
            # wait for view to be created by the first ctx.send
            while ctx._last_view is None:
                await asyncio.sleep(0.01)
            view = ctx._last_view
            # wait for view.ready (the wizard will set this when done)
            while not view.ready:
                await asyncio.sleep(0.01)
            # simulate pressing confirm
            view.confirmed = True
            view.stop()

        monitor = asyncio.create_task(monitor_confirm())

        # inject the bot into cog and run the wizard
        result = await run_reactrole_wizard(cog, ctx)

        # cancel monitor
        monitor.cancel()

        assert result is True
        # verify config saved
        store = cfg._store
        key = str(message_id)
        assert key in store
        # ensure an entry exists for the emoji used
        assert "👍" in store[key]
        assert store[key]["👍"]["type"] == action_type or (action_type == "standard" and store[key]["👍"]["type"] == "standard")

    asyncio.get_event_loop().run_until_complete(run_case())


def test_view_types_button_sends_embeds():
    async def run_case():
        author = DummyUser(555555555555555553)
        state = WizardState()
        view = ReactRoleWizardView(author.id, state, ReactionAction)
        interaction = DummyInteraction(author.id, None)
        btn = view.types_button
        await btn.callback(interaction)
        assert len(interaction.response.sent) >= 1

    asyncio.get_event_loop().run_until_complete(run_case())


def test_wizard_cancel_during_emoji_prompt():
    async def run_case():
        guild_id = 111111111111111111
        message_id = 888888888888888886
        role_id = 777777777777777775

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
        author = DummyUser(555555555555555552)
        channel = DummyChannel(222222222222222220)
        ctx = DummyCtx(guild, author, channel, prefix="?")

        # only provide the message id; we'll inject a dummy reply to
        # unblock the emoji wait when we press Cancel
        msgs = [DummyMessageFromUser(str(message_id), author, channel)]
        bot = DummyBot(msgs)

        cog = React(bot=bot)
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        interaction = DummyInteraction(author.id, ctx)

        async def monitor_cancel():
            while ctx._last_view is None:
                await asyncio.sleep(0.01)
            view = ctx._last_view
            # press Cancel after Types has not been pressed; inject dummy reply
            bot._messages.append(DummyMessageFromUser("ignored", author, channel))
            btn = view.cancel_button
            await btn.callback(interaction)

        monitor = asyncio.create_task(monitor_cancel())
        result = await run_reactrole_wizard(cog, ctx)
        monitor.cancel()

        assert result is False

    asyncio.get_event_loop().run_until_complete(run_case())
