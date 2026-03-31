import asyncio
import re

import pytest

from react.react import React
from react.views import ReactRoleListView, ReactRoleWizardView, WizardState


class DummyUser:
    def __init__(self, id: int, manage_roles: bool = False):
        self.id = id
        class GP:
            pass
        self.guild_permissions = GP()
        self.guild_permissions.manage_roles = manage_roles


class DummyGuild:
    def __init__(self, id: int, roles=None):
        self.id = id
        self.roles = roles or []
        self.text_channels = []
        self.me = None

    def get_role(self, rid: int):
        for r in self.roles:
            if r.id == rid:
                return r
        return None


class DummyChannel:
    def __init__(self, id: int):
        self.id = id


class DummyCtx:
    def __init__(self, guild, author, channel, prefix="!"):
        self.guild = guild
        self.author = author
        self.channel = channel
        self.prefix = prefix
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append((content, kwargs))
        return None


class DummyResponse:
    def __init__(self):
        self.sent = []
        self.edited = False

    async def send_message(self, *args, **kwargs):
        self.sent.append((args, kwargs))

    async def edit_message(self, *args, **kwargs):
        self.edited = True


class DummyInteraction:
    def __init__(self, user, ctx=None):
        self.user = user
        self._ctx = ctx
        self.response = DummyResponse()
        self.followup = None


def _make_grouped(n: int):
    # Create a grouped mapping with `n` entries under one rendered emoji
    mappings = [(f"raw{i}", {"type": "standard"}) for i in range(n)]
    return {"👍": mappings}


def test_listview_pagination_next_prev():
    async def run_case():
        guild = DummyGuild(1)
        author = DummyUser(42)
        ctx = DummyCtx(guild, author, DummyChannel(2))

        grouped = _make_grouped(60)
        view = ReactRoleListView(cog=None, ctx=ctx, msg_id="1", grouped=grouped)

        # Expect 3 pages for 60 items (25 per page)
        assert len(view.pages) == 3

        # find next and prev buttons
        next_btn = None
        prev_btn = None
        for child in view.children:
            if getattr(child, 'label', None) == ">":
                next_btn = child
            if getattr(child, 'label', None) == "<":
                prev_btn = child

        assert next_btn is not None and prev_btn is not None

        # simulate pressing next as the author
        interaction = DummyInteraction(author)
        await next_btn.callback(interaction)
        assert view.page_idx == 1

        # mapping select options updated
        sel = None
        for child in view.children:
            from discord.ui import Select
            if isinstance(child, view.MappingSelect):
                sel = child
                break
        assert sel is not None
        assert len(sel.options) == 25

        # go to last page
        await next_btn.callback(interaction)
        assert view.page_idx == 2

        # go back
        await prev_btn.callback(interaction)
        assert view.page_idx == 1

    asyncio.get_event_loop().run_until_complete(run_case())


def test_mapping_select_unauthorized():
    async def run_case():
        guild = DummyGuild(1)
        author = DummyUser(100)
        ctx = DummyCtx(guild, author, DummyChannel(2))

        grouped = _make_grouped(5)
        view = ReactRoleListView(cog=None, ctx=ctx, msg_id="1", grouped=grouped)

        # get the mapping select and simulate a non-author user without perms
        sel = None
        for child in view.children:
            if isinstance(child, view.MappingSelect):
                sel = child
                break
        assert sel is not None
        # simulate chosen value
        # discord Select exposes `.values` as a read-only property; set the
        # underlying backing field used by the Select implementation instead.
        try:
            sel._values = [sel.options[0].value]
        except Exception:
            # fallback if implementation differs
            setattr(sel, "_values", [sel.options[0].value])

        intr = DummyInteraction(DummyUser(999, manage_roles=False))
        await sel.callback(intr)

        # parent.selected should remain None and a response was sent
        assert view.selected is None
        assert len(intr.response.sent) >= 1

    asyncio.get_event_loop().run_until_complete(run_case())


def test_undo_add_view_removes_mapping():
    async def run_case():
        # Minimal config stub with reactions and message_channels
        class _ReactionsValue:
            def __init__(self, parent):
                self._parent = parent

            async def __call__(self):
                return self._parent._store

            async def set(self, val):
                self._parent._store = val

        class _MessageChannelsValue:
            def __init__(self, parent):
                self._parent = parent

            async def __call__(self):
                return self._parent._mcstore

            async def set(self, val):
                self._parent._mcstore = val

        class _ConfigGuildStub:
            def __init__(self, parent):
                self.reactions = _ReactionsValue(parent)
                self.message_channels = _MessageChannelsValue(parent)

        class _ConfigStub:
            def __init__(self):
                self._store = {}
                self._mcstore = {}
                self._guild = _ConfigGuildStub(self)

            def guild(self, guild):
                return self._guild

        # prepare cog and cfg
        cfg = _ConfigStub()
        msg_id = "555"
        emoji_key = "<:foo:123456789012345678>"
        cfg._store = {msg_id: {emoji_key: {"type": "standard"}}}
        cfg._mcstore = {msg_id: 999}

        class DummyBot:
            def __init__(self):
                class _U:
                    id = 1
                self.user = _U()

        cog = React(bot=DummyBot())
        cog.config = cfg
        try:
            cog._timed_role_cleanup.cancel()
        except Exception:
            pass

        class Guild:
            def __init__(self):
                self.id = 10

        guild = Guild()
        author = DummyUser(42)
        ctx = DummyCtx(guild, author, DummyChannel(2))

        # Create UndoAddView and interaction as the author
        from react.react import UndoAddView
        view = UndoAddView(cog, ctx, msg_id, emoji_key, timeout=1.0)
        intr = DummyInteraction(author, ctx)

        # Perform undo
        await view.undo_button.callback(intr)

        # mapping should be removed
        assert msg_id not in cfg._store
        # message_channels mapping removed
        assert msg_id not in cfg._mcstore

    asyncio.get_event_loop().run_until_complete(run_case())
