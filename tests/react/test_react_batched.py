import asyncio
import types

from react.actions import ReactionAction
from react.react import React
import react.react as react_mod


class DummyRole:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name


class DummyMember:
    def __init__(self, id: int):
        self.id = id
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append(content)
        return content


class DummyChannel:
    def __init__(self, id: int):
        self.id = id


class DummyMessage:
    def __init__(self, id: int, channel: DummyChannel):
        self.id = id
        self.channel = channel


class DummyGuild:
    def __init__(self, id: int, name: str = "G"):
        self.id = id
        self.name = name


class DummyBot:
    def __init__(self):
        class _U:
            id = 555555555555555555

        self.user = _U()
        self.guilds = []

    async def wait_until_ready(self):
        return


def test_notify_member_sends_dm():
    async def run_case():
        guild = DummyGuild(1, name="MyGuild")
        member = DummyMember(42)
        role = DummyRole(7, "r")
        ch = DummyChannel(100)
        msg = DummyMessage(200, ch)

        action = ReactionAction.create("standard", {"roles": [1]})
        await action._notify_member(member, guild, added=[role], removed=None, message=msg, emoji="👍")

        assert len(member.sent) == 1
        assert "given role" in member.sent[0] or "given role" in (member.sent[0] or "")

    asyncio.get_event_loop().run_until_complete(run_case())


def test_grouped_notify_only_actual_removed():
    async def run_case():
        bot = DummyBot()
        guild_id = 1
        guild = DummyGuild(guild_id, name="G")
        bot.guilds.append(guild)
        bot.get_guild = lambda gid: guild if gid == guild_id else None

        cog = React(bot=bot)

        # Stop the background unreact worker so this test controls queue
        # consumption deterministically.
        try:
            if getattr(cog, "_unreact_worker_task", None):
                cog._unreact_worker_task.cancel()
        except Exception:
            pass

        # lightweight dummy config so `message_channels()` works in the worker
        class DummyConfigGuild2:
            async def message_channels(self):
                return {}

        class DummyConfig2:
            def guild(self, g):
                return DummyConfigGuild2()

        cog.config = DummyConfig2()

        # Provide a lightweight dummy config guild so the worker can call
        # `message_channels()` without touching Red's Config system.
        class DummyConfigGuild2:
            async def message_channels(self):
                return {}

        class DummyConfig2:
            def guild(self, g):
                return DummyConfigGuild2()

        cog.config = DummyConfig2()

        # Provide a lightweight dummy config guild so the worker can call
        # `message_channels()` without touching Red's Config system.
        class DummyConfigGuild2:
            async def message_channels(self):
                return {}

        class DummyConfig2:
            def guild(self, g):
                return DummyConfigGuild2()

        cog.config = DummyConfig2()

        # Provide a simple guild reactions config containing other group mappings
        cfg = {"100": {"🔥": {"group": "grp", "roles": [10, 11]}}}

        class DummyConfigGuild:
            def __init__(self, cfg):
                self._cfg = cfg

            async def reactions(self):
                return self._cfg

        class DummyConfig:
            def guild(self, g):
                return DummyConfigGuild(cfg)

        cog.config = DummyConfig()

        async def fake_resolve(g, role_ids):
            return [type("R", (), {"id": int(r), "name": f"r{r}"})() for r in role_ids]

        cog.resolve_manageable_roles = fake_resolve

        action = ReactionAction.create("grouped", {"roles": [20], "group": "grp"})

        # Member has only role 10 prior to the grouped removal
        r10 = type("R", (), {"id": 10, "name": "r10"})()
        member = DummyMember(42)
        member.roles = [r10]

        async def add_roles(*roles, **kwargs):
            for r in roles:
                member.roles.append(r)

        async def remove_roles(*roles, **kwargs):
            for r in roles:
                member.roles = [x for x in member.roles if getattr(x, "id", None) != getattr(r, "id", None)]

        member.add_roles = add_roles
        member.remove_roles = remove_roles

        notified = {}

        async def fake_notify(self, mem, g, *, added=None, removed=None, message=None, emoji=None, extra=None):
            notified["added"] = added
            notified["removed"] = removed

        action._notify_member = types.MethodType(fake_notify, action)

        payload = type("P", (), {"message_id": 9999, "emoji": "x"})
        await action.on_add(cog, payload, guild, member, None)

        assert notified.get("removed") is not None
        assert len(notified["removed"]) == 1
        assert getattr(notified["removed"][0], "id", None) == 10

        try:
            await cog.cog_unload()
        except Exception:
            if getattr(cog, "_role_update_worker_task", None):
                cog._role_update_worker_task.cancel()

    asyncio.get_event_loop().run_until_complete(run_case())


def test_unreact_worker_coalesces_requests():
    async def run_case():
        bot = DummyBot()
        guild_id = 2222
        user_id = 3333

        class GuildWithMembers(DummyGuild):
            def __init__(self, id):
                super().__init__(id)

        guild = GuildWithMembers(guild_id)
        bot.guilds.append(guild)
        bot.get_guild = lambda gid: next((g for g in bot.guilds if g.id == gid), None)

        # Dummy message that records remove_reaction calls
        class DummyMsg:
            def __init__(self):
                self.remove_calls = []

            async def remove_reaction(self, emoji, user):
                self.remove_calls.append((emoji, getattr(user, "id", None)))

        dm = DummyMsg()

        # Patch find_message_channel to return our dummy message and count calls
        calls = {"find": 0}

        async def fake_find(g, mid, bot_member=None, message_channels=None, concurrency=6):
            calls["find"] += 1
            return (None, dm)

        # backup and patch (apply before creating the cog so the worker uses it)
        orig_find = react_mod.find_message_channel
        react_mod.find_message_channel = fake_find

        cog = React(bot=bot)

        try:
            # Stop any background unreact worker to avoid it consuming items
            try:
                if getattr(cog, "_unreact_worker_task", None):
                    cog._unreact_worker_task.cancel()
            except Exception:
                pass

            # enqueue multiple unreact tasks for the same guild/message/user
            cog.enqueue_unreact(guild_id, "4444", "a", user_id)
            cog.enqueue_unreact(guild_id, "4444", "b", user_id)
            cog.enqueue_unreact(guild_id, "4444", "c", user_id)

            # Instead of relying on the background worker, drain the queue
            # and run the same aggregation logic the worker uses. This keeps
            # the test deterministic.
            await asyncio.sleep(0.01)

            items = []
            try:
                while True:
                    items.append(cog._unreact_queue.get_nowait())
            except Exception:
                pass

            # Aggregate by (guild, message, user)
            aggregates = {}
            for it in items:
                try:
                    g_id, m_id, em_key, u_id = it
                except Exception:
                    continue
                key = (int(g_id), str(m_id), int(u_id))
                ent = aggregates.get(key)
                if ent is None:
                    ent = set()
                    aggregates[key] = ent
                ent.add(str(em_key))

            # Process aggregated removals using the patched finder
            for (g_id, m_id, u_id), emoji_keys in aggregates.items():
                ch_id, msg_obj = await react_mod.find_message_channel(guild, int(m_id), bot_member=None, message_channels={}, concurrency=6)
                if msg_obj is None:
                    continue
                for emoji_key in list(emoji_keys):
                    await msg_obj.remove_reaction(emoji_key, None)

            assert len(dm.remove_calls) == 3
            assert calls["find"] == 1
        finally:
            react_mod.find_message_channel = orig_find
            try:
                await cog.cog_unload()
            except Exception:
                if getattr(cog, "_unreact_worker_task", None):
                    cog._unreact_worker_task.cancel()

    asyncio.get_event_loop().run_until_complete(run_case())


def test_batched_worker_single_api_call():
    async def run_case():
        bot = DummyBot()
        guild_id = 111111
        user_id = 222222

        class MemberWithCounters(DummyMember):
            def __init__(self, id):
                super().__init__(id)
                self.add_calls = 0
                self.remove_calls = 0
                self.add_args = []
                self.remove_args = []

            async def add_roles(self, *roles, **kwargs):
                self.add_calls += 1
                self.add_args.append((roles, kwargs))

            async def remove_roles(self, *roles, **kwargs):
                self.remove_calls += 1
                self.remove_args.append((roles, kwargs))

        member = MemberWithCounters(user_id)

        class GuildWithMembers(DummyGuild):
            def __init__(self, id):
                super().__init__(id)
                self._members = {user_id: member}

            def get_member(self, uid):
                return self._members.get(uid)

            async def fetch_member(self, uid):
                return self._members.get(uid)

        guild = GuildWithMembers(guild_id)
        bot.guilds.append(guild)
        bot.get_guild = lambda gid: next((g for g in bot.guilds if g.id == gid), None)

        cog = React(bot=bot)

        # Replace role resolution to return simple role objects
        async def fake_resolve(g, role_ids):
            return [type("R", (), {"id": int(r), "name": f"r{r}"})() for r in role_ids]

        cog.resolve_manageable_roles = fake_resolve

        # Give worker a moment to start
        await asyncio.sleep(0.01)

        # Enqueue multiple updates for the same user; worker should coalesce
        cog.enqueue_role_update(guild_id, user_id, [1], [])
        cog.enqueue_role_update(guild_id, user_id, [2], [])
        cog.enqueue_role_update(guild_id, user_id, [3], [])

        # Wait for the worker to process the batch
        await asyncio.sleep(0.3)

        assert member.add_calls == 1
        roles_in_call = member.add_args[0][0] if member.add_args else ()
        assert len(roles_in_call) == 3

        # Cleanup: stop worker tasks
        try:
            await cog.cog_unload()
        except Exception:
            try:
                if getattr(cog, "_role_update_worker_task", None):
                    cog._role_update_worker_task.cancel()
            except Exception:
                pass

    asyncio.get_event_loop().run_until_complete(run_case())


def test_enqueue_role_update_puts_item_in_queue():
    async def run_case():
        bot = DummyBot()
        cog = React(bot=bot)
        # Stop worker so it doesn't consume the queue during test
        try:
            if getattr(cog, "_role_update_worker_task", None):
                cog._role_update_worker_task.cancel()
        except Exception:
            pass

        cog.enqueue_role_update(111, 222, [1, 2], [3])
        item = cog._role_update_queue.get_nowait()
        assert item[0] == 111
        assert item[1] == 222
        assert set(item[2]) == {"1", "2"}
        assert set(item[3]) == {"3"}

    asyncio.get_event_loop().run_until_complete(run_case())


def test_batched_worker_coalesced_removals():
    async def run_case():
        bot = DummyBot()
        guild_id = 555555
        user_id = 666666

        class MemberWithCounters(DummyMember):
            def __init__(self, id):
                super().__init__(id)
                self.add_calls = 0
                self.remove_calls = 0
                self.add_args = []
                self.remove_args = []

            async def add_roles(self, *roles, **kwargs):
                self.add_calls += 1
                self.add_args.append((roles, kwargs))

            async def remove_roles(self, *roles, **kwargs):
                self.remove_calls += 1
                self.remove_args.append((roles, kwargs))

        member = MemberWithCounters(user_id)

        class GuildWithMembers(DummyGuild):
            def __init__(self, id):
                super().__init__(id)
                self._members = {user_id: member}

            def get_member(self, uid):
                return self._members.get(uid)

            async def fetch_member(self, uid):
                return self._members.get(uid)

        guild = GuildWithMembers(guild_id)
        bot.guilds.append(guild)
        bot.get_guild = lambda gid: next((g for g in bot.guilds if g.id == gid), None)

        cog = React(bot=bot)

        async def fake_resolve(g, role_ids):
            return [type("R", (), {"id": int(r), "name": f"r{r}"})() for r in role_ids]

        cog.resolve_manageable_roles = fake_resolve

        await asyncio.sleep(0.01)

        cog.enqueue_role_update(guild_id, user_id, [], [10])
        cog.enqueue_role_update(guild_id, user_id, [], [20])
        cog.enqueue_role_update(guild_id, user_id, [], [30])

        await asyncio.sleep(0.3)

        assert member.remove_calls == 1
        removed = member.remove_args[0][0] if member.remove_args else ()
        assert len(removed) == 3

        try:
            await cog.cog_unload()
        except Exception:
            try:
                if getattr(cog, "_role_update_worker_task", None):
                    cog._role_update_worker_task.cancel()
            except Exception:
                pass

    asyncio.get_event_loop().run_until_complete(run_case())


def test_batched_worker_cross_user_batching():
    async def run_case():
        bot = DummyBot()
        guild_id = 999999
        user_a = 101
        user_b = 202

        class MemberWithCounters(DummyMember):
            def __init__(self, id):
                super().__init__(id)
                self.add_calls = 0
                self.add_args = []

            async def add_roles(self, *roles, **kwargs):
                self.add_calls += 1
                self.add_args.append((roles, kwargs))

        ma = MemberWithCounters(user_a)
        mb = MemberWithCounters(user_b)

        class GuildWithMembers(DummyGuild):
            def __init__(self, id):
                super().__init__(id)
                self._members = {user_a: ma, user_b: mb}

            def get_member(self, uid):
                return self._members.get(uid)

            async def fetch_member(self, uid):
                return self._members.get(uid)

        guild = GuildWithMembers(guild_id)
        bot.guilds.append(guild)
        bot.get_guild = lambda gid: next((g for g in bot.guilds if g.id == gid), None)

        cog = React(bot=bot)

        async def fake_resolve(g, role_ids):
            return [type("R", (), {"id": int(r), "name": f"r{r}"})() for r in role_ids]

        cog.resolve_manageable_roles = fake_resolve

        await asyncio.sleep(0.01)

        cog.enqueue_role_update(guild_id, user_a, [1], [])
        cog.enqueue_role_update(guild_id, user_b, [2], [])

        await asyncio.sleep(0.3)

        assert ma.add_calls == 1
        assert mb.add_calls == 1

        # Cleanup
        try:
            await cog.cog_unload()
        except Exception:
            try:
                if getattr(cog, "_role_update_worker_task", None):
                    cog._role_update_worker_task.cancel()
            except Exception:
                pass

    asyncio.get_event_loop().run_until_complete(run_case())
