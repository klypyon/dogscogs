import asyncio

from react.actions import ReactionAction
from react.react import React


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
