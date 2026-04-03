"""React cog: pluggable, config-driven reaction-role system for Red.

This module implements a small framework that maps message reactions to
behaviours called "actions". Each action is a self-contained class that
implements two asynchronous hooks: `on_add` (reaction added) and
`on_remove` (reaction removed). Actions are registered in a central
registry and instantiated dynamically from the guild config.

Design goals
- Extensible: new behaviours are implemented by adding a `ReactionAction`
    subclass and registering it via `@ReactionAction.register("type")`.
- Config-driven: administrators configure which message + emoji maps to
    which action and options using the `reactions` mapping in the guild's
    Config (see schema below).

Config (guild-level)
- `reactions`: { message_id (str) : { emoji (str) : action_data (dict) } }
    Example:

    {
        "123456789012345678": {
            "👍": {"type": "standard", "roles": ["111111111111111111"]},
            "🔥": {"type": "timed", "roles": ["222222222222222222"], "duration": 3600}
        }
    }

- `groups`: (optional) configuration for grouped behaviours.
- `timed_roles`: runtime store for scheduled removals. Shape:

    {
        "<user_id_str>": { "<role_id_str>": <unix_expiration_timestamp> }
    }

Adding new actions
- Subclass `ReactionAction`, implement `on_add` and `on_remove`, then
    register with `@ReactionAction.register("your_type")`.

Runtime flow
- `on_raw_reaction_add`/`on_raw_reaction_remove` validate the guild and
    message/emoji presence in config and forward to `handle_react`/
    `handle_unreact`.
- `handle_react`/`handle_unreact` resolve the appropriate `ReactionAction`
    from config and call its hook. Action implementations are responsible
    for permission checks, role hierarchy validation, and error handling.

Notes
- The bot needs `Manage Roles` and must be higher than roles it manages.
- Timed-role removals require a scheduler that inspects `timed_roles`
    and removes expired entries; a scheduler is left as TODO.
"""

from typing import Literal, Any, Dict, Optional

import asyncio
import logging
import time
from discord.ext import tasks
import re

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.config import Config

from dogscogs.constants import COG_IDENTIFIER

from .actions import ReactionAction
from .utils import build_types_embeds, find_message_channel, validate_emoji

RequestType = Literal["discord_deleted_user", "owner", "user", "user_strict"]

log = logging.getLogger(__name__)


class UndoAddView(discord.ui.View):
    def __init__(self, cog: "React", ctx: commands.Context, msg_id: str, emoji_key: str, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.msg_id = str(msg_id)
        self.emoji_key = str(emoji_key)

    @discord.ui.button(label="Undo", style=discord.ButtonStyle.danger)
    async def undo_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("Only the command author or members with Manage Roles can undo.", ephemeral=True)
            return
        async with self.cog._get_reaction_lock(self.ctx.guild.id):
            try:
                cfg = await self.cog.config.guild(self.ctx.guild).reactions()
            except Exception:
                await interaction.response.send_message("Failed to read config.", ephemeral=True)
                return
            msg_map = cfg.get(self.msg_id, {}) or {}
            removed = False
            if self.emoji_key in msg_map:
                msg_map.pop(self.emoji_key, None)
                removed = True
            else:
                m = re.search(r"(\d{17,21})", str(self.emoji_key))
                if m:
                    eid = m.group(1)
                    for k in list(msg_map.keys()):
                        if re.search(eid, str(k)):
                            msg_map.pop(k, None)
                            removed = True
                            break
            if not removed:
                await interaction.response.send_message("Mapping not found.", ephemeral=True)
                return

            if not msg_map:
                cfg.pop(self.msg_id, None)
                try:
                    mcfg = await self.cog.config.guild(self.ctx.guild).message_channels()
                    if self.msg_id in (mcfg or {}):
                        mcfg.pop(self.msg_id, None)
                        await self.cog.config.guild(self.ctx.guild).message_channels.set(mcfg)
                except Exception:
                    pass
            else:
                cfg[self.msg_id] = msg_map

            try:
                await self.cog.config.guild(self.ctx.guild).reactions.set(cfg)
            except Exception:
                await interaction.response.send_message("Failed to persist undo.", ephemeral=True)
                return

        try:
            await interaction.response.edit_message(content="Mapping removed (undo).", view=None)
        except Exception:
            try:
                await interaction.response.send_message("Mapping removed (undo).", ephemeral=True)
            except Exception:
                pass

# Action implementations (StandardAction, PermanentAddAction, PermanentRemoveAction,
# ReverseAction, GroupedAction, TimedAction) are defined in `react.actions`.
# Importing `ReactionAction` from that module ensures the registry is loaded
# and `ReactionAction.create()` is available to the cog below.


class React(commands.Cog):
    """
    Reaction-role cog: maps configured message+emoji pairs to behaviour classes.

    This cog is a thin dispatcher. The heavy lifting is performed by
    `ReactionAction` subclasses which encapsulate specific behaviours
    (standard, permanent, reverse, grouped, timed, etc.).

    The `reactions` guild config maps message ids to emojis to a small
    configuration dict (called `action_data`) that is passed to the
    appropriate `ReactionAction` constructor as `self.cfg`.

    Abilities (high level):
    - Standard (Add/Remove)
    - Permanent Add / Permanent Remove
    - Reverse (Remove/Add)
    - Grouped (mutually exclusive role sets)
    - Timed variants (role expires after configured duration)

    Config schema examples and notes are described at the top of the
    module. To add a new action type, implement and register a
    `ReactionAction` subclass.
    """

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=COG_IDENTIFIER,
            force_registration=True,
        )

        self.config.register_guild(
            reactions={},
            groups={},
            timed_roles={},
            message_channels={},
        )

        # in-memory helpers for timed-role coordination
        # - _timed_locks: per-guild locks to serialize read-modify-write
        # - _timed_guilds: set of guild IDs that currently have entries
        #   in `timed_roles` (keeps cleanup loop fast)
        self._timed_locks: Dict[int, asyncio.Lock] = {}
        self._timed_guilds: set[int] = set()
        # per-guild locks for reaction config writes to avoid races
        self._reaction_locks: Dict[int, asyncio.Lock] = {}
        # queue and worker for performing bot-initiated reaction removals
        self._unreact_queue: "asyncio.Queue[tuple[int,str,str,int]]" = asyncio.Queue()
        self._unreact_worker_task: Optional[asyncio.Task] = None
        # queue and worker for batched role updates (add/remove consolidation)
        self._role_update_queue: "asyncio.Queue[tuple[int,int,tuple,tuple]]" = asyncio.Queue()
        self._role_update_worker_task: Optional[asyncio.Task] = None
        # suppression set for bot-initiated reaction removals to avoid
        # handling the corresponding on_raw_reaction_remove events.
        # Shape: { guild_id: set((message_id_str, emoji_key_str, user_id_int)) }
        self._suppressed_unreacts: Dict[int, set] = {}

        # Start the background timed-role cleanup loop. The loop waits for
        # the bot to be ready before running (see `_before_timed_role_cleanup`).
        # We catch RuntimeError to be safe during reloads where the loop may
        # already be running.
        try:
            self._timed_role_cleanup.start()
        except RuntimeError:
            # If the loop is already running (e.g., during a reload), ignore.
            pass
        # Start the unreact worker (best-effort; ignore runtime errors on reload)
        try:
            self._unreact_worker_task = asyncio.create_task(self._unreact_worker())
        except RuntimeError:
            # ignore if loop not running or on reload
            self._unreact_worker_task = None
        # Start the role-update worker (best-effort; ignore runtime errors on reload)
        try:
            self._role_update_worker_task = asyncio.create_task(self._role_update_worker())
        except RuntimeError:
            self._role_update_worker_task = None

    def _get_timed_lock(self, guild_id: int) -> asyncio.Lock:
        """Return (and create if needed) a per-guild asyncio.Lock."""
        lock = self._timed_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._timed_locks[guild_id] = lock
        return lock

    def _get_reaction_lock(self, guild_id: int) -> asyncio.Lock:
        """Return (and create) a per-guild lock for `reactions` writes."""
        lock = self._reaction_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._reaction_locks[guild_id] = lock
        return lock

    def _suppress_unreact(self, guild_id: int, message_id: str, emoji_key: str, user_id: int) -> None:
        """Record a bot-initiated unreact so the upcoming raw removal
        event can be ignored by `handle_unreact`.
        """
        try:
            s = self._suppressed_unreacts.get(guild_id)
            if s is None:
                s = set()
                self._suppressed_unreacts[guild_id] = s
            s.add((str(message_id), str(emoji_key), int(user_id)))
        except Exception:
            # best-effort; do not let suppression failures break processing
            pass

    def enqueue_role_update(self, guild_id: int, user_id: int, add_role_ids=None, remove_role_ids=None) -> None:
        """Enqueue a batched role update for background consolidation.

        Arguments are best-effort: role ids may be ints or strings. This
        call is non-blocking and failures are logged but do not raise.
        """
        try:
            adds = [str(x) for x in (add_role_ids or [])]
            removes = [str(x) for x in (remove_role_ids or [])]
            try:
                self._role_update_queue.put_nowait((int(guild_id), int(user_id), tuple(adds), tuple(removes)))
            except Exception:
                asyncio.create_task(self._role_update_queue.put((int(guild_id), int(user_id), tuple(adds), tuple(removes))))
        except Exception:
            log.exception("Failed to enqueue role update for guild %s user %s", guild_id, user_id)

    def enqueue_unreact(self, guild_id: int, message_id: str, emoji_key: str, user_id: int) -> None:
        """Enqueue a bot-initiated reaction removal for background processing.

        This is non-blocking and best-effort; failures are logged.
        """
        try:
            self._unreact_queue.put_nowait((int(guild_id), str(message_id), str(emoji_key), int(user_id)))
        except Exception:
            try:
                # fallback to scheduling a background put
                asyncio.create_task(self._unreact_queue.put((int(guild_id), str(message_id), str(emoji_key), int(user_id))))
            except Exception:
                log.exception("Failed to enqueue unreact task for guild %s message %s emoji %s user %s", guild_id, message_id, emoji_key, user_id)

    async def _unreact_worker(self) -> None:
        """Background worker: remove queued reactions reliably with backoff.

        The worker locates the message (using `message_channels` fast-path or
        a channel scan via `find_message_channel`), attempts to remove the
        reaction on behalf of the configured user, and retries on transient
        failures with exponential backoff. Each removal is suppressed via
        `_suppress_unreact` to avoid re-processing the resulting raw event.
        """
        try:
            await self.bot.wait_until_ready()
        except Exception:
            # Some test harnesses or dummy bot objects may not implement
            # `wait_until_ready`; in that case proceed immediately.
            pass
        while True:
            item = None
            try:
                item = await self._unreact_queue.get()
                if not item or len(item) != 4:
                    continue
                guild_id, msg_id, emoji_key, user_id = item
                guild = self.bot.get_guild(int(guild_id))
                if guild is None:
                    continue

                try:
                    mcfg = await self.config.guild(guild).message_channels()
                except Exception:
                    mcfg = {}

                bot_member = guild.me
                if bot_member is None:
                    try:
                        bot_member = await guild.fetch_member(self.bot.user.id)
                    except Exception:
                        bot_member = None

                # Locate the message (best-effort)
                try:
                    ch_id, msg_obj = await find_message_channel(guild, int(msg_id), bot_member=bot_member, message_channels=mcfg, concurrency=6)
                except Exception:
                    msg_obj = None

                if msg_obj is None:
                    continue

                # Resolve emoji object for removal
                try:
                    try:
                        pe = discord.PartialEmoji.from_str(str(emoji_key))
                    except Exception:
                        pe = None
                    if pe is not None and getattr(pe, "id", None) is not None:
                        emoji_obj = pe
                    else:
                        m = re.search(r"(\d{17,21})", str(emoji_key))
                        if m:
                            try:
                                eobj = guild.get_emoji(int(m.group(1)))
                                if eobj is not None:
                                    emoji_obj = eobj
                                else:
                                    emoji_obj = str(emoji_key)
                            except Exception:
                                emoji_obj = str(emoji_key)
                        else:
                            emoji_obj = str(emoji_key)
                except Exception:
                    emoji_obj = str(emoji_key)

                # Attempt removal with retries/backoff
                attempts = 0
                max_attempts = 5
                backoff_base = 0.5
                while attempts < max_attempts:
                    attempts += 1
                    try:
                        # mark suppression so the raw unreact event is ignored
                        try:
                            self._suppress_unreact(guild.id, msg_id, emoji_key, user_id)
                        except Exception:
                            pass

                        # prefer Member if resolvable; otherwise use a light-weight object
                        user_obj = guild.get_member(user_id) or discord.Object(user_id)
                        await msg_obj.remove_reaction(emoji_obj, user_obj)
                        break
                    except discord.NotFound:
                        # message or reaction already gone
                        break
                    except discord.Forbidden:
                        log.warning("Forbidden removing reaction %s from message %s for user %s in guild %s", emoji_key, msg_id, user_id, guild.id)
                        break
                    except discord.HTTPException:
                        # transient error/rate-limit — backoff then retry
                        try:
                            await asyncio.sleep(backoff_base * attempts)
                        except Exception:
                            pass
                        continue
                    except Exception:
                        log.exception("Unexpected error removing reaction %s on message %s for user %s in guild %s", emoji_key, msg_id, user_id, guild.id)
                        break
            finally:
                try:
                    if item is not None:
                        self._unreact_queue.task_done()
                except Exception:
                    pass

    async def _role_update_worker(self) -> None:
        """Background worker: consolidate batched role add/remove operations.

        The worker collects closely timed role-update intents for the same
        guild+user and applies the net adds/removes in one or two atomic
        role operations. Retries on transient errors with exponential
        backoff. This is best-effort and does not guarantee strict
        ordering across different users or guilds.
        """
        try:
            await self.bot.wait_until_ready()
        except Exception:
            # Proceed immediately if test harness provides no wait_until_ready
            pass

        while True:
            items = []
            popped = 0
            try:
                # Block until at least one item is available
                first = await self._role_update_queue.get()
                popped += 1
                if first and len(first) == 4:
                    items.append(first)

                # Small window to allow rapid coalescing of multiple enqueues
                try:
                    await asyncio.sleep(0.05)
                except Exception:
                    pass

                # Drain the rest of the currently queued items
                while True:
                    try:
                        more = self._role_update_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    popped += 1
                    if more and len(more) == 4:
                        items.append(more)

                if not items:
                    continue

                # Aggregate per (guild, user) so we perform at most one
                # add/remove pair per member in this batch loop.
                aggregates: Dict[tuple[int, int], Dict[str, set]] = {}
                for it in items:
                    try:
                        g_id, u_id, adds, removes = it
                    except Exception:
                        continue
                    key = (int(g_id), int(u_id))
                    ent = aggregates.get(key)
                    if ent is None:
                        ent = {"adds": set(), "removes": set()}
                        aggregates[key] = ent
                    ent["adds"].update(str(x) for x in (adds or ()) if x)
                    ent["removes"].update(str(x) for x in (removes or ()) if x)

                # Apply aggregated updates per-member
                for (g_id, u_id), sets in aggregates.items():
                    adds_set = sets.get("adds", set())
                    removes_set = sets.get("removes", set())
                    final_adds = list(adds_set - removes_set)
                    final_removes = list(removes_set - adds_set)

                    guild = self.bot.get_guild(int(g_id))
                    if guild is None:
                        continue
                    member = guild.get_member(int(u_id))
                    if member is None:
                        try:
                            member = await guild.fetch_member(int(u_id))
                        except Exception:
                            member = None
                    if member is None:
                        continue

                    add_roles = await self.resolve_manageable_roles(guild, final_adds) if final_adds else []
                    remove_roles = await self.resolve_manageable_roles(guild, final_removes) if final_removes else []

                    attempts = 0
                    max_attempts = 5
                    backoff_base = 0.5
                    while attempts < max_attempts:
                        attempts += 1
                        try:
                            if add_roles:
                                await member.add_roles(*add_roles, reason="batched role update", atomic=True)
                            if remove_roles:
                                await member.remove_roles(*remove_roles, reason="batched role update", atomic=True)
                            break
                        except discord.Forbidden:
                            log.warning("Forbidden applying batched role update for user %s in guild %s", u_id, g_id)
                            break
                        except discord.HTTPException:
                            try:
                                await asyncio.sleep(backoff_base * attempts)
                            except Exception:
                                pass
                            continue
                        except Exception:
                            log.exception("Unexpected error applying batched role update for user %s in guild %s", u_id, g_id)
                            break
            finally:
                # Mark all dequeued items as done
                for _ in range(popped):
                    try:
                        self._role_update_queue.task_done()
                    except Exception:
                        pass

    def _emoji_key_matches(self, key: str, payload_emoji: object) -> bool:
        """Return True if the stored key likely refers to the given payload emoji.

        This is intentionally permissive: it attempts to match either by
        exact string, by custom-emoji id embedded in the stored key, or
        by emoji name for unicode emojis.
        """
        try:
            if key == str(payload_emoji):
                return True
        except Exception:
            pass

        # If the stored key contains a snowflake, try to match against payload id
        m = re.search(r"(\d{17,21})", str(key))
        pid = getattr(payload_emoji, "id", None)
        if m and pid is not None:
            try:
                return int(m.group(1)) == int(pid)
            except Exception:
                pass

        # Fallback: compare the name (works for some unicode cases)
        name = getattr(payload_emoji, "name", None)
        if name and key == name:
            return True

        return False

    async def resolve_manageable_roles(self, guild: discord.Guild, role_ids: list) -> list:
        """Resolve role IDs to Role objects and filter by bot manageability.

        Returns a list of Role objects that the bot can manage (not managed,
        and lower than the bot's top role). This centralizes permission and
        hierarchy checks for all actions.
        """
        roles = []
        if not role_ids or guild is None:
            return roles

        for rid in role_ids:
            try:
                rid_int = int(rid)
            except Exception:
                continue
            role = guild.get_role(rid_int)
            if role is not None:
                roles.append(role)

        return await self.filter_manageable_roles(guild, roles)

    async def filter_manageable_roles(self, guild: discord.Guild, roles: list) -> list:
        """Filter a list of Role objects to those the bot can manage.

        Criteria:
        - Bot has `manage_roles` permission in the guild.
        - Role is not `managed` (integration-created).
        - Role position is strictly lower than the bot's top role.
        """
        if not roles or guild is None:
            return []

        bot_member = guild.me
        if bot_member is None:
            try:
                bot_member = await guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        if bot_member is None:
            log.debug("Cannot determine bot member for guild %s", getattr(guild, "id", None))
            return []

        if not bot_member.guild_permissions.manage_roles:
            log.warning("Bot lacks manage_roles in guild %s", getattr(guild, "id", None))
            return []

        bot_top_pos = getattr(bot_member.top_role, "position", 0)
        manageable = []
        for r in roles:
            if r is None:
                continue
            if getattr(r, "managed", False):
                continue
            try:
                if r.position >= bot_top_pos:
                    continue
            except Exception:
                continue
            manageable.append(r)

        return manageable

    def _validate_action_data(self, action_data: Any) -> tuple:
        """Validate a single `action_data` mapping from the `reactions` config.

        Returns a tuple `(is_valid: bool, reason: str)`. This is a lightweight
        sanity check to detect common misconfigurations that would otherwise
        raise during runtime.
        """
        if not isinstance(action_data, dict):
            return False, "action_data is not a mapping"

        action_type = action_data.get("type", "standard")
        if action_type not in ReactionAction.registry:
            return False, f"unknown action type '{action_type}'"

        # roles may be many shapes; ensure they are int-like or iterable of int-like
        roles = action_data.get("roles", [])
        if roles is None:
            pass
        elif isinstance(roles, dict):
            for k in roles.keys():
                try:
                    int(k)
                except Exception:
                    return False, "roles dict keys are not int-like"
        elif isinstance(roles, (list, tuple, set)):
            for v in roles:
                try:
                    int(v)
                except Exception:
                    return False, "roles contain non-int-like values"
        else:
            try:
                int(roles)
            except Exception:
                return False, "roles scalar is not int-like"

        # duration must be int-like if present
        if "duration" in action_data and action_data.get("duration") is not None:
            try:
                int(action_data.get("duration"))
            except Exception:
                return False, "duration is not int-like"

        # group must be a string if present
        if "group" in action_data and action_data.get("group") is not None:
            if not isinstance(action_data.get("group"), str):
                return False, "group must be a string"

        return True, "ok"

    @commands.group(name="reactrole", aliases=("reactroles", "reactmap"))
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole(self, ctx: commands.Context) -> None:
        """Admin commands for managing reaction-role mappings.

        Subcommands include `add`, `remove`, `list`, `validate`, `clean`,
        `types`, `sample` and `wizard`.
        """
        pass

    @reactrole.command(name="validate")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_validate(self, ctx: commands.Context) -> None:
        """Validate the current `reactions` config for this guild."""
        try:
            cfg = await self.config.guild(ctx.guild).reactions()
        except Exception:
            await ctx.send("Failed to read reactions config.")
            return

        invalid = []
        # Pre-resolve bot member for permission checks
        bot_member = ctx.guild.me
        if bot_member is None:
            try:
                bot_member = await ctx.guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        async def _message_exists(guild: discord.Guild, message_id: int) -> bool:
            mcfg = {}
            try:
                mcfg = await self.config.guild(guild).message_channels()
            except Exception:
                mcfg = {}
            ch_id, fetched = await find_message_channel(guild, message_id, bot_member=bot_member, message_channels=mcfg, concurrency=8)
            return fetched is not None

        for msg_id, emojis in (cfg or {}).items():
            try:
                mid = int(msg_id)
            except Exception:
                invalid.append((msg_id, None, "invalid message id"))
                continue
            # mark missing/deleted messages as invalid
            try:
                exists = await _message_exists(ctx.guild, mid)
            except Exception:
                exists = True
            if not exists:
                invalid.append((msg_id, None, "message not found (deleted)"))
                continue
            if not isinstance(emojis, dict):
                invalid.append((msg_id, None, "message mapping is not a dict"))
                continue
            for emoji, action_data in emojis.items():
                ok, reason = self._validate_action_data(action_data)
                if not ok:
                    invalid.append((msg_id, emoji, reason))

        if not invalid:
            await ctx.send("No invalid reaction-role mappings found.")
            return

        lines = [f"Message {m} Emoji {e or '<all>'}: {r}" for m, e, r in invalid]
        out = "\n".join(lines[:20])
        if len(lines) > 20:
            out += f"\n... and {len(lines)-20} more invalid entries"
        await ctx.send(f"Found {len(invalid)} invalid entries:\n{out}")

    @reactrole.command(name="clean")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_clean(self, ctx: commands.Context, fix: bool = False) -> None:
        """Report invalid config entries; pass `fix=True` to remove them."""
        try:
            cfg = await self.config.guild(ctx.guild).reactions()
        except Exception:
            await ctx.send("Failed to read reactions config.")
            return

        invalid = []
        # Pre-resolve bot member for permission checks
        bot_member = ctx.guild.me
        if bot_member is None:
            try:
                bot_member = await ctx.guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        async def _message_exists(guild: discord.Guild, message_id: int) -> bool:
            mcfg = {}
            try:
                mcfg = await self.config.guild(guild).message_channels()
            except Exception:
                mcfg = {}
            ch_id, fetched = await find_message_channel(guild, message_id, bot_member=bot_member, message_channels=mcfg, concurrency=8)
            return fetched is not None

        for msg_id, emojis in list((cfg or {}).items()):
            try:
                mid = int(msg_id)
            except Exception:
                invalid.append((msg_id, None, "invalid message id"))
                continue
            try:
                exists = await _message_exists(ctx.guild, mid)
            except Exception:
                exists = True
            if not exists:
                invalid.append((msg_id, None, "message not found (deleted)"))
                continue
            if not isinstance(emojis, dict):
                invalid.append((msg_id, None, "message mapping is not a dict"))
                continue
            for emoji, action_data in list(emojis.items()):
                ok, reason = self._validate_action_data(action_data)
                if not ok:
                    invalid.append((msg_id, emoji, reason))

        if not invalid:
            await ctx.send("No invalid reaction-role mappings found.")
            return

        if not fix:
            await ctx.send(f"Found {len(invalid)} invalid entries. Re-run with `{ctx.prefix}reactrole clean True` to remove them.")
            return

        # Apply removals under the per-guild reaction lock to avoid races.
        removed = 0
        async with self._get_reaction_lock(ctx.guild.id):
            try:
                cfg = await self.config.guild(ctx.guild).reactions()
            except Exception:
                await ctx.send("Failed to read reactions config during cleanup.")
                return

            for msg_id, emoji, _ in invalid:
                if emoji is None:
                    if msg_id in cfg:
                        cfg.pop(msg_id, None)
                        removed += 1
                        # also remove any persisted channel mapping
                        try:
                            mcfg = await self.config.guild(ctx.guild).message_channels()
                            if msg_id in mcfg:
                                mcfg.pop(msg_id, None)
                                await self.config.guild(ctx.guild).message_channels.set(mcfg)
                        except Exception:
                            pass
                else:
                    msg_map = cfg.get(msg_id, {})
                    if emoji in msg_map:
                        msg_map.pop(emoji, None)
                        removed += 1
                    if not msg_map:
                        cfg.pop(msg_id, None)
                        # remove persisted channel mapping when message has no more mappings
                        try:
                            mcfg = await self.config.guild(ctx.guild).message_channels()
                            if msg_id in mcfg:
                                mcfg.pop(msg_id, None)
                                await self.config.guild(ctx.guild).message_channels.set(mcfg)
                        except Exception:
                            pass
                    else:
                        cfg[msg_id] = msg_map

            try:
                await self.config.guild(ctx.guild).reactions.set(cfg)
            except Exception:
                log.exception("Failed to persist cleaned reactions config for guild %s", getattr(ctx.guild, "id", None))
                await ctx.send("Failed to persist changes — check bot logs.")
                return

        await ctx.send(f"Removed {removed} invalid mapping entries.")

    @reactrole.command(name="add")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_add(self, ctx: commands.Context, message: str, emoji: str, action_type: str = "standard", *args) -> None:
        """Add or update a reaction-role mapping for a message.

        `message` accepts either a raw message id or a full message URL
        (https://discord.com/channels/<guild_id>/<channel_id>/<message_id>).
        `emoji` accepts a unicode emoji, a custom emoji mention like
        `<:name:id>`, or just the emoji id number.

        Usage examples:
        - `reactrole add <message_id> 👍 standard 123456789012345678`
        - `reactrole add https://discord.com/channels/123/456/789 🔥 timed 345678901234567890 duration=3600`
        - `reactrole add 123 987654321098765432 grouped 456 group=color`

        Optional key/value options may be passed after role ids, e.g.
        `duration=3600` or `remove_on_unreact=false`.
        """
        if ctx.guild is None:
            await ctx.send("This command must be used in a guild.")
            return

        # Normalize the provided message argument: accept either a URL or id
        msg_token = str(message).strip()
        matches = re.findall(r"\d{17,21}", msg_token)
        if not matches:
            await ctx.send("Could not parse a message id from the provided value. Provide a message id or a message link.")
            return
        try:
            msg_id_val = int(matches[-1])
        except Exception:
            await ctx.send("Invalid message id provided.")
            return
        # Optionally capture a pasted channel id (for jump-links later)
        ch_id_val = None
        if "channels" in msg_token.lower() and len(matches) >= 2:
            try:
                ch_id_val = int(matches[-2])
            except Exception:
                ch_id_val = None

        # Parse arguments: support `key=value`, `--key value`, and positional
        # parameters. Heuristics: tokens that look like role ids or mentions
        # are treated as roles; remaining positionals are mapped to declared
        # action options (after `roles`) in declaration order.
        tokens = [str(x) for x in args]
        explicit_opts: dict[str, str] = {}
        positional: list[str] = []
        i = 0
        while i < len(tokens):
            t = tokens[i].strip()
            if not t:
                i += 1
                continue
            # key=value
            if "=" in t:
                k, v = t.split("=", 1)
                explicit_opts[k] = v
                i += 1
                continue
            # --key value or -key value
            if t.startswith("--") or (t.startswith("-") and len(t) > 1 and not t[1].isdigit()):
                key = t.lstrip("-")
                val = ""
                if i + 1 < len(tokens):
                    val = tokens[i + 1]
                    i += 2
                else:
                    i += 1
                explicit_opts[key] = val
                continue
            positional.append(t)
            i += 1

        # Split positional tokens into roles (where detectable) and other
        roles: list[str] = []
        other_positional: list[str] = []
        for p in positional:
            # Numeric id anywhere in token
            m = re.search(r"\d{17,21}", p)
            if m:
                roles.append(m.group(0))
                continue
            # Role mention like <@&id>
            rm = re.match(r"<@&(\d{17,21})>$", p)
            if rm:
                roles.append(rm.group(1))
                continue
            # Try exact role name match (single-token names only)
            if ctx.guild is not None:
                role_obj = discord.utils.get(ctx.guild.roles, name=p)
                if role_obj is not None:
                    roles.append(str(role_obj.id))
                    continue
            # Otherwise treat as positional option value
            other_positional.append(p)

        # If `roles` was supplied as an explicit option, consume it.
        if "roles" in explicit_opts and not roles:
            raw = str(explicit_opts.pop("roles") or "")
            if raw:
                toks = re.split(r"[,\s]+", raw.strip())
                for t in toks:
                    tt = t.strip()
                    if not tt:
                        continue
                    mm = re.search(r"\d{17,21}", tt)
                    if mm:
                        roles.append(mm.group(0))
                        continue
                    if ctx.guild is not None:
                        role_obj = discord.utils.get(ctx.guild.roles, name=tt)
                        if role_obj is not None:
                            roles.append(str(role_obj.id))
                            continue
                    roles.append(tt)

        # Map remaining positional values to declared options (after `roles`).
        cls = ReactionAction.registry.get(action_type)
        declared_opts = getattr(cls, "options", []) if cls is not None else []
        ordered_opts = [o for o in (declared_opts or []) if o != "roles"]
        pos_vals = list(other_positional)
        for opt_name in ordered_opts:
            if opt_name in explicit_opts:
                continue
            if pos_vals:
                explicit_opts[opt_name] = pos_vals.pop(0)

        # Build action_data from parsed values, converting known types.
        action_data: dict = {"type": action_type, "roles": roles}
        if "duration" in explicit_opts:
            dur_raw = explicit_opts.get("duration")
            if isinstance(dur_raw, str) and dur_raw.lower() in ("none", ""):
                pass
            else:
                try:
                    action_data["duration"] = int(explicit_opts["duration"])
                except Exception:
                    await ctx.send("Invalid duration value; must be integer seconds.")
                    return
        if "group" in explicit_opts:
            action_data["group"] = explicit_opts.get("group")
        if "remove_on_unreact" in explicit_opts:
            action_data["remove_on_unreact"] = ReactionAction._parse_bool(explicit_opts.get("remove_on_unreact"), True)

        # Validate presence of required options. Conservative rule: any
        # declared option except `remove_on_unreact` must be present.
        required_opts = [o for o in (declared_opts or []) if o != "remove_on_unreact"]
        missing: list[str] = []
        for o in required_opts:
            if o == "roles":
                if not action_data.get("roles"):
                    missing.append("roles")
            else:
                v = action_data.get(o, None)
                if v is None:
                    missing.append(o)
                else:
                    if isinstance(v, str) and v.strip() == "":
                        missing.append(o)

        if missing:
            sample = getattr(cls, "sample", None) or f"reactrole add <message_id> <emoji> {action_type} <role_id>"
            await ctx.send(f"Missing required options for `{action_type}`: {', '.join(missing)}. Example: `{sample}`")
            return

        ok, reason = self._validate_action_data(action_data)
        if not ok:
            await ctx.send(f"Invalid action_data: {reason}")
            return

        # Persist under a per-guild write lock to avoid races with other
        # mutating commands (clean/remove/wizard).
        msg_id = str(msg_id_val)
        async with self._get_reaction_lock(ctx.guild.id):
            try:
                cfg = await self.config.guild(ctx.guild).reactions()
            except Exception:
                await ctx.send("Failed to read reactions config.")
                return

            msg_map = cfg.get(msg_id, {}) or {}
            # Normalize emoji storage: accept either `<:name:id>`, unicode
            # emoji or bare numeric id. Try to parse a PartialEmoji when
            # possible; if the provided value is numeric use that string.
            emoji_token = str(emoji).strip()
            emoji_key = emoji_token
            try:
                pe = discord.PartialEmoji.from_str(emoji_token)
                emoji_key = str(pe)
            except Exception:
                m = re.search(r"(\d{17,21})", emoji_token)
                if m:
                    emoji_key = m.group(1)

            # Validate emoji early to provide clearer feedback for unicode
            try:
                pe = discord.PartialEmoji.from_str(emoji_token)
                eid = getattr(pe, "id", None)
            except Exception:
                eid = None
            # If PartialEmoji returned an object with no id (or from_str
            # raised), but the provided token contains a bare numeric id,
            # treat that as a custom emoji id so we can warn instead of
            # rejecting outright.
            if eid is None:
                m = re.search(r"(\d{17,21})", emoji_token)
                if m:
                    try:
                        eid = int(m.group(1))
                    except Exception:
                        eid = None
            try:
                ok, reason = await validate_emoji(self.bot, ctx.guild, emoji_token)
            except Exception:
                ok, reason = True, None
            if not ok:
                # If this is a custom emoji (has an id) accept it but warn;
                # reject only invalid unicode emoji inputs to preserve
                # historical behavior of `reactrole add`.
                if eid is None:
                    await ctx.send(f"Invalid emoji: {reason}")
                    return
                else:
                    try:
                        await ctx.send(f"Warning: custom emoji may be inaccessible to the bot: {reason}")
                    except Exception:
                        pass

            msg_map[emoji_key] = action_data
            cfg[msg_id] = msg_map

            try:
                await self.config.guild(ctx.guild).reactions.set(cfg)
            except Exception:
                log.exception("Failed to persist reaction mapping for guild %s", getattr(ctx.guild, "id", None))
                await ctx.send("Failed to persist mapping — check bot logs for details.")
                return

        # Persist channel->message mapping when a channel id was provided
        if ch_id_val is not None:
            try:
                mcfg = await self.config.guild(ctx.guild).message_channels()
                mcfg[msg_id] = ch_id_val
                await self.config.guild(ctx.guild).message_channels.set(mcfg)
            except Exception:
                log.exception("Failed to persist message channel mapping for guild %s", getattr(ctx.guild, "id", None))

        # Friendly non-pinging summary: render emoji and role mentions
        try:
            msg_disp = str(msg_id)
            if ch_id_val is not None:
                try:
                    guild_id = ctx.guild.id
                    link = f"https://discord.com/channels/{guild_id}/{ch_id_val}/{msg_id}"
                    msg_disp = f"[Jump to message]({link}) ({msg_id})"
                except Exception:
                    msg_disp = str(msg_id)
        except Exception:
            msg_disp = str(msg_id)

        # Render the emoji visually when possible
        try:
            emoji_disp = emoji_key
            try:
                pe = discord.PartialEmoji.from_str(emoji_key)
                emoji_disp = str(pe)
            except Exception:
                m = re.search(r"(\d{17,21})", str(emoji_key))
                if m:
                    try:
                        eobj = ctx.guild.get_emoji(int(m.group(1)))
                        if eobj is not None:
                            emoji_disp = str(eobj)
                    except Exception:
                        pass
        except Exception:
            emoji_disp = str(emoji_key)

        # Render roles mentions without pinging
        roles_tokens = action_data.get("roles") or []
        role_parts: list[str] = []
        try:
            for token in roles_tokens:
                try:
                    rid = int(token)
                except Exception:
                    role_parts.append(str(token))
                    continue
                role_obj = ctx.guild.get_role(rid) if ctx.guild is not None else None
                if role_obj is not None:
                    role_parts.append(role_obj.mention)
                else:
                    role_parts.append(str(rid))
        except Exception:
            role_parts = [str(x) for x in roles_tokens]

        roles_disp = ", ".join(role_parts) if role_parts else "(none)"

        allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
        content = (
            f"Mapping added successfully.\n"
            f"Message: {msg_disp}\n"
            f"Emoji: {emoji_disp}\n"
            f"Action: `{action_type}`\n"
            f"Roles: {roles_disp}\n\n"
            f"Use `{ctx.prefix}reactrole list` to verify or `{ctx.prefix}reactrole remove {msg_id} {emoji_key}` to remove."
        )
        try:
            view = UndoAddView(self, ctx, msg_id, emoji_key, timeout=30.0)
            await ctx.send(content, allowed_mentions=allowed, view=view)
        except Exception:
            await ctx.send(content, allowed_mentions=allowed)

    @reactrole.command(name="remove")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_remove(self, ctx: commands.Context, message: str, emoji: Optional[str] = None) -> None:
        """Remove a mapping for a message. `message` may be a message id or a message link.

        `emoji` may be omitted to remove the whole message mapping, or can be
        a unicode emoji, a custom emoji mention like `<:name:id>` or an emoji id.
        """
        if ctx.guild is None:
            await ctx.send("This command must be used in a guild.")
            return

        # Normalize message token to an id (accept links or raw ids)
        msg_token = str(message).strip()
        matches = re.findall(r"\d{17,21}", msg_token)
        if not matches:
            await ctx.send("Could not parse a message id from the provided value. Provide a message id or a message link.")
            return
        try:
            msg_id_val = int(matches[-1])
        except Exception:
            await ctx.send("Invalid message id provided.")
            return

        msg_id = str(msg_id_val)
        async with self._get_reaction_lock(ctx.guild.id):
            try:
                cfg = await self.config.guild(ctx.guild).reactions()
            except Exception:
                await ctx.send("Failed to read reactions config.")
                return

            if msg_id not in cfg:
                await ctx.send("No mappings found for that message.")
                return

            if emoji is None:
                cfg.pop(msg_id, None)
            else:
                emoji_token = str(emoji).strip()
                # Try to normalize the provided emoji to a key that matches
                # what's stored in the mapping. We attempt PartialEmoji, an
                # embedded id, or a direct equality match.
                try:
                    pe = discord.PartialEmoji.from_str(emoji_token)
                    emoji_key = str(pe)
                except Exception:
                    m = re.search(r"(\d{17,21})", emoji_token)
                    if m:
                        emoji_key = m.group(1)
                    else:
                        emoji_key = emoji_token

                msg_map = cfg.get(msg_id, {}) or {}
                removed = False
                # Direct key match first
                if emoji_key in msg_map:
                    msg_map.pop(emoji_key, None)
                    removed = True
                else:
                    # If emoji_key is numeric id, try to find a key that embeds that id
                    m = re.search(r"(\d{17,21})", str(emoji_key))
                    if m:
                        eid = m.group(1)
                        for k in list(msg_map.keys()):
                            if re.search(eid, str(k)):
                                msg_map.pop(k, None)
                                removed = True
                                break
                    # Fallback: try equality against stored keys (unicode cases)
                    if not removed:
                        for k in list(msg_map.keys()):
                            if str(k) == emoji_token:
                                msg_map.pop(k, None)
                                removed = True
                                break

                if not removed:
                    await ctx.send("No mapping for that emoji on the specified message.")
                    return

                if not msg_map:
                    cfg.pop(msg_id, None)
                else:
                    cfg[msg_id] = msg_map

            try:
                await self.config.guild(ctx.guild).reactions.set(cfg)
            except Exception:
                log.exception("Failed to persist reaction removals for guild %s", getattr(ctx.guild, "id", None))
                await ctx.send("Failed to persist changes — check bot logs.")
                return

        # If the message mapping was removed entirely, also drop any stored channel id
        try:
            if msg_id not in (await self.config.guild(ctx.guild).reactions() or {}):
                try:
                    mcfg = await self.config.guild(ctx.guild).message_channels()
                    if msg_id in (mcfg or {}):
                        mcfg.pop(msg_id, None)
                        await self.config.guild(ctx.guild).message_channels.set(mcfg)
                except Exception:
                    pass
        except Exception:
            pass

        # Friendly non-pinging confirmation
        allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
        try:
            emoji_disp = emoji if emoji is not None else "<all>"
            try:
                pe = discord.PartialEmoji.from_str(str(emoji or ""))
                emoji_disp = str(pe)
            except Exception:
                m = re.search(r"(\d{17,21})", str(emoji or ""))
                if m:
                    try:
                        eobj = ctx.guild.get_emoji(int(m.group(1)))
                        if eobj is not None:
                            emoji_disp = str(eobj)
                    except Exception:
                        pass
        except Exception:
            emoji_disp = str(emoji or "")

        await ctx.send(f"Mapping removed for message {msg_id} emoji {emoji_disp}.", allowed_mentions=allowed)

    @reactrole.command(name="list")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_list(self, ctx: commands.Context) -> None:
        """List configured reaction-role mappings for this guild."""
        try:
            cfg = await self.config.guild(ctx.guild).reactions()
        except Exception:
            await ctx.send("Failed to read reactions config.")
            return

        if not cfg:
            await ctx.send("No reaction-role mappings configured for this guild.")
            return
        # Build embeds for each message entry. Each embed contains a jump
        # link (if we can locate the channel), and a field per emoji mapping
        # showing the rendered emoji, formatted action type, role mentions,
        # and any options.
        embeds: list[discord.Embed] = []

        # Pre-resolve a bot member object for permission checks
        bot_member = ctx.guild.me
        if bot_member is None:
            try:
                bot_member = await ctx.guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        async def _find_channel_for_message(guild: discord.Guild, message_id: int) -> int | None:
            """Attempt to locate the channel id that contains `message_id`.

            This does a best-effort scan of text channels the bot can read.
            It's intentionally conservative: skip channels the bot can't
            read, and ignore errors when fetching messages.
            """
            try:
                mcfg = await self.config.guild(guild).message_channels()
            except Exception:
                mcfg = {}
            ch_id, fetched = await find_message_channel(guild, message_id, bot_member=bot_member, message_channels=mcfg, concurrency=8)
            return ch_id

        for msg_id, emojis in (cfg or {}).items():
            if not isinstance(emojis, dict):
                continue

            embed = discord.Embed(title=f"Message {msg_id}")
            # Try to locate the message's channel to build a jump link
            try:
                chan_id = await _find_channel_for_message(ctx.guild, int(msg_id))
                if chan_id is not None:
                    link = f"https://discord.com/channels/{ctx.guild.id}/{chan_id}/{msg_id}"
                    embed.description = f"[Jump to message]({link})"
            except Exception:
                # Ignore failures to avoid breaking the listing command
                pass

            # Group mappings by their rendered emoji so visually-identical
            # emoji (e.g., stored with different raw tokens) don't produce
            # duplicated fields in the embed. Each group will be presented
            # as a single field with per-key details.
            grouped: dict[str, list[tuple[str, dict]]] = {}
            for emoji_key, action_data in emojis.items():
                if not isinstance(action_data, dict):
                    continue

                rendered = str(emoji_key)
                try:
                    pe = discord.PartialEmoji.from_str(emoji_key)
                    rendered = str(pe)
                except Exception:
                    m = re.search(r"(\d{17,21})", str(emoji_key))
                    if m:
                        try:
                            eobj = ctx.guild.get_emoji(int(m.group(1)))
                            if eobj is not None:
                                rendered = str(eobj)
                        except Exception:
                            pass

                grouped.setdefault(rendered, []).append((emoji_key, action_data))

            for rendered_emoji, mappings in grouped.items():
                parts: list[str] = []
                raw_keys = [str(k) for k, _ in mappings]
                # Show only the rendered emoji in the field name; include a
                # concise count when multiple stored tokens map to the same
                # rendered glyph.
                field_name = (
                    f"{rendered_emoji}"
                    if len(raw_keys) == 1
                    else f"{rendered_emoji} (+{len(raw_keys)})"
                )

                for raw_key, action_data in mappings:
                    typ = action_data.get("type", "standard")
                    typ_fmt = f"`{typ}`"

                    # Resolve role ids to mentions when possible for readability
                    human_roles: list[str] = []
                    for r in (action_data.get("roles", []) or []):
                        try:
                            rid = int(r)
                        except Exception:
                            human_roles.append(str(r))
                            continue
                        role_obj = ctx.guild.get_role(rid)
                        if role_obj is not None:
                            human_roles.append(role_obj.mention)
                        else:
                            human_roles.append(str(r))

                    roles_str = ", ".join(human_roles) if human_roles else "(none)"

                    # Build options as separate lines for readability
                    opts_lines: list[str] = []
                    if "duration" in action_data:
                        opts_lines.append(f"duration: {action_data.get('duration')}")
                    if "group" in action_data:
                        opts_lines.append(f"group: {action_data.get('group')}")
                    if "remove_on_unreact" in action_data:
                        opts_lines.append(f"remove_on_unreact: {action_data.get('remove_on_unreact')}")

                    block_lines: list[str] = []
                    # Show stored token only when multiple underlying tokens map
                    # to the same rendered emoji to avoid confusion.
                    if len(raw_keys) > 1:
                        block_lines.append(f"Stored token: {raw_key}")
                    block_lines.append(f"Type: {typ_fmt}")
                    block_lines.append(f"Roles: {roles_str}")
                    if opts_lines:
                        block_lines.append("Options:")
                        for ol in opts_lines:
                            block_lines.append(f"- {ol}")

                    parts.append("\n".join(block_lines))

                value = "\n\n".join(parts)
                embed.add_field(name=(field_name[:256] or "mapping"), value=(value[:1024] or "(too long)"), inline=False)

            embeds.append(embed)

        if not embeds:
            await ctx.send("No valid reaction-role mappings found.")
            return

        allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
        for embed in embeds:
            await ctx.send(embed=embed, allowed_mentions=allowed)

    @reactrole.command(name="diag")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_diag(self, ctx: commands.Context, message: Optional[str] = None) -> None:
        """Diagnostic: dump `message_channels` and a reactions summary.

        Usage: `reactrole diag` or `reactrole diag <message_id_or_link>`
        """
        if ctx.guild is None:
            await ctx.send("This command must be used in a guild.")
            return

        try:
            mcfg = await self.config.guild(ctx.guild).message_channels()
        except Exception:
            mcfg = {}
        try:
            cfg = await self.config.guild(ctx.guild).reactions()
        except Exception:
            cfg = {}

        if message:
            token = str(message)
            matches = re.findall(r"\d{17,21}", token)
            if not matches:
                await ctx.send(f"Could not parse message id from `{message}`.")
                return
            mid = str(matches[-1])
            channel_id = None
            try:
                if isinstance(mcfg, dict):
                    # stored as string keys
                    channel_id = mcfg.get(mid) or mcfg.get(int(mid))
            except Exception:
                channel_id = None
            ch_obj = None
            if channel_id is not None:
                try:
                    ch_obj = ctx.guild.get_channel(int(channel_id))
                except Exception:
                    ch_obj = None

            mapping_info = "(no mapping)"
            try:
                if mid in (cfg or {}):
                    mapping_info = f"{len(cfg.get(mid, {}))} mapping(s) present"
            except Exception:
                mapping_info = "(unknown)"

            lines = [
                f"message_id: {mid}",
                f"stored_channel_id: {channel_id}",
                f"channel_object: {getattr(ch_obj, 'id', repr(ch_obj))}",
                f"reactions_mapping: {mapping_info}",
            ]
            await ctx.send("``\n" + "\n".join(lines) + "\n``")
            return

        # No specific message: dump summary and stored channel mapping
        import json

        try:
            mcfg_str = json.dumps(mcfg, indent=2)
        except Exception:
            mcfg_str = str(mcfg)
        try:
            total_msgs = len(list((cfg or {}).keys()))
        except Exception:
            total_msgs = "unknown"

        body = f"message_channels:\n{mcfg_str}\n\nreactions_config_messages: {total_msgs}"
        if len(body) > 1900:
            body = body[:1900] + "\n...[truncated]"
        await ctx.send("```\n" + body + "\n```")

    @reactrole.command(name="sync")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_sync(self, ctx: commands.Context, mode: str = "dry") -> None:
        """Scan configured message IDs to locate their channels.

        Usage: `reactrole sync` (dry-run) or `reactrole sync apply` to persist found mappings.
        """
        if ctx.guild is None:
            await ctx.send("This command must be used in a guild.")
            return

        mode_str = (mode or "").strip().lower()
        apply_changes = mode_str in ("apply", "persist", "save")

        try:
            cfg = await self.config.guild(ctx.guild).reactions()
        except Exception:
            await ctx.send("Failed to read reactions config.")
            return

        if not cfg:
            await ctx.send("No reaction-role mappings configured for this guild.")
            return

        try:
            mcfg = await self.config.guild(ctx.guild).message_channels() or {}
        except Exception:
            mcfg = {}

        # Pre-resolve bot member for permission checks
        bot_member = ctx.guild.me
        if bot_member is None:
            try:
                bot_member = await ctx.guild.fetch_member(self.bot.user.id)
            except Exception:
                bot_member = None

        mids = []
        for k in (cfg or {}).keys():
            try:
                mids.append(int(k))
            except Exception:
                continue

        found: dict[str, int] = {}

        async def _scan_one(mid: int):
            try:
                ch_id, _ = await find_message_channel(ctx.guild, mid, bot_member=bot_member, message_channels=mcfg, concurrency=6)
                return mid, ch_id
            except Exception:
                return mid, None

        # Process in small batches to avoid excessive concurrent channel fetches
        BATCH = 6
        for i in range(0, len(mids), BATCH):
            batch = mids[i : i + BATCH]
            tasks = [_scan_one(mid) for mid in batch]
            try:
                results = await asyncio.gather(*tasks)
            except Exception:
                results = []
            for mid, ch_id in results:
                if ch_id is not None:
                    found[str(mid)] = int(ch_id)

        summary_lines = [f"Scanned {len(mids)} message(s). Found {len(found)} channel mappings."]
        if found:
            sample = list(found.items())[:8]
            for mid, ch in sample:
                summary_lines.append(f"{mid} -> {ch}")

        if apply_changes and found:
            try:
                # Merge but do not overwrite existing entries unless discovered now
                new_map = dict(mcfg or {})
                new_map.update(found)
                await self.config.guild(ctx.guild).message_channels.set(new_map)
                summary_lines.insert(0, "Applied discovered mappings to `message_channels`.")
            except Exception:
                summary_lines.insert(0, "Failed to persist discovered mappings — see logs.")

        await ctx.send("``\n" + "\n".join(summary_lines) + "\n``")

    @reactrole.command(name="types")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_types(self, ctx: commands.Context) -> None:
        """List available action types and their options (dynamically from the registry).

        Condensed embed: use the embed description to list types as compact bullets
        (`- `name`: desc  \n  Options: `a`, `b``) and split into multiple embeds if
        the description would exceed Discord limits.
        """
        embeds = build_types_embeds(ReactionAction.registry)
        if not embeds:
            await ctx.send("No action types are registered.")
            return

        for embed in embeds:
            await ctx.send(embed=embed)

    @reactrole.command(name="sample")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_sample(self, ctx: commands.Context, action_type: str = "standard") -> None:
        """Show a Discord-friendly example command for the given action type.

        This prints an example `reactrole add` invocation you can copy
        into the server. Replace placeholders with the target message id,
        emoji and role ids/mentions.
        """
        typ = action_type or "standard"
        cls = ReactionAction.registry.get(typ)
        if cls is None:
            await ctx.send(f"Unknown action type '{action_type}'. Use `{ctx.prefix}reactrole types` to list types.")
            return

        example = getattr(cls, "sample", None)
        if example is None:
            # fallback to a conservative default
            example = f"reactrole add <message_id> <emoji> {typ} <role_id>"

        msg = f"Example for `{typ}`:\n```\n{example}\n```\nNotes: use role IDs or mentions; for custom emoji use `<:name:id>` format."
        await ctx.send(msg)

    @reactrole.command(name="wizard")
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def reactrole_wizard_cmd(self, ctx: commands.Context) -> None:
        """Interactive wizard to add a reaction-role mapping."""
        if ctx.guild is None:
            await ctx.send("This command must be used in a guild.")
            return
        # Import at call time to avoid import-time cycles
        try:
            from .views import run_reactrole_wizard
        except Exception:
            log.exception("Failed importing react views for wizard")
            await ctx.send("Internal error: wizard is unavailable.")
            return

        try:
            added = await run_reactrole_wizard(self, ctx)
        except Exception:
            log.exception("reactrole wizard failed")
            await ctx.send("Wizard failed due to an internal error.")
            return

        if added:
            await ctx.send("Mapping added via wizard.")
        else:
            await ctx.send("Wizard cancelled or no changes made.")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        """Event listener for when a reaction is added to a message."""
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        # Delegate to the handler; it will load the config and perform
        # a robust emoji-key match. Avoid duplicate config reads here.
        await self.handle_react(payload)
        
    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        """Event listener for when a reaction is removed from a message."""
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        # Delegate to handler which will perform config lookup and matching.
        await self.handle_unreact(payload)

    async def handle_react(self, payload: discord.RawReactionActionEvent) -> None:
        """Handles the logic for when a reaction is added to a message."""
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return

        # 1) load the per-guild reactions config and look up the action.
        cfg = await self.config.guild(guild).reactions()
        msg_id = str(payload.message_id)
        msg_map = cfg.get(msg_id)
        if not isinstance(msg_map, dict):
            return

        # Find the stored emoji key that matches the payload emoji. This
        # allows admin-provided keys to be flexible (custom emoji mention
        # formats, unicode, ids, etc.).
        emoji_key = None
        for key in msg_map.keys():
            try:
                if self._emoji_key_matches(key, payload.emoji):
                    emoji_key = key
                    break
            except Exception:
                continue
        if emoji_key is None:
            return

        # action_data is the small dict stored under reactions[message][emoji].
        raw_action = msg_map.get(emoji_key)
        if not isinstance(raw_action, dict):
            log.warning(
                "Invalid action_data for guild %s message %s emoji_key %s: %r",
                getattr(guild, "id", None),
                msg_id,
                emoji_key,
                raw_action,
            )
            return
        action_data = raw_action or {}
        action_type = action_data.get("type", "standard")
        try:
            action = ReactionAction.create(action_type, action_data)
        except KeyError:
            # configured action type not available (perhaps code not loaded);
            # callers could log this but we silently return to avoid crashes.
            log.exception(
                f"ReactionAction type '{action_type}' not found for message {msg_id} emoji_key {emoji_key} in guild {guild.id}"
            )
            return

        # 2) Resolve the member who reacted. Raw payloads sometimes include
        # the member on add events, but not always. We attempt a cache lookup
        # then fall back to a network fetch. Actions must handle member==None.
        member = getattr(payload, "member", None)
        if member is None:
            member = guild.get_member(payload.user_id)
            if member is None:
                try:
                    member = await guild.fetch_member(payload.user_id)
                except Exception:
                    member = None

        # 3) Some actions may want the original Message object. Try to fetch
        # it when possible, but tolerate fetch failures.
        channel = guild.get_channel(payload.channel_id) if payload.channel_id else None
        message = None
        if channel is not None:
            try:
                message = await channel.fetch_message(payload.message_id)
            except Exception:
                message = None

        # 4) Dispatch to the action implementation. The action is responsible
        # for performing role adds/removals, permission checks and error handling.
        try:
            await action.on_add(self, payload, guild, member, message)
        except Exception:
            log.exception(
                "Unhandled exception in action.on_add for guild %s message %s emoji %s user %s",
                getattr(guild, "id", None),
                msg_id,
                emoji_key,
                getattr(member, "id", None),
            )

    async def handle_unreact(self, payload: discord.RawReactionActionEvent) -> None:
        """Handles the logic for when a reaction is removed from a message."""
        if payload.guild_id is None:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return

        # Mirror of handle_react: load config, locate an emoji key that
        # matches the payload, and instantiate the appropriate action.
        cfg = await self.config.guild(guild).reactions()
        msg_id = str(payload.message_id)
        msg_map = cfg.get(msg_id)
        if not isinstance(msg_map, dict):
            return

        emoji_key = None
        for key in msg_map.keys():
            try:
                if self._emoji_key_matches(key, payload.emoji):
                    emoji_key = key
                    break
            except Exception:
                continue
        if emoji_key is None:
            return

        # If this unreact was initiated by the bot (we suppressed it),
        # ignore it and clear the suppression entry.
        try:
            sup = self._suppressed_unreacts.get(guild.id) if hasattr(self, "_suppressed_unreacts") else None
            if sup and (msg_id, emoji_key, int(payload.user_id)) in sup:
                try:
                    sup.discard((msg_id, emoji_key, int(payload.user_id)))
                except Exception:
                    pass
                return
        except Exception:
            pass

        raw_action = msg_map.get(emoji_key)
        if not isinstance(raw_action, dict):
            log.warning(
                "Invalid action_data for guild %s message %s emoji_key %s: %r",
                getattr(guild, "id", None),
                msg_id,
                emoji_key,
                raw_action,
            )
            return
        action_data = raw_action or {}
        action_type = action_data.get("type", "standard")
        try:
            action = ReactionAction.create(action_type, action_data)
        except KeyError:
            await self.bot.send_to_owners(
                f"ReactionAction type '{action_type}' not found for message {msg_id} emoji_key {emoji_key} in guild {guild.id}"
            )
            return

        # Resolve the member (may not be present in raw removal payloads).
        member = guild.get_member(payload.user_id)
        if member is None:
            try:
                member = await guild.fetch_member(payload.user_id)
            except Exception:
                member = None

        channel = guild.get_channel(payload.channel_id) if payload.channel_id else None
        message = None
        if channel is not None:
            try:
                message = await channel.fetch_message(payload.message_id)
            except Exception:
                message = None

        try:
            await action.on_remove(self, payload, guild, member, message)
        except Exception:
            log.exception(
                "Unhandled exception in action.on_remove for guild %s message %s emoji %s user %s",
                getattr(guild, "id", None),
                msg_id,
                emoji_key,
                getattr(member, "id", None),
            )

    @tasks.loop(seconds=60.0)
    async def _timed_role_cleanup(self) -> None:
        """Background loop: remove expired timed roles.

        Runs every 60 seconds. The loop scans each guild's `timed_roles`
        config namespace for entries whose expiration timestamp is <=
        current time, attempts to remove the role from the user, and
        persists the updated `timed_roles` store.
        """
        now = int(time.time())
        # iterate only guilds that have scheduled timed roles (fast path)
        for guild_id in list(self._timed_guilds):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                # guild not available; drop from tracking set
                self._timed_guilds.discard(guild_id)
                continue

            lock = self._get_timed_lock(guild_id)
            removals: list[tuple[int, int]] = []
            changed = False

            # Read and mutate the timed_roles store under the per-guild lock,
            # but avoid performing network I/O (role removal) while holding it.
            try:
                async with lock:
                    try:
                        timed = await self.config.guild(guild).timed_roles()
                    except Exception:
                        log.exception("Failed to read timed_roles for guild %s", guild_id)
                        continue

                    if not isinstance(timed, dict) or not timed:
                        # nothing scheduled; stop tracking this guild
                        self._timed_guilds.discard(guild_id)
                        continue

                    # timed structure: { user_id_str: { role_id_str: expires_unix, ... }, ... }
                    for user_str, role_map in list(timed.items()):
                        if not isinstance(role_map, dict):
                            del timed[user_str]
                            changed = True
                            continue

                        for role_str, expires in list(role_map.items()):
                            try:
                                expires_int = int(expires)
                            except Exception:
                                # malformed entry; remove it
                                del role_map[role_str]
                                changed = True
                                continue

                            if expires_int <= now:
                                # schedule removal to be executed after releasing the lock
                                try:
                                    user_id = int(user_str)
                                except Exception:
                                    user_id = None
                                try:
                                    role_id = int(role_str) if role_str.isdigit() else None
                                except Exception:
                                    role_id = None

                                if user_id is not None and role_id is not None:
                                    removals.append((user_id, role_id))

                                # remove entry from the store
                                del role_map[role_str]
                                changed = True

                        if not role_map:
                            timed.pop(user_str, None)
                        else:
                            timed[user_str] = role_map

                    if changed:
                        try:
                            await self.config.guild(guild).timed_roles.set(timed)
                        except Exception:
                            log.exception("Failed to persist timed_roles for guild %s", guild_id)

                    # If nothing remains after cleanup, stop tracking the guild
                    if not isinstance(timed, dict) or not timed:
                        self._timed_guilds.discard(guild_id)
            except Exception:
                log.exception("Error while processing timed role cleanup for guild %s", guild_id)
                continue

            # Execute removals outside the lock to avoid blocking other schedulers.
            many = len(removals) > 5
            for user_id, role_id in removals:
                member = None
                try:
                    member = guild.get_member(user_id) or await guild.fetch_member(user_id)
                except Exception:
                    member = None

                role = guild.get_role(role_id)
                if member is not None and role is not None:
                    try:
                        await member.remove_roles(role, reason="Timed role expired")
                    except Exception:
                        log.exception(
                            "Failed removing timed role %s from user %s in guild %s",
                            role_id,
                            user_id,
                            guild_id,
                        )
                # Small throttle to reduce risk of hitting rate limits when
                # processing many removals at once.
                if many:
                    try:
                        await asyncio.sleep(0.15)
                    except Exception:
                        pass

    @_timed_role_cleanup.before_loop
    async def _before_timed_role_cleanup(self) -> None:
        # Ensure the bot is ready before the first run
        await self.bot.wait_until_ready()

        # Rebuild the in-memory set of guilds that have timed roles so
        # the cleanup loop can iterate only those guilds.
        for guild in list(self.bot.guilds):
            try:
                timed = await self.config.guild(guild).timed_roles()
            except Exception:
                continue
            if isinstance(timed, dict) and timed:
                # basic existence check; entries may be cleaned by the loop
                self._timed_guilds.add(guild.id)

    async def schedule_timed_role(
            self, 
            guild: discord.Guild, 
            user_id: int, 
            role_id: int, 
            duration_seconds: int
        ) -> None:
        """Schedule a role to be removed after `duration_seconds` seconds.

        Writes an entry into the `timed_roles` guild config. The scheduler
        loop will remove the role when the expiration is reached.
        """
        expires = int(time.time()) + int(duration_seconds)
        if guild is None:
            return

        lock = self._get_timed_lock(guild.id)
        async with lock:
            try:
                timed = await self.config.guild(guild).timed_roles()
            except Exception:
                timed = {}

            if not isinstance(timed, dict):
                timed = {}

            user_key = str(user_id)
            role_key = str(role_id)
            user_map = timed.get(user_key, {}) or {}
            user_map[role_key] = expires
            timed[user_key] = user_map

            try:
                await self.config.guild(guild).timed_roles.set(timed)
                # keep the in-memory index current
                if timed:
                    self._timed_guilds.add(guild.id)
            except Exception:
                log.exception(
                    "Failed to schedule timed role %s for user %s in guild %s",
                    role_id,
                    user_id,
                    getattr(guild, "id", None),
                )

    async def unschedule_timed_role(self, guild: discord.Guild, user_id: int, role_id: int) -> None:
        """Remove a scheduled timed-role entry if present."""
        if guild is None:
            return

        lock = self._get_timed_lock(guild.id)
        async with lock:
            try:
                timed = await self.config.guild(guild).timed_roles()
            except Exception:
                return

            if not isinstance(timed, dict):
                return

            user_key = str(user_id)
            role_key = str(role_id)
            user_map = timed.get(user_key)
            if not user_map or not isinstance(user_map, dict):
                return

            if role_key in user_map:
                user_map.pop(role_key, None)

            if not user_map:
                timed.pop(user_key, None)
            else:
                timed[user_key] = user_map

            try:
                await self.config.guild(guild).timed_roles.set(timed)
                if not timed:
                    # no scheduled entries remain for this guild
                    self._timed_guilds.discard(guild.id)
            except Exception:
                log.exception(
                    "Failed to unschedule timed role %s for user %s in guild %s",
                    role_id,
                    user_id,
                    getattr(guild, "id", None),
                )

    async def cog_unload(self) -> None:
        """Stop background tasks when the cog is unloaded."""
        try:
            self._timed_role_cleanup.cancel()
        except Exception:
            pass
        try:
            if getattr(self, "_unreact_worker_task", None):
                try:
                    self._unreact_worker_task.cancel()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if getattr(self, "_role_update_worker_task", None):
                try:
                    self._role_update_worker_task.cancel()
                except Exception:
                    pass
        except Exception:
            pass

    