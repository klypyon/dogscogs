"""Action classes for the React cog.

This module provides the `ReactionAction` base class and the built-in
action implementations used by the `react` cog. Each action encapsulates
the behaviour performed when a configured reaction is added or removed
from a message.

Key concepts
- Action: a small object with two async hooks, ``on_add`` and
  ``on_remove``, invoked by the cog when a reaction is added or
  removed.
- Registry: actions are registered by name using the
  ``@ReactionAction.register("type")`` decorator. The cog uses
  ``ReactionAction.create(type, cfg)`` to instantiate actions.

Config (guild-level)

The cog expects a guild-level mapping called ``reactions`` with this
shape::

    {
        "<message_id>": {
            "<emoji>": { "type": "standard", "roles": ["<role_id>"] },
            "🔥": { "type": "timed", "roles": ["<role_id>"], "duration": 3600 }
        }
    }

Built-in action types
- ``standard`` — add configured roles on add; remove them on unreact.
- ``permanent_add`` — add roles on add; do not remove on unreact.
- ``permanent_remove`` — remove roles on add; do not re-add on unreact.
- ``reverse`` — remove roles on add; re-add them on unreact.
- ``grouped`` — mutually-exclusive group behaviour; adding a group's
  reaction removes roles defined by other reactions in the same group.
- ``timed`` — adds roles then schedules their removal after ``duration``
  seconds. If ``remove_on_unreact`` is truthy (default True) unreact
  will remove them immediately and cancel scheduling.

Notes for implementors
- Actions receive the cog instance as the first argument to their
  hooks. Use the cog helpers (for example, ``resolve_manageable_roles``,
  ``schedule_timed_role`` and ``unschedule_timed_role``) instead of
  reimplementing permission/hierarchy checks.
- ``self.cfg`` is a deep-copied mapping of the action's config and
  should be treated as read-only by action implementations.
- Actions should be defensive: handle missing members/roles and log
  exceptions rather than letting errors bubble to the dispatcher.

Adding a new action

1. Subclass ``ReactionAction``.
2. Implement ``async def on_add(self, cog, payload, guild, member, message)``
   and ``async def on_remove(self, ...)``.
3. Register the class::

    @ReactionAction.register("my_type")
    class MyAction(ReactionAction):
        ...

The cog will call ``ReactionAction.create("my_type", cfg)`` to
instantiate your action when it encounters that type in the guild
config.
"""

from typing import Any, Dict, Type, Optional, TYPE_CHECKING
from abc import ABC, abstractmethod
import logging
import copy

import discord
import asyncio
from .utils import find_message_channel

if TYPE_CHECKING:
    # Import the cog only for type-checkers to avoid runtime circular
    # imports; action implementations use the cog instance passed into
    # their hooks rather than importing it directly.
    from .react import React  # noqa: F401

log = logging.getLogger(__name__)


class ReactionAction(ABC):
    """Abstract base for all reaction actions.

    Implementations MUST:
    - Not mutate ``self.cfg`` (it's a deep-copied snapshot).
    - Be resilient to missing members/roles.
    - Use the cog's helpers for permission-aware role resolution and
      scheduling.

    Registry
    - Use ``@ReactionAction.register("name")`` to register a subclass.
    - Use ``ReactionAction.create(name, cfg)`` to instantiate.
    """

    registry: Dict[str, Type["ReactionAction"]] = {}

    def __init__(self, cfg: Dict[str, Any]) -> None:
        try:
            self.cfg = copy.deepcopy(cfg or {})
        except Exception:
            log.warning("ReactionAction.__init__: deepcopy failed, falling back to shallow copy: %r", cfg)
            try:
                self.cfg = dict(cfg or {})
            except Exception:
                self.cfg = {}

    @staticmethod
    def _parse_bool(val: Any, default: bool = True) -> bool:
        """Return a robust boolean interpretation of ``val``.

        Accepts booleans, numeric-like values and common strings like
        ``"true"``, ``"false"``, ``"1"``, ``"0"``. Falls back to
        ``default`` for unrecognised values.
        """
        if isinstance(val, bool):
            return val
        if val is None:
            return default
        if isinstance(val, str):
            v = val.strip().lower()
            if v in ("1", "true", "yes", "y", "on"):
                return True
            if v in ("0", "false", "no", "n", "off"):
                return False
            return default
        try:
            return bool(int(val))
        except Exception:
            return default

    def _normalize_role_ids(self, key: str = "roles") -> list:
        """Normalize a config entry to a list of role-id strings.

        Accepts: None, scalar int/str, list/tuple/set of ids, or a dict
        (where keys are treated as ids). Returns ``[]`` for missing
        values.
        """
        v = self.cfg.get(key, [])
        if v is None:
            return []
        if isinstance(v, (list, tuple, set)):
            return [str(x) for x in v if x is not None]
        if isinstance(v, dict):
            return [str(k) for k in v.keys()]
        return [str(v)]

    async def _notify_member(
        self,
        member: Optional[discord.Member],
        guild: discord.Guild,
        *,
        added: Optional[list] = None,
        removed: Optional[list] = None,
        message: Optional[discord.Message] = None,
        emoji: Optional[object] = None,
        extra: Optional[str] = None,
    ) -> None:
        """Send a brief DM to ``member`` describing added/removed roles.

        This is best-effort and failures (for example, DMs disabled) are
        swallowed after logging.
        """
        if member is None:
            return

        try:
            parts: list[str] = []
            try:
                gname = getattr(guild, "name", str(getattr(guild, "id", "?")))
            except Exception:
                gname = str(getattr(guild, "id", "?"))

            if added:
                try:
                    names = ", ".join(getattr(r, "name", str(r)) for r in added)
                except Exception:
                    names = ", ".join(str(getattr(r, "id", r)) for r in added)
                parts.append(f"You were given role{'s' if len(added) != 1 else ''} in {gname}: {names}")

            if removed:
                try:
                    names = ", ".join(getattr(r, "name", str(r)) for r in removed)
                except Exception:
                    names = ", ".join(str(getattr(r, "id", r)) for r in removed)
                parts.append(f"Role{'s' if len(removed) != 1 else ''} removed in {gname}: {names}")

            if extra:
                parts.append(str(extra))

            if message is not None and getattr(message, "channel", None) is not None:
                try:
                    ch = message.channel
                    link = f"https://discord.com/channels/{getattr(guild, 'id', '')}/{getattr(ch, 'id', '')}/{getattr(message, 'id', '')}"
                    parts.append(f"Message: {link}")
                except Exception:
                    pass

            if emoji is not None:
                try:
                    parts.append(f"Emoji: {str(emoji)}")
                except Exception:
                    pass

            if not parts:
                return

            content = "\n".join(parts)
            try:
                await member.send(content)
            except discord.Forbidden:
                # user has DMs closed or blocked the bot
                return
            except Exception:
                log.exception("Failed to DM user %s in guild %s", getattr(member, "id", None), getattr(guild, "id", None))
        except Exception:
            log.exception("Error constructing DM for user %s in guild %s", getattr(member, "id", None), getattr(guild, "id", None))

    @classmethod
    def register(cls, name: str):
        """Decorator to register an action implementation.

        Example::

            @ReactionAction.register("my_type")
            class MyAction(ReactionAction):
                ...
        """

        def _decorator(subclass: Type["ReactionAction"]):
            cls.registry[name] = subclass
            return subclass

        return _decorator

    @classmethod
    def create(cls, name: str, cfg: Dict[str, Any]) -> "ReactionAction":
        """Instantiate a registered action by ``name``.

        Raises ``KeyError`` if no action is registered under ``name``.
        The provided ``cfg`` is copied to avoid accidental shared
        mutations.
        """
        sub = cls.registry.get(name)
        if sub is None:
            raise KeyError(f"No ReactionAction registered under '{name}'")
        try:
            cfg_dict = dict(cfg or {})
        except Exception:
            log.warning("ReactionAction.create: action config for '%s' is not a mapping, using empty dict: %r", name, cfg)
            cfg_dict = {}
        return sub(cfg_dict)

    @abstractmethod
    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        """Handle a reaction being added.

        ``cog`` is the React cog instance; use it for helpers such as
        role resolution and scheduling. ``payload`` is the raw reaction
        event; ``guild`` and ``member`` may be ``None`` in edge cases and
        implementations must handle that.
        """
        raise NotImplementedError

    @abstractmethod
    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        """Handle a reaction being removed.

        ``member`` may be ``None`` if the user cannot be resolved; actions
        should be resilient to that.
        """
        raise NotImplementedError


@ReactionAction.register("standard")
class StandardAction(ReactionAction):
    """Default behaviour: add roles on add, remove on unreact.

    Config keys:
    - ``roles``: list/str/dict of role ids (see ``_normalize_role_ids``)
    """
    description = "Add roles on react; remove on unreact."
    options = ["roles"]
    sample = "reactrole add <message_id> 👍 standard <role_id>"

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("StandardAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.add_roles(*roles, reason=f"react role add ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("StandardAction.on_add failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, added=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after StandardAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [getattr(r, "id", r) for r in roles], [])
        except Exception:
            log.exception("Failed to enqueue role update after StandardAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("StandardAction: no manageable roles found for requested roles %s in guild %s on remove", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.remove_roles(*roles, reason=f"react role remove ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("StandardAction.on_remove failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, removed=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after StandardAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [], [getattr(r, "id", r) for r in roles])
        except Exception:
            log.exception("Failed to enqueue role update after StandardAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))


@ReactionAction.register("permanent_add")
class PermanentAddAction(ReactionAction):
    """Add roles on add; do not remove them on unreact.

    Config keys:
    - ``roles``: same format as ``standard``
    """

    # Metadata for help generation
    description = "Add roles on react; do not remove on unreact."
    options = ["roles"]
    sample = "reactrole add <message_id> 👍 permanent_add <role_id>"

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("PermanentAddAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.add_roles(*roles, reason=f"permanent add via reaction ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("PermanentAddAction.on_add failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, added=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after PermanentAddAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [getattr(r, "id", r) for r in roles], [])
        except Exception:
            log.exception("Failed to enqueue role update after PermanentAddAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        return


@ReactionAction.register("permanent_remove")
class PermanentRemoveAction(ReactionAction):
    """Remove roles on add; do not re-add on unreact.

    Also cancels any scheduled timed removals for the affected roles
    for the user.
    Config keys:
    - ``roles``
    """

    description = "Remove roles on react; do not re-add on unreact."
    options = ["roles"]
    sample = "reactrole add <message_id> 👍 permanent_remove <role_id>"

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("PermanentRemoveAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.remove_roles(*roles, reason=f"permanent remove via reaction ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("PermanentRemoveAction.on_add failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, removed=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after PermanentRemoveAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

        for role in roles:
            try:
                await cog.unschedule_timed_role(guild, member.id, role.id)
            except Exception:
                log.exception("Failed to unschedule timed role %s for user %s in guild %s", getattr(role, "id", None), getattr(member, "id", None), getattr(guild, "id", None))

        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [], [getattr(r, "id", r) for r in roles])
        except Exception:
            log.exception("Failed to enqueue role update after ReverseAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [], [getattr(r, "id", r) for r in roles])
        except Exception:
            log.exception("Failed to enqueue role update after PermanentRemoveAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        return


@ReactionAction.register("reverse")
class ReverseAction(ReactionAction):
    """Remove roles on add; re-add them on unreact.

    Config keys:
    - ``roles``
    """

    description = "Remove roles on react; re-add them on unreact."
    options = ["roles"]
    sample = "reactrole add <message_id> 👍 reverse <role_id>"

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("ReverseAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.remove_roles(*roles, reason=f"reverse action remove on add ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("ReverseAction.on_add failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, removed=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after ReverseAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

        for role in roles:
            try:
                await cog.unschedule_timed_role(guild, member.id, role.id)
            except Exception:
                log.exception("Failed to unschedule timed role %s for user %s in guild %s", getattr(role, "id", None), getattr(member, "id", None), getattr(guild, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("ReverseAction: no manageable roles found for requested roles %s in guild %s on remove", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.add_roles(*roles, reason=f"reverse action add on remove ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("ReverseAction.on_remove failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            try:
                await self._notify_member(member, guild, added=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after ReverseAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [getattr(r, "id", r) for r in roles], [])
        except Exception:
            log.exception("Failed to enqueue role update after ReverseAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))


@ReactionAction.register("grouped")
class GroupedAction(ReactionAction):
    """Grouped (mutually-exclusive) action.

    Config keys:
    - ``group``: string identifier shared by reactions that belong to the
      same mutual-exclusion set.
    - ``roles``: role ids to grant for this reaction.

    Behaviour: when a reaction in a group is added, roles assigned by
    other reactions in the same group are removed from the user.
    """

    description = "Mutually-exclusive group behaviour; adding a group's reaction removes roles from other group members."
    options = ["roles", "group"]
    sample = "reactrole add <message_id> <:emote:123456789012345678> grouped <role_id> group=color"

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        group = self.cfg.get("group")
        role_ids = self._normalize_role_ids()
        if member is None:
            return

        to_remove_ids = set()
        if group:
            try:
                cfg = await cog.config.guild(guild).reactions()
            except Exception:
                cfg = {}

            for other_msg, emojis in (cfg or {}).items():
                if not isinstance(emojis, dict):
                    continue
                for other_emoji, other_data in emojis.items():
                    if not isinstance(other_data, dict):
                        continue
                    if other_data.get("group") != group:
                        continue
                    if other_msg == str(payload.message_id) and other_emoji == str(payload.emoji):
                        continue
                    for rid in other_data.get("roles", []):
                        try:
                            to_remove_ids.add(int(rid))
                        except Exception:
                            continue

        if to_remove_ids:
            to_remove_roles = await cog.resolve_manageable_roles(guild, [str(r) for r in to_remove_ids])
        else:
            to_remove_roles = []

        removed_successful: list = []
        added_successful: list = []
        if to_remove_roles:
            try:
                await member.remove_roles(*to_remove_roles, reason=f"grouped action: enforce group {group}", atomic=True)
            except Exception:
                log.exception("Failed to remove group roles for user %s in guild %s", getattr(member, "id", None), getattr(guild, "id", None))
            else:
                removed_successful = to_remove_roles
            for role in to_remove_roles:
                try:
                    await cog.unschedule_timed_role(guild, member.id, role.id)
                except Exception:
                    log.exception("Failed to unschedule timed role %s for user %s in guild %s", getattr(role, "id", None), getattr(member, "id", None), getattr(guild, "id", None))
            # Enqueue a batched role-update for eventual reconciliation of removed group roles
            try:
                if removed_successful:
                    cog.enqueue_role_update(guild.id, member.id, [], [getattr(r, "id", r) for r in removed_successful])
            except Exception:
                log.exception("Failed to enqueue role update for grouped removals in guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

        # Enqueue background unreact tasks for other group mappings. The
        # worker will locate messages and perform removals with retries and
        # backoff so we do not limit the number of reactions here.
        if group:
            try:
                cfg = await cog.config.guild(guild).reactions()
            except Exception:
                cfg = {}

            payload_msg_id = str(getattr(payload, "message_id", ""))
            payload_emoji_str = None
            try:
                payload_emoji_str = str(getattr(payload, "emoji", ""))
            except Exception:
                payload_emoji_str = None

            for other_msg, emojis in (cfg or {}).items():
                if not isinstance(emojis, dict):
                    continue
                for other_emoji, other_data in emojis.items():
                    if not isinstance(other_data, dict):
                        continue
                    if other_data.get("group") != group:
                        continue
                    # Skip the reaction that triggered this action
                    try:
                        if other_msg == payload_msg_id and payload_emoji_str is not None:
                            try:
                                if other_emoji == payload_emoji_str or str(discord.PartialEmoji.from_str(other_emoji)) == payload_emoji_str:
                                    continue
                            except Exception:
                                if other_emoji == payload_emoji_str:
                                    continue
                    except Exception:
                        pass

                    # Enqueue the removal task; worker will perform the actual
                    # fetch+remove and call `_suppress_unreact` as needed.
                    try:
                        try:
                            cog.enqueue_unreact(guild.id, other_msg, other_emoji, member.id)
                        except Exception:
                            # fallback to directly pushing into the queue
                            try:
                                cog._unreact_queue.put_nowait((guild.id, str(other_msg), str(other_emoji), member.id))
                            except Exception:
                                log.exception("Failed to enqueue unreact task for message %s emoji %s user %s guild %s", other_msg, other_emoji, getattr(member, "id", None), getattr(guild, "id", None))
                    except Exception:
                        log.exception("Failed preparing unreact task for message %s emoji %s user %s guild %s", other_msg, other_emoji, getattr(member, "id", None), getattr(guild, "id", None))

        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles:
            if role_ids and not roles:
                log.warning("GroupedAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.add_roles(*roles, reason=f"grouped action add ({group})", atomic=True)
        except Exception:
            log.exception("Failed to add grouped roles for user %s in guild %s", getattr(member, "id", None), getattr(guild, "id", None))
        else:
            added_successful = roles

        # Enqueue a batched role-update for eventual reconciliation of added group roles
        try:
            if added_successful:
                cog.enqueue_role_update(guild.id, member.id, [getattr(r, "id", r) for r in added_successful], [])
        except Exception:
            log.exception("Failed to enqueue role update for grouped additions in guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

        if removed_successful or added_successful:
            try:
                await self._notify_member(member, guild, added=(added_successful or None), removed=(removed_successful or None), message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after GroupedAction in guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("GroupedAction: no manageable roles found for requested roles %s in guild %s on remove", role_ids, getattr(guild, "id", None))
            return
        try:
            await member.remove_roles(*roles, reason=f"grouped action remove ({self.cfg.get('group')})", atomic=True)
        except Exception:
            log.exception("Failed to remove grouped roles for user %s in guild %s", getattr(member, "id", None), getattr(guild, "id", None))
        else:
            try:
                await self._notify_member(member, guild, removed=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after GroupedAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))


@ReactionAction.register("timed")
class TimedAction(ReactionAction):
    """Timed action: grant roles and schedule their removal.

    Config keys:
    - ``roles``: role ids
    - ``duration``: seconds until expiration (int)
    - ``remove_on_unreact``: optional bool (defaults to True)

    Behaviour: adds roles immediately and, if ``duration`` is positive,
    schedules removal via the cog's scheduler. If ``remove_on_unreact`` is
    true the role will be removed immediately on unreact and any
    scheduled removal will be cancelled.
    """

    description = "Add roles and schedule removal after `duration` seconds. `remove_on_unreact` controls immediate removal on unreact."
    options = ["roles", "duration", "remove_on_unreact"]
    sample = "reactrole add <message_id> 🔥 timed <role_id> duration=3600"

    def __init__(self, cfg: Dict[str, Any]) -> None:
        super().__init__(cfg)
        self.remove_on_unreact = self._parse_bool(self.cfg.get("remove_on_unreact", True), True)

    async def on_add(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: discord.Member, message: Optional[discord.Message]) -> None:
        role_ids = self._normalize_role_ids()
        duration = self.cfg.get("duration")
        try:
            duration = int(duration)
        except Exception:
            duration = None

        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            if role_ids and not roles:
                log.warning("TimedAction: no manageable roles found for requested roles %s in guild %s", role_ids, getattr(guild, "id", None))
            return

        try:
            await member.add_roles(*roles, reason=f"timed react role add ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("TimedAction.on_add failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            if duration and duration > 0:
                for role in roles:
                    try:
                        await cog.schedule_timed_role(guild, member.id, role.id, duration)
                    except Exception:
                        log.exception("Failed to schedule timed removal for role %s user %s guild %s", role.id, getattr(member, "id", None), getattr(guild, "id", None))
            extra = None
            if duration and duration > 0:
                extra = f"These role(s) will expire in {duration} seconds."
            try:
                await self._notify_member(member, guild, added=roles, message=message, emoji=getattr(payload, "emoji", None), extra=extra)
            except Exception:
                log.exception("Failed to DM user after TimedAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [getattr(r, "id", r) for r in roles], [])
        except Exception:
            log.exception("Failed to enqueue role update after TimedAction.on_add for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))

    async def on_remove(self, cog: "React", payload: discord.RawReactionActionEvent, guild: discord.Guild, member: Optional[discord.Member], message: Optional[discord.Message]) -> None:
        if not self.remove_on_unreact:
            return

        role_ids = self._normalize_role_ids()
        roles = await cog.resolve_manageable_roles(guild, role_ids)
        if not roles or member is None:
            return

        try:
            await member.remove_roles(*roles, reason=f"timed react role remove ({payload.emoji})", atomic=True)
        except Exception:
            log.exception("TimedAction.on_remove failed for guild %s, user %s", getattr(guild, "id", None), getattr(member, "id", None))
        else:
            for role in roles:
                try:
                    await cog.unschedule_timed_role(guild, member.id, role.id)
                except Exception:
                    log.exception("Failed to unschedule timed role %s for user %s in guild %s", role.id, getattr(member, "id", None), getattr(guild, "id", None))
            try:
                await self._notify_member(member, guild, removed=roles, message=message, emoji=getattr(payload, "emoji", None))
            except Exception:
                log.exception("Failed to DM user after TimedAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
        # Enqueue a batched role-update for eventual reconciliation
        try:
            cog.enqueue_role_update(guild.id, member.id, [], [getattr(r, "id", r) for r in roles])
        except Exception:
            log.exception("Failed to enqueue role update after TimedAction.on_remove for guild %s user %s", getattr(guild, "id", None), getattr(member, "id", None))
