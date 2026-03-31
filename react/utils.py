"""Utility helpers for the react cog.

Small, import-safe helpers used by both the admin command and the
embed-driven wizard to render the same "types" output.
"""
from typing import Dict, List, Type

import discord
import re
from typing import Optional, Tuple, List
from typing import Union

# Prefer the `emoji` package when available for robust emoji detection.
try:
    import emoji as _emoji  # type: ignore
    _has_emoji = True
except Exception:
    _emoji = None
    _has_emoji = False


def _emoji_regex() -> re.Pattern:
    return re.compile(
        r"[\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF]+",
        flags=re.UNICODE,
    )


async def validate_emoji(bot: Optional[discord.Client], guild: Optional[discord.Guild], emoji_str: str) -> Tuple[bool, Optional[str]]:
    """Return (is_valid, reason_or_None) for the provided emoji string.

    Uses the `emoji` package when available to detect unicode emoji
    (including variation selectors and sequences). Custom Discord
    emoji are still validated via the bot/guild caches.
    """
    if not emoji_str or not emoji_str.strip():
        return False, "emoji is empty"

    s = str(emoji_str).strip()
    emoji_regex = _emoji_regex()

    # First, try parsing as a Discord partial emoji (handles <:name:id> and <a:name:id>)
    try:
        pe = discord.PartialEmoji.from_str(s)
    except Exception:
        pe = None

    # If it's a custom emoji mention with an id, validate access
    if pe is not None and getattr(pe, "id", None) is not None:
        try:
            eid = int(pe.id)
        except Exception:
            return False, "invalid custom emoji id"
        try:
            if guild is not None and guild.get_emoji(eid) is not None:
                return True, None
        except Exception:
            pass
        if bot is not None and bot.get_emoji(eid) is not None:
            return True, None
        return False, "custom emoji id not found or inaccessible to this bot"

    # Use the emoji package if available for robust unicode/sequence detection
    if _has_emoji and _emoji is not None:
        try:
            if hasattr(_emoji, "emoji_list"):
                if _emoji.emoji_list(s):
                    return True, None
            # older/newer variants might expose distinct_emoji_list
            if hasattr(_emoji, "distinct_emoji_list"):
                if _emoji.distinct_emoji_list(s):
                    return True, None
            # fallback: EMOJI_DATA dict contains known emoji in some versions
            if hasattr(_emoji, "EMOJI_DATA"):
                for ch in s:
                    if ch in _emoji.EMOJI_DATA:
                        return True, None
        except Exception:
            # if the emoji package misbehaves, continue to other heuristics
            pass

    # If the input contains a bare numeric id, try that as a custom emoji id
    m = re.search(r"(\d{17,21})", s)
    if m:
        try:
            eid = int(m.group(1))
            if guild is not None and guild.get_emoji(eid) is not None:
                return True, None
            if bot is not None and bot.get_emoji(eid) is not None:
                return True, None
            return False, "custom emoji id not found or inaccessible to this bot"
        except Exception:
            pass

    # Fallback heuristics: regex or high-codepoint characters
    if emoji_regex.search(s) or any(ord(ch) >= 0x1F000 for ch in s):
        return True, None

    return False, "not a valid emoji"


def resolve_roles_from_guild(guild: Optional[discord.Guild], tokens: List[str]) -> Tuple[List[str], List[str]]:
    """Resolve tokens to role IDs where possible.

    Returns (resolved_ids_as_str, unresolved_tokens).
    """
    resolved: List[str] = []
    unresolved: List[str] = []
    for token in tokens:
        mm = re.search(r"\d{17,21}", token)
        if mm:
            try:
                rid = int(mm.group(0))
                role_obj = guild.get_role(rid) if guild is not None else None
                if role_obj is not None:
                    resolved.append(str(role_obj.id))
                else:
                    unresolved.append(token)
            except Exception:
                unresolved.append(token)
        else:
            role_obj = discord.utils.get(guild.roles, name=token) if guild is not None else None
            if role_obj is not None:
                resolved.append(str(role_obj.id))
            else:
                unresolved.append(token)
    return resolved, unresolved


def build_types_embeds(registry: Dict[str, Type], title: str = "Available Action Types") -> List[discord.Embed]:
    """Build one or more embeds listing registered action types.

    The function mirrors the presentation used by the `reactrole types`
    command so callers (commands and UI views) can show identical output.
    Returns a list of `discord.Embed` objects (one or more chunks).
    """
    items: list[tuple[str, str, list[str]]] = []
    for name in sorted(registry.keys()):
        cls = registry[name]
        desc = getattr(cls, "description", None)
        if not desc:
            doc = (cls.__doc__ or "").strip().splitlines()
            desc = doc[0].strip() if doc else "(no description)"
        keys = getattr(cls, "options", []) or []
        items.append((name, desc, list(keys)))

    if not items:
        return []

    lines: list[str] = []
    for name, desc, keys in items:
        if keys:
            opts_lines = "\n".join(f"    • `{k}`" for k in keys)
            opts_text = f"\n    __Options__:\n{opts_lines}"
        else:
            opts_text = ""

        lines.append(f"`{name}`: {desc}{opts_text}\n")

    # Chunk into safe embed description sizes
    MAX_DESC = 4000
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for line in lines:
        if cur_len + len(line) + 1 > MAX_DESC:
            chunks.append("\n".join(cur))
            cur = [line]
            cur_len = len(line) + 1
        else:
            cur.append(line)
            cur_len += len(line) + 1
    if cur:
        chunks.append("\n".join(cur))

    embeds: list[discord.Embed] = []
    for idx, ch in enumerate(chunks):
        title_text = title if idx == 0 else f"{title} (cont.)"
        embed = discord.Embed(title=title_text, description=ch)
        embeds.append(embed)

    return embeds


async def find_message_channel(
    guild: Optional[discord.Guild],
    message_id: int,
    bot_member: Optional[discord.Member] = None,
    message_channels: Optional[dict] = None,
    concurrency: int = 8,
) -> Tuple[Optional[int], Optional[discord.Message]]:
    """Locate the channel containing ``message_id`` and return (channel_id, message).

    - If ``message_channels`` (a mapping) contains an entry for this message id,
      it is tried first for a fast path.
    - Otherwise the function will scan the guild's text channels using up to
      ``concurrency`` concurrent fetches and return the first successful result.
    Returns ``(None, None)`` when the message cannot be found or access is
    blocked.
    """
    if guild is None:
        return None, None

    # Fast path: check provided mapping
    try:
        if message_channels:
            key = str(message_id)
            ch_id = message_channels.get(key) if isinstance(message_channels, dict) else None
            if ch_id is None:
                # try numeric key
                ch_id = message_channels.get(message_id) if isinstance(message_channels, dict) else None
            if ch_id is not None:
                try:
                    ch = guild.get_channel(int(ch_id))
                    if ch is not None:
                        try:
                            msg = await ch.fetch_message(message_id)
                            return int(ch_id), msg
                        except Exception:
                            # fall through to scanning
                            pass
                except Exception:
                    pass
    except Exception:
        pass

    # Concurrent scan across text channels
    sem = asyncio.Semaphore(concurrency)

    async def _try_fetch(ch: discord.abc.Messageable):
        try:
            if bot_member is not None:
                try:
                    if not ch.permissions_for(bot_member).read_messages:
                        return None
                except Exception:
                    pass
            async with sem:
                msg = await ch.fetch_message(message_id)
                return ch.id, msg
        except discord.NotFound:
            return None
        except (discord.Forbidden, discord.HTTPException):
            return None
        except Exception:
            return None

    tasks = []
    for ch in getattr(guild, "text_channels", []):
        tasks.append(asyncio.create_task(_try_fetch(ch)))

    if not tasks:
        return None, None

    try:
        for fut in asyncio.as_completed(tasks):
            try:
                res = await fut
            except asyncio.CancelledError:
                continue
            if res:
                # cancel remaining tasks
                for t in tasks:
                    if not t.done():
                        t.cancel()
                return res
    finally:
        for t in tasks:
            if not t.done():
                try:
                    t.cancel()
                except Exception:
                    pass

    return None, None
