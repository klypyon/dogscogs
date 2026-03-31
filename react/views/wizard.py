"""Wizard view and helpers for react role configuration.

Contains `WizardState`, `ReactRoleWizardView`, and `run_reactrole_wizard`.
"""
import asyncio
import re
from typing import TYPE_CHECKING, Dict, Any
from dataclasses import dataclass, field

import discord
from redbot.core import commands

from ..actions import ReactionAction
from ..utils import build_types_embeds, validate_emoji, resolve_roles_from_guild, find_message_channel

if TYPE_CHECKING:
    from ..react import React


@dataclass
class WizardState:
    message_id: int | None = None
    emoji: str | None = None
    action_type: str | None = None
    opts: Dict[str, Any] = field(default_factory=dict)
    channel_id: int | None = None
    message_preview: str | None = None


class ReactRoleWizardView(discord.ui.View):
    def __init__(self, author_id: int, state: WizardState, action_registry: type[ReactionAction], timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.state = state
        self.action_registry = action_registry
        self.cancelled = False
        self.confirmed = False
        self.ready = False
        self.message_ref: discord.Message | None = None
        self.edit_request = False

    def get_embed(self) -> discord.Embed:
        emb = discord.Embed(title="ReactRole Wizard")
        # Message ID: try to show a jump link when we have channel_id and guild
        if self.state.message_id:
            msg_val = str(self.state.message_id)
            try:
                if self.state.channel_id is not None and self.message_ref is not None and getattr(self.message_ref, "guild", None) is not None:
                    guild_id = self.message_ref.guild.id
                    link = f"https://discord.com/channels/{guild_id}/{self.state.channel_id}/{self.state.message_id}"
                    msg_val = f"[Jump to message]({link}) ({self.state.message_id})"
            except Exception:
                msg_val = str(self.state.message_id)
        else:
            msg_val = "(not set)"
        emb.add_field(name="Message", value=msg_val)

        # Emoji: render unicode or custom emoji visually when possible
        if self.state.emoji:
            rendered = str(self.state.emoji)
            try:
                pe = discord.PartialEmoji.from_str(self.state.emoji)
                rendered = str(pe)
            except Exception:
                try:
                    # try to extract id and lookup in guild emojis
                    m = re.search(r"(\d{17,21})", str(self.state.emoji))
                    if m and self.message_ref is not None and getattr(self.message_ref, "guild", None) is not None:
                        eobj = self.message_ref.guild.get_emoji(int(m.group(1)))
                        if eobj is not None:
                            rendered = str(eobj)
                except Exception:
                    pass
            emb.add_field(name="Emoji", value=rendered)
        else:
            emb.add_field(name="Emoji", value="(not set)")

        # Action type formatted in backticks
        emb.add_field(name="Action Type", value=(f"`{self.state.action_type}`" if self.state.action_type else "(not set)"))

        # Consolidate option display into a single field to avoid hitting
        # embed field limits and to keep the embed compact.
        try:
            declared = []
            if self.state.action_type:
                cls = self.action_registry.registry.get(self.state.action_type)
                if cls is not None:
                    declared = getattr(cls, "options", []) or []

            opts_lines: list[str] = []
            guild = None
            if self.message_ref is not None:
                guild = getattr(self.message_ref, "guild", None)

            for opt_name in declared:
                val = self.state.opts.get(opt_name, None)
                if opt_name == "roles":
                    if val is None:
                        v_display = "(not set)"
                    else:
                        if isinstance(val, (list, tuple, set)):
                            tokens = [str(x) for x in val]
                        else:
                            tokens = str(val).split()
                        human_roles: list[str] = []
                        for token in tokens:
                            mm = re.search(r"(\d{17,21})", token)
                            if mm:
                                try:
                                    rid = int(mm.group(1))
                                    if guild is not None:
                                        role_obj = guild.get_role(rid)
                                        human_roles.append(role_obj.mention if role_obj is not None else str(rid))
                                    else:
                                        human_roles.append(str(rid))
                                    continue
                                except Exception:
                                    pass
                            if guild is not None:
                                role_obj = discord.utils.get(guild.roles, name=token)
                                human_roles.append(role_obj.mention if role_obj is not None else token)
                            else:
                                human_roles.append(token)
                        v_display = ", ".join(human_roles) if human_roles else str(val)
                else:
                    v_display = str(val) if val is not None else "(not set)"
                if len(v_display) > 1000:
                    v_display = v_display[:997] + "..."
                opts_lines.append(f"`{opt_name}`: {v_display}")

            if not opts_lines:
                emb.add_field(name="Options", value="(none)")
            else:
                emb.add_field(name="Options", value="\n".join(opts_lines)[:4000])
        except Exception:
            emb.add_field(name="Options", value=str(self.state.opts or {}) or "(none)")
        status = "Cancelled" if self.cancelled else "Ready to confirm" if self.ready else "In progress"
        emb.set_footer(text=f"Status: {status} — Cancel via the Cancel button or type `cancel` while answering prompts")
        return emb

    async def on_timeout(self) -> None:
        self.cancelled = True
        for child in self.children:
            child.disabled = True
        if self.message_ref is not None:
            allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
            try:
                # Remove the view from the message to prevent further interactions
                await self.message_ref.edit(embed=self.get_embed(), view=None, allowed_mentions=allowed)
            except Exception:
                try:
                    await self.message_ref.edit(embed=self.get_embed(), allowed_mentions=allowed)
                except Exception:
                    pass
        # Ensure view waiters are released
        try:
            self.stop()
        except Exception:
            pass

    async def _delete_after(self, message: discord.Message, delay: float) -> None:
        """Delete `message` after `delay` seconds, ignoring errors."""
        try:
            await asyncio.sleep(delay)
            await message.delete()
        except Exception:
            # Best-effort deletion; ignore failures (permissions, already deleted)
            return

    @discord.ui.button(label="Types", style=discord.ButtonStyle.secondary)
    async def types_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the wizard starter can use this.", ephemeral=True)
            return
        embeds = build_types_embeds(self.action_registry.registry)
        if not embeds:
            await interaction.response.send_message("No action types are registered.", ephemeral=True)
            return

        # Send non-ephemeral so we can delete the messages after a timeout.
        # Schedule deletion for each sent message after 30 seconds.
        try:
            await interaction.response.send_message(embed=embeds[0], ephemeral=False)
            try:
                msg = await interaction.original_response()
                asyncio.create_task(self._delete_after(msg, 30.0))
            except Exception:
                pass
        except Exception:
            # As a fallback, attempt an ephemeral reply if send_message fails.
            try:
                await interaction.response.send_message(embed=embeds[0], ephemeral=True)
            except Exception:
                pass

        for embed in embeds[1:]:
            try:
                msg = await interaction.followup.send(embed=embed, ephemeral=False)
                asyncio.create_task(self._delete_after(msg, 30.0))
            except Exception:
                try:
                    await interaction.followup.send(embed=embed, ephemeral=True)
                except Exception:
                    pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the wizard starter can cancel.", ephemeral=True)
            return
        self.cancelled = True
        for child in self.children:
            child.disabled = True
        allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
        try:
            await interaction.response.edit_message(embed=self.get_embed(), view=self, allowed_mentions=allowed)
        except Exception:
            try:
                await interaction.response.send_message("Wizard cancelled.")
            except Exception:
                pass
        self.stop()

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the wizard starter can confirm.", ephemeral=True)
            return
        if not self.ready:
            await interaction.response.send_message("Complete all steps before confirming.", ephemeral=True)
            return
        self.confirmed = True
        for child in self.children:
            child.disabled = True
        allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
        try:
            await interaction.response.edit_message(embed=self.get_embed(), view=self, allowed_mentions=allowed)
        except Exception:
            try:
                await interaction.response.send_message("Confirmed.")
            except Exception:
                pass
        self.stop()

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the wizard starter can edit.", ephemeral=True)
            return
        # re-enable children so the view is interactive again
        for child in self.children:
            child.disabled = False
        self.edit_request = True
        try:
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        except Exception:
            try:
                await interaction.response.send_message("Edit requested.", ephemeral=True)
            except Exception:
                pass
        # stop the view wait so the wizard function can resume and re-enter prompts
        try:
            self.stop()
        except Exception:
            pass

    @discord.ui.button(label="Show CLI", style=discord.ButtonStyle.secondary)
    async def cli_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the wizard starter can use this.", ephemeral=True)
            return
        # Build a CLI equivalent for the current state
        prefix = getattr(getattr(self, 'ctx', None), 'prefix', '?')
        mid = str(self.state.message_id or '')
        emoji = str(self.state.emoji or '')
        atype = str(self.state.action_type or 'standard')
        parts = [f"{prefix}reactrole add", mid, emoji, atype]
        opts = self.state.opts or {}
        # roles first
        if 'roles' in opts:
            roles = opts.get('roles') or []
            if isinstance(roles, (list, tuple, set)):
                parts.extend([str(x) for x in roles])
            else:
                parts.append(str(roles))
        for k, v in opts.items():
            if k == 'roles':
                continue
            parts.append(f"{k}={v}")
        cli = " ".join([p for p in parts if p])
        try:
            await interaction.response.send_message(f"CLI equivalent:\n{cli}", ephemeral=True)
        except Exception:
            try:
                await interaction.response.send_message("Unable to show CLI.", ephemeral=True)
            except Exception:
                pass


async def run_reactrole_wizard(cog: "React", ctx: commands.Context, initial_state: WizardState | None = None, author_id: int | None = None) -> bool:
    """Run an embed-driven wizard using a persistent view.

    Cancellation is allowed only via the Cancel button on the embed.
    The Types button shows available action types. The user replies
    in-channel to fill fields; the embed is updated as state changes.
    """
    def check(m: discord.Message) -> bool:
        return m.author == ctx.author and m.channel == ctx.channel

    state = initial_state if initial_state is not None else WizardState(message_id=None, emoji=None, action_type=None, opts={})
    aid = author_id if author_id is not None else ctx.author.id
    view = ReactRoleWizardView(aid, state, ReactionAction)
    # expose ctx to the view for CLI/help generation
    setattr(view, "ctx", ctx)
    embed = view.get_embed()
    sent = await ctx.send(embed=embed, view=view)
    view.message_ref = sent

    # Helper to update the embed
    async def refresh_embed() -> None:
        try:
            allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
            await view.message_ref.edit(embed=view.get_embed(), view=view, allowed_mentions=allowed)
        except Exception:
            pass

    # Validation/resolve helpers now live in react.utils

    # Cleanup helper to ensure the message no longer exposes the active view
    async def _cleanup_view() -> None:
        if view.message_ref is None:
            return
        for child in view.children:
            child.disabled = True
        try:
            allowed = discord.AllowedMentions(roles=False, users=False, everyone=False)
            await view.message_ref.edit(embed=view.get_embed(), view=None, allowed_mentions=allowed)
        except Exception:
            try:
                await view.message_ref.edit(embed=view.get_embed(), allowed_mentions=allowed)
            except Exception:
                pass
        try:
            view.stop()
        except Exception:
            pass

    try:
        # Helpers to keep the channel tidy: delete bot prompts and user replies
        async def _delayed_delete(message: discord.Message, delay: float = 30.0) -> None:
            try:
                await asyncio.sleep(delay)
                await message.delete()
            except Exception:
                pass

        async def _ask(prompt_text: str | None = None, embed: discord.Embed | None = None, timeout: int = 120) -> discord.Message | None:
            prompt_msg = None
            try:
                if embed is not None:
                    prompt_msg = await ctx.send(embed=embed)
                else:
                    prompt_msg = await ctx.send(prompt_text)
            except Exception:
                prompt_msg = None
            try:
                m = await cog.bot.wait_for("message", check=check, timeout=timeout)
            except asyncio.TimeoutError:
                if prompt_msg is not None:
                    try:
                        await prompt_msg.delete()
                    except Exception:
                        pass
                await ctx.send("Timed out. Wizard cancelled.")
                return None
            # remove the prompt + the user's reply to keep the channel tidy
            try:
                if prompt_msg is not None:
                    await prompt_msg.delete()
            except Exception:
                pass
            try:
                await m.delete()
            except Exception:
                pass
            return m

        # Step 1: message id (try once) — skip prompt when prefilled
        if state.message_id is None:
            m = await _ask("Enter the target message id (or paste the message URL):")
            if m is None:
                return False
            if view.cancelled:
                await ctx.send("Wizard cancelled.")
                return False
            if m.content.strip().lower() in ("cancel", "quit", "exit"):
                await ctx.send("Wizard cancelled.")
                return False

            # Extract numeric IDs from the provided content. If a full message URL
            # was pasted, capture the channel id as well so we can show a jump link.
            matches = re.findall(r"\d{17,21}", m.content)
            if not matches:
                await ctx.send("Could not find a message id. Wizard cancelled.")
                return False

            # If the user pasted a message URL (contains 'channels'), prefer the
            # last value as message id and the second-last as channel id when present.
            msg_id = int(matches[-1])
            ch_id: int | None = None
            if "channels" in m.content.lower() and len(matches) >= 2:
                ch_id = int(matches[-2])

            state.message_id = msg_id
            if ch_id is not None:
                state.channel_id = ch_id
        else:
            msg_id = state.message_id

        # If we don't have an explicit channel id, attempt a faster lookup
        # using any persisted mapping, then fall back to a concurrent scan.
        if state.channel_id is None:
            try:
                mcfg = {}
                try:
                    mcfg = await cog.config.guild(ctx.guild).message_channels()
                except Exception:
                    mcfg = {}
                ch_id, fetched = await find_message_channel(ctx.guild, state.message_id, bot_member=ctx.guild.me, message_channels=mcfg, concurrency=8)
                if ch_id is not None:
                    state.channel_id = ch_id
                    try:
                        preview = ""
                        if fetched is not None:
                            if fetched.content:
                                preview = fetched.content.strip()
                            elif fetched.embeds:
                                e = fetched.embeds[0]
                                preview = (e.title or "") or (e.description or "")
                        if preview:
                            preview = " ".join(preview.split())
                            if len(preview) > 200:
                                preview = preview[:197] + "..."
                            state.message_preview = preview
                    except Exception:
                        pass
            except Exception:
                pass

        await refresh_embed()

        async def _collect_details() -> bool:
            # Step 2: emoji (skip if already provided)
            if state.emoji is None:
                while True:
                    m = await _ask("Enter the emoji to map (unicode or custom like `<:name:id>`):")
                    if m is None:
                        return False
                    if view.cancelled:
                        await ctx.send("Wizard cancelled.")
                        return False
                    content = m.content.strip()
                    if content.lower() in ("cancel", "quit", "exit"):
                        await ctx.send("Wizard cancelled.")
                        return False
                    if content.lower().endswith("types"):
                        embeds = build_types_embeds(ReactionAction.registry)
                        if not embeds:
                            tmp = await ctx.send("No action types are registered.")
                            asyncio.create_task(_delayed_delete(tmp, 8.0))
                        else:
                            for embed in embeds:
                                try:
                                    tmpmsg = await ctx.send(embed=embed)
                                    asyncio.create_task(_delayed_delete(tmpmsg, 30.0))
                                except Exception:
                                    pass
                        continue

                    # (no debug send)
                    ok, reason = await validate_emoji(cog.bot, ctx.guild, content)
                    if not ok:
                        tmp = await ctx.send(f"Invalid emoji: {reason}. Please enter a valid emoji or try again.")
                        asyncio.create_task(_delayed_delete(tmp, 8.0))
                        continue
                    state.emoji = content
                    break

            await refresh_embed()

            # Step 3: action type (skip if already set)
            if state.action_type is None:
                while True:
                    m = await _ask("Enter the action type. You can also press the Types button on the embed to view available types.")
                    if m is None:
                        return False
                    if view.cancelled:
                        await ctx.send("Wizard cancelled.")
                        return False
                    content = m.content.strip()
                    if content.lower() in ("cancel", "quit", "exit"):
                        await ctx.send("Wizard cancelled.")
                        return False
                    if content == "":
                        state.action_type = "standard"
                        break
                    if content.lower().endswith("types"):
                        lines = ["Available action types:"]
                        for name in sorted(ReactionAction.registry.keys()):
                            cls = ReactionAction.registry[name]
                            desc = getattr(cls, "description", None) or (cls.__doc__ or "").strip().splitlines()[0]
                            lines.append(f"{name}: {desc}")
                        tmp = await ctx.send("\n".join(lines))
                        asyncio.create_task(_delayed_delete(tmp, 30.0))
                        continue
                    state.action_type = content
                    if state.action_type not in ReactionAction.registry:
                        tmp = await ctx.send(f"Unknown action type `{state.action_type}`. Try again or press the Types button.")
                        asyncio.create_task(_delayed_delete(tmp, 8.0))
                        continue
                    break
            await refresh_embed()

            # Prompt for declared options with `roles` first
            cls = ReactionAction.registry.get(state.action_type)
            declared_opts = getattr(cls, "options", []) if cls is not None else []
            ordered_opts = list(declared_opts)
            if "roles" in ordered_opts:
                ordered_opts.remove("roles")
                ordered_opts.insert(0, "roles")

            opts: Dict[str, Any] = state.opts or {}
            for opt_name in ordered_opts:
                # Skip prompting when an explicit value is already present
                existing = (state.opts or {}).get(opt_name, None)
                if existing is not None and not (isinstance(existing, str) and str(existing).strip() == ""):
                    continue

                if opt_name == "roles":
                    while True:
                        m = await _ask("Enter role mentions or IDs separated by spaces (or `none`):")
                        if m is None:
                            return False
                        if view.cancelled:
                            await ctx.send("Wizard cancelled.")
                            return False
                        roles_raw = m.content.strip()
                        if roles_raw.lower() in ("none", ""):
                            # keep existing value if present
                            break
                        if roles_raw.lower() in ("cancel", "quit", "exit"):
                            await ctx.send("Wizard cancelled.")
                            return False
                        tokens = roles_raw.split()
                        resolved, unresolved = resolve_roles_from_guild(ctx.guild, tokens)
                        if unresolved:
                            if not resolved:
                                tmp = await ctx.send(
                                    f"Could not resolve any of the provided roles: {', '.join(unresolved)}.\n"
                                    "Type `retry` to enter roles again, `keep` to store unresolved tokens as-is, or `none` to skip."
                                )
                                asyncio.create_task(_delayed_delete(tmp, 30.0))
                                m2 = await _ask()
                                if m2 is None:
                                    return False
                                ans = m2.content.strip().lower()
                                if ans == "retry":
                                    continue
                                if ans in ("none", "skip"):
                                    break
                                if ans in ("keep", "store"):
                                    opts["roles"] = resolved + unresolved
                                    break
                                tmp2 = await ctx.send("Unrecognized response; please type `retry`, `keep`, or `none`.")
                                asyncio.create_task(_delayed_delete(tmp2, 8.0))
                                continue
                            else:
                                # some resolved, some unresolved
                                tmp = await ctx.send(
                                    f"Some roles could not be resolved: {', '.join(unresolved)}.\n"
                                    "Type `retry` to re-enter, `keep` to store unresolved tokens, or `none` to skip them."
                                )
                                asyncio.create_task(_delayed_delete(tmp, 30.0))
                                m2 = await _ask()
                                if m2 is None:
                                    return False
                                ans = m2.content.strip().lower()
                                if ans == "retry":
                                    continue
                                if ans in ("none", "skip"):
                                    opts["roles"] = resolved
                                    break
                                if ans in ("keep", "store"):
                                    opts["roles"] = resolved + unresolved
                                    break
                                tmp2 = await ctx.send("Unrecognized response; please type `retry`, `keep`, or `none`.")
                                asyncio.create_task(_delayed_delete(tmp2, 8.0))
                                continue
                        else:
                            opts["roles"] = resolved
                            break
                    state.opts = opts
                    await refresh_embed()
                    continue

                if opt_name == "duration":
                    while True:
                        m = await _ask("Enter duration in seconds (e.g. 3600) for the timed role (or `none`):")
                        if m is None:
                            return False
                        if view.cancelled:
                            await ctx.send("Wizard cancelled.")
                            return False
                        txt = m.content.strip()
                        if txt.lower() in ("cancel", "quit", "exit"):
                            await ctx.send("Wizard cancelled.")
                            return False
                        if txt.lower() in ("none", ""):
                            break
                        try:
                            dur = int(txt)
                            if dur <= 0:
                                tmp = await ctx.send("Duration must be a positive integer. Try again or type `none` to skip.")
                                asyncio.create_task(_delayed_delete(tmp, 8.0))
                                continue
                            opts["duration"] = dur
                            break
                        except Exception:
                            tmp = await ctx.send("Invalid duration provided; enter an integer number of seconds or `none` to skip.")
                            asyncio.create_task(_delayed_delete(tmp, 8.0))
                            continue
                    state.opts = opts
                    await refresh_embed()
                    continue

                if opt_name == "group":
                    while True:
                        m = await _ask("Enter the group name (string) to use for this grouped mapping (or `none`):")
                        if m is None:
                            return False
                        if view.cancelled:
                            await ctx.send("Wizard cancelled.")
                            return False
                        txt = m.content.strip()
                        if txt.lower() in ("cancel", "quit", "exit"):
                            await ctx.send("Wizard cancelled.")
                            return False
                        if txt.lower() in ("none", ""):
                            break
                        if txt:
                            opts["group"] = txt
                            break
                        tmp = await ctx.send("Invalid group name; enter a non-empty string or `none` to skip.")
                        asyncio.create_task(_delayed_delete(tmp, 8.0))
                        continue
                    state.opts = opts
                    await refresh_embed()
                    continue

                if opt_name == "remove_on_unreact":
                    while True:
                        m = await _ask("Remove roles immediately on unreact? (true/false) — default is `true`. Enter `none` to accept default:")
                        if m is None:
                            return False
                        if view.cancelled:
                            await ctx.send("Wizard cancelled.")
                            return False
                        txt = m.content.strip()
                        if txt.lower() in ("cancel", "quit", "exit"):
                            await ctx.send("Wizard cancelled.")
                            return False
                        if txt.lower() in ("none", ""):
                            # keep default behavior (True) unless explicitly provided later
                            break
                        if txt.lower() in ("true", "false"):
                            opts[opt_name] = txt.lower() == "true"
                            break
                        tmp = await ctx.send("Invalid value; enter `true`, `false`, or `none` to accept default.")
                        asyncio.create_task(_delayed_delete(tmp, 8.0))
                        continue
                    state.opts = opts
                    await refresh_embed()
                    continue

                # Generic option handler
                while True:
                    m = await _ask(f"Enter value for option `{opt_name}` (or `none` to skip):")
                    if m is None:
                        return False
                    if view.cancelled:
                        await ctx.send("Wizard cancelled.")
                        return False
                    txt = m.content.strip()
                    if txt.lower() in ("cancel", "quit", "exit"):
                        await ctx.send("Wizard cancelled.")
                        return False
                    if txt.lower() in ("none", ""):
                        break
                    # Interpret booleans and integers, otherwise keep as string
                    if txt.lower() in ("true", "false"):
                        opts[opt_name] = txt.lower() == "true"
                        break
                    try:
                        opts[opt_name] = int(txt)
                        break
                    except Exception:
                        opts[opt_name] = txt
                        break
                state.opts = opts
                await refresh_embed()

            # Ensure timed default: remove_on_unreact True when not specified
            if state.action_type == "timed" and "remove_on_unreact" not in (state.opts or {}):
                (state.opts if state.opts is not None else {}).update({"remove_on_unreact": True})
            return True

        # run the first pass of details collection
        ok = await _collect_details()
        if not ok:
            return False


        # Validate action data
        action_data = {"type": state.action_type, "roles": state.opts.get("roles", [])}
        action_data.update(state.opts or {})
        ok, reason = cog._validate_action_data(action_data)
        if not ok:
            await ctx.send(f"Invalid action configuration: {reason}. Wizard cancelled.")
            return False

        # Finalize: enable confirm on the view and wait for the user to press Confirm.
        # Support re-entering the flow via the Edit button which sets `view.edit_request`.
        view.ready = True
        await refresh_embed()
        await ctx.send("Review the mapping in the embed and press Confirm to persist, Edit to re-enter prompts, or Cancel to abort.")

        while True:
            await view.wait()
            if view.cancelled:
                await ctx.send("Wizard cancelled.")
                return False
            if getattr(view, "edit_request", False):
                # clear edit_request and re-run the details collection
                view.edit_request = False
                view.ready = False
                await refresh_embed()
                ok = await _collect_details()
                if not ok:
                    return False
                # re-validate after edits
                action_data = {"type": state.action_type, "roles": state.opts.get("roles", [])}
                action_data.update(state.opts or {})
                ok, reason = cog._validate_action_data(action_data)
                if not ok:
                    await ctx.send(f"Invalid action configuration: {reason}. Wizard cancelled.")
                    return False
                view.ready = True
                await refresh_embed()
                await ctx.send("Review the mapping in the embed and press Confirm to persist, Edit to re-enter prompts, or Cancel to abort.")
                continue
            if not view.confirmed:
                await ctx.send("Wizard did not complete. No changes made.")
                return False
            break

        # Persist mapping under cog lock
        msg_key = str(state.message_id)
        async with cog._get_reaction_lock(ctx.guild.id):
            try:
                cfg = await cog.config.guild(ctx.guild).reactions()
            except Exception:
                await ctx.send("Failed to read reactions config; abort.")
                return False

            msg_map = cfg.get(msg_key, {}) or {}
            msg_map[str(state.emoji)] = action_data
            cfg[msg_key] = msg_map
            try:
                await cog.config.guild(ctx.guild).reactions.set(cfg)
            except Exception:
                await ctx.send("Failed to persist mapping — check bot logs.")
                return False

        # Build a friendly, non-pinging summary using rendered values.
        # Message jump link when possible
        try:
            msg_disp = str(state.message_id)
            if state.channel_id is not None and view.message_ref is not None and getattr(view.message_ref, "guild", None) is not None:
                guild_id = view.message_ref.guild.id
                link = f"https://discord.com/channels/{guild_id}/{state.channel_id}/{state.message_id}"
                msg_disp = f"[Jump to message]({link}) ({state.message_id})"
        except Exception:
            msg_disp = str(state.message_id)

        # Render emoji visually when possible
        try:
            emoji_disp = str(state.emoji or "")
            try:
                pe = discord.PartialEmoji.from_str(state.emoji or "")
                emoji_disp = str(pe)
            except Exception:
                m = re.search(r"(\d{17,21})", str(state.emoji or ""))
                if m and view.message_ref is not None and getattr(view.message_ref, "guild", None) is not None:
                    eobj = view.message_ref.guild.get_emoji(int(m.group(1)))
                    if eobj is not None:
                        emoji_disp = str(eobj)
        except Exception:
            emoji_disp = str(state.emoji or "")

        # Roles: prefer opts['roles'] (may be ids or names); render mentions when resolvable
        roles_tokens = (action_data.get("roles") or [])
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
            f"Action: `{state.action_type}`\n"
            f"Roles: {roles_disp}\n\n"
            f"Use `{ctx.prefix}reactrole list` to verify or `{ctx.prefix}reactrole remove {msg_key} {state.emoji}` to remove."
        )
        await ctx.send(content, allowed_mentions=allowed)
        return True
    finally:
        try:
            await _cleanup_view()
        except Exception:
            pass
