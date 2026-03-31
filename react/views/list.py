"""List view with pagination and edit/delete actions for react role mappings."""
from __future__ import annotations

import discord
from redbot.core import commands

from .wizard import run_reactrole_wizard, WizardState


class ReactRoleListView(discord.ui.View):
    def __init__(self, cog, ctx: commands.Context, msg_id: str, grouped: dict, timeout: float = 300.0):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.msg_id = str(msg_id)
        self.grouped = grouped
        self.selected: str | None = None
        self.message_ref: discord.Message | None = None

        options: list[discord.SelectOption] = []
        for rendered, mappings in grouped.items():
            for raw_key, action_data in mappings:
                label = f"{rendered} {action_data.get('type', '')}".strip()
                if len(label) > 100:
                    label = label[:97] + "..."
                options.append(discord.SelectOption(label=label, value=str(raw_key)))

        # Chunk into pages of 25 options to respect Discord's select limits
        self.pages: list[list[discord.SelectOption]] = [options[i:i+25] for i in range(0, len(options), 25)] if options else []
        self.page_idx = 0
        if self.pages:
            self.add_item(self.MappingSelect(self.pages[self.page_idx], parent=self))
            if len(self.pages) > 1:
                prev_btn = discord.ui.Button(label="<", style=discord.ButtonStyle.secondary)
                next_btn = discord.ui.Button(label=">", style=discord.ButtonStyle.secondary)

                async def prev_callback(interaction: discord.Interaction):
                    if interaction.user.id != self.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
                        await interaction.response.send_message("Only the list starter or members with Manage Roles can change pages.", ephemeral=True)
                        return
                    self.page_idx = max(0, self.page_idx - 1)
                    for child in self.children:
                        if isinstance(child, ReactRoleListView.MappingSelect):
                            child.options = self.pages[self.page_idx]
                            child.placeholder = f"Select mapping (page {self.page_idx+1}/{len(self.pages)})"
                            break
                    try:
                        await interaction.response.edit_message(view=self)
                    except Exception:
                        try:
                            await interaction.response.send_message("Page updated.", ephemeral=True)
                        except Exception:
                            pass

                async def next_callback(interaction: discord.Interaction):
                    if interaction.user.id != self.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
                        await interaction.response.send_message("Only the list starter or members with Manage Roles can change pages.", ephemeral=True)
                        return
                    self.page_idx = min(len(self.pages)-1, self.page_idx + 1)
                    for child in self.children:
                        if isinstance(child, ReactRoleListView.MappingSelect):
                            child.options = self.pages[self.page_idx]
                            child.placeholder = f"Select mapping (page {self.page_idx+1}/{len(self.pages)})"
                            break
                    try:
                        await interaction.response.edit_message(view=self)
                    except Exception:
                        try:
                            await interaction.response.send_message("Page updated.", ephemeral=True)
                        except Exception:
                            pass

                prev_btn.callback = prev_callback
                next_btn.callback = next_callback
                self.add_item(prev_btn)
                self.add_item(next_btn)

    class MappingSelect(discord.ui.Select):
        def __init__(self, options: list[discord.SelectOption], parent: "ReactRoleListView") -> None:
            super().__init__(placeholder="Select mapping...", min_values=1, max_values=1, options=options)
            self.parent = parent

        async def callback(self, interaction: discord.Interaction) -> None:
            if interaction.user.id != self.parent.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
                await interaction.response.send_message("Only the listing starter or members with Manage Roles can use this.", ephemeral=True)
                return
            self.parent.selected = self.values[0]
            await interaction.response.send_message("Selected mapping.", ephemeral=True)

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("Only the list starter or members with Manage Roles can edit.", ephemeral=True)
            return
        sel = self.selected
        if sel is None:
            all_keys = [k for mappings in self.grouped.values() for k, _ in mappings]
            if len(all_keys) == 1:
                sel = all_keys[0]
            else:
                await interaction.response.send_message("Select which mapping to edit using the dropdown.", ephemeral=True)
                return

        action_data = None
        for mappings in self.grouped.values():
            for raw_key, ad in mappings:
                if str(raw_key) == str(sel):
                    action_data = ad
                    break
            if action_data is not None:
                break
        if action_data is None:
            await interaction.response.send_message("Selected mapping not found.", ephemeral=True)
            return

        initial_opts = dict(action_data or {})
        initial_opts.pop("type", None)
        try:
            await interaction.response.send_message("Opening editor...", ephemeral=True)
        except Exception:
            pass
        state = WizardState(message_id=int(self.msg_id), emoji=str(sel), action_type=action_data.get("type"), opts=initial_opts)
        try:
            added = await run_reactrole_wizard(self.cog, self.ctx, initial_state=state, author_id=interaction.user.id)
        except Exception:
            await interaction.followup.send("Editor failed to start.", ephemeral=True)
            return
        if added:
            await interaction.followup.send("Mapping updated.", ephemeral=True)
        else:
            await interaction.followup.send("No changes made.", ephemeral=True)

        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            try:
                if self.message_ref is not None:
                    await self.message_ref.edit(view=None)
            except Exception:
                pass
        self.stop()

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.danger)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self.ctx.author.id and not interaction.user.guild_permissions.manage_roles:
            await interaction.response.send_message("Only the list starter or members with Manage Roles can delete.", ephemeral=True)
            return
        sel = self.selected
        if sel is None:
            all_keys = [k for mappings in self.grouped.values() for k, _ in mappings]
            if len(all_keys) == 1:
                sel = all_keys[0]
            else:
                await interaction.response.send_message("Select which mapping to delete using the dropdown.", ephemeral=True)
                return

        async with self.cog._get_reaction_lock(self.ctx.guild.id):
            try:
                cfg = await self.cog.config.guild(self.ctx.guild).reactions()
            except Exception:
                await interaction.response.send_message("Failed to read config.", ephemeral=True)
                return
            msg_map = cfg.get(str(self.msg_id), {}) or {}
            if str(sel) not in msg_map:
                await interaction.response.send_message("Mapping not found.", ephemeral=True)
                return
            msg_map.pop(str(sel), None)
            if not msg_map:
                cfg.pop(str(self.msg_id), None)
            else:
                cfg[str(self.msg_id)] = msg_map
            try:
                await self.cog.config.guild(self.ctx.guild).reactions.set(cfg)
            except Exception:
                await interaction.response.send_message("Failed to persist deletion.", ephemeral=True)
                return
            # Also remove persisted channel mapping when no mappings remain for the message
            if str(self.msg_id) not in (await self.cog.config.guild(self.ctx.guild).reactions() or {}):
                try:
                    mcfg = await self.cog.config.guild(self.ctx.guild).message_channels()
                    if str(self.msg_id) in (mcfg or {}):
                        mcfg.pop(str(self.msg_id), None)
                        await self.cog.config.guild(self.ctx.guild).message_channels.set(mcfg)
                except Exception:
                    pass

        await interaction.response.send_message("Mapping removed.", ephemeral=True)
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            try:
                if self.message_ref is not None:
                    await self.message_ref.edit(view=None)
            except Exception:
                pass
        self.stop()
