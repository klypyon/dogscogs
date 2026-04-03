from typing import Literal
import typing

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.config import Config

from dogscogs.constants import COG_IDENTIFIER

RequestType = Literal["discord_deleted_user", "owner", "user", "user_strict"]

DEFAULT_GUILD = {
    "enabled": True,
    "will_delete": False,
}

DEFAULT_CHANNEL = {
    "roles": [],
} # type: ignore[var-annotated]

class SeasonalRoles(commands.Cog):
    """
    Automatically applies roles to users who post in a channel.
    """

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=COG_IDENTIFIER,
            force_registration=True,
        )

        self.config.register_guild(**DEFAULT_GUILD)
        self.config.register_channel(**DEFAULT_CHANNEL)

    @commands.is_owner()
    async def red_delete_data_for_user(self, *, requester: RequestType, user_id: int) -> None:
        # TODO: Replace this with the proper end user data removal handling.
        await super().red_delete_data_for_user(requester=requester, user_id=user_id)

    
    @commands.group()
    @commands.guild_only()
    async def seasonalroles(self, ctx: commands.GuildContext) -> None:
        """Manage seasonal roles."""
        pass

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def enable(self, ctx: commands.GuildContext) -> None:
        """Enable seasonal roles."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("Seasonal roles enabled.")

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def disable(self, ctx: commands.GuildContext) -> None:
        """Disable seasonal roles."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("Seasonal roles disabled.")

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def enabled(self, ctx: commands.GuildContext, is_enabled: typing.Optional[bool]) -> None:
        """Check if seasonal roles are enabled."""
        if is_enabled is None:
            is_enabled = await self.config.guild(ctx.guild).enabled()
        else:
            await self.config.guild(ctx.guild).enabled.set(is_enabled)

        await ctx.send(f"Seasonal roles are {'enabled' if is_enabled else 'disabled'}.")

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def channel(self, ctx: commands.GuildContext, channel: typing.Union[discord.TextChannel, discord.Thread], roles: commands.Greedy[discord.Role]) -> None:
        """Set the channel to watch for seasonal roles."""
        if not roles or len(roles) == 0:
            role_ids = await self.config.channel(channel).roles()
            roles = [channel.guild.get_role(role_id) for role_id in role_ids] # type: ignore[assignment]
            roles = [role for role in roles if role] # type: ignore[assignment]
        else:
            await self.config.channel(channel).roles.set([role.id for role in roles])

        if not roles:
            await ctx.send(f"No roles set for {channel.mention}.")
            return
        
        await ctx.send(f"Roles set for {channel.mention}: {', '.join(role.mention for role in roles)}")

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def clear(self, ctx: commands.GuildContext, channel: discord.TextChannel) -> None:
        """Clear the seasonal roles for a channel."""
        await self.config.channel(channel).roles.set([])
        await ctx.send(f"Roles cleared for {channel.mention}.")

    @seasonalroles.command()
    @commands.guild_only()
    async def channels(self, ctx: commands.GuildContext) -> None:
        """List the channels with seasonal roles."""
        channels = await self.config.all_channels()
        if not channels:
            await ctx.send("No channels have seasonal roles.")
            return

        embed = discord.Embed()
        embed.title = "Seasonal Roles"
        embed.description = "The following channels will apply roles to users who post in them:\n\n"
        text = ""
        for channel_id, data in channels.items():
            channel = ctx.guild.get_channel_or_thread(channel_id)
            if channel is None:
                continue

            roles = [ctx.guild.get_role(role_id) for role_id in data["roles"]]
            roles = [role for role in roles if role is not None]
            if len(roles) == 0:
                continue

            text += f"{channel.mention}: {', '.join(role.mention for role in roles)}\n" # type: ignore[union-attr]
        embed.add_field(name="Channels", value=text)

        await ctx.send(embed=embed)

    @seasonalroles.command()
    @commands.guild_only()
    @commands.has_guild_permissions(manage_roles=True)
    async def cleanup(self, ctx: commands.GuildContext, will_delete: typing.Optional[bool]) -> None:
        """Set whether or not the bot will delete messages after applying roles."""
        if will_delete is None:
            will_delete = await self.config.guild(ctx.guild).will_delete()
        else:
            await self.config.guild(ctx.guild).will_delete.set(will_delete)

        await ctx.send(f"Messages will {'be' if will_delete else 'not be'} deleted after applying roles.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        guild = message.guild
        if not guild:
            return

        if not await self.config.guild(guild).enabled():
            return

        channel = message.channel
        data = await self.config.channel(channel).roles()
        roles = [r for r in [guild.get_role(role_id) for role_id in data] if r is not None]
        if len(roles) == 0:
            return

        member : discord.Member = message.author # type: ignore[assignment]
        await member.add_roles(*roles, reason="Seasonal roles", atomic=True)

        if await self.config.guild(guild).will_delete():
            await message.delete(delay=5)

    @commands.Cog.listener()
    async def on_role_delete(self, role: discord.Role) -> None:
        guild = role.guild
        if not guild:
            return

        channels = await self.config.all_channels()
        for channel_id, data in channels.items():
            if role.id in data["roles"]:
                data["roles"].remove(role.id)
                await self.config.channel(guild.get_channel_or_thread(channel_id)).roles.set(data["roles"])

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel) -> None:
        guild = channel.guild
        if not guild:
            return

        if not isinstance(channel, discord.TextChannel):
            return

        await self.config.channel(channel).clear()

    @commands.Cog.listener()
    async def on_thread_delete(self, thread: discord.Thread) -> None:
        guild = thread.guild
        if not guild:
            return

        await self.config.channel(thread).clear()

    @commands.Cog.listener()
    async def on_thread_remove(self, thread: discord.Thread) -> None:
        guild = thread.guild
        if not guild:
            return

        await self.config.channel(thread).clear()
