import typing

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.config import Config

from dogscogs.constants import COG_IDENTIFIER
from dogscogs.views.confirmation import ConfirmationView

from .config import GuildConfig, MemberConfig, RegisteredRole, get_registry_entry, get_role_depth
from .embeds import GraduationConfigEmbed

REGISTERED_ROLE_TOKEN = "$REGISTERED_ROLES$"
REGISTERED_ROLE_COUNT_TOKEN = "$REGISTERED_ROLE_COUNT$"
ASSIGNED_ROLE_TOKEN = "$ASSIGNED_ROLE$"

DEFAULT_GUILD: GuildConfig = {
    "enabled": True,
    "head_id": 1313226901799833680,
    # "head_id": 1315572475576324097,
    "registry": [
        {
            "role_id": 1313226901799833680,
            "next_ids": [1287870791752355971],
            "exclusive": True,
        },
        {
            "role_id": 1287870791752355971,
            "next_ids": [1246866082988556339],
            "exclusive": True,
        },
        {
            "role_id": 1246866082988556339,
            "next_ids": [1218902138516799549],
            "exclusive": True,
        },
        {
            "role_id": 1218902138516799549,
            "next_ids": [1287673776208740352, 1313227769483755560],
            "exclusive": True,
        },
        {
            "role_id": 1287673776208740352,
            "next_ids": [],
            "exclusive": True,
        },
        {
            "role_id": 1313227769483755560,
            "next_ids": [],
            "exclusive": False,
        }

        
        # {
        #     "role_id": 1315572475576324097,
        #     "next_ids": [1315572516542087178],
        #     "exclusive": True,
        # },
        # {
        #     "role_id": 1315572516542087178,
        #     "next_ids": [1315572543024795672],
        #     "exclusive": True,
        # },
        # {
        #     "role_id": 1315572543024795672,
        #     "next_ids": [1315572572166815754, 1315572595529089065],
        #     "exclusive": True,
        # },
        # {
        #     "role_id": 1315572572166815754,
        #     "next_ids": [],
        #     "exclusive": True,
        # },
        # {
        #     "role_id": 1315572595529089065,
        #     "next_ids": [],
        #     "exclusive": False,
        # }
    ],
    "responses": [
        f"You already have {REGISTERED_ROLE_COUNT_TOKEN} roles: {REGISTERED_ROLE_TOKEN}.  You have been given {ASSIGNED_ROLE_TOKEN}.",
    ],
}

DEFAULT_MEMBER: MemberConfig = {
    "last_promotion_timestamp": None,
    "graduation_timestamp": None,
}


class Graduation(commands.Cog):
    """
    Controls how users migrate from noob to beginner to pro over the course of seasons.
    """

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=COG_IDENTIFIER,
            force_registration=True,
        )

        self.config.register_guild(**DEFAULT_GUILD)
        self.config.register_member(**DEFAULT_MEMBER)

    async def __find_users(self, ctx: commands.GuildContext) -> typing.List[typing.Tuple[discord.Member, typing.List[discord.Role]]]:
        """
        Find all users who have registered roles.
        """
        
        registry = await self.config.guild(ctx.guild).registry()
        registered_role_ids = [entry["role_id"] for entry in registry]
        
        members : typing.List[typing.Tuple[discord.Member, typing.List[discord.Role]]] = []

        for member in ctx.guild.members:
            found_roles = [role for role in member.roles if role.id in registered_role_ids]
            if len(found_roles) > 0:
                members.append((member, [get_registry_entry(role.id, registry) for role in found_roles]))

        return members

    async def __promote(self, ctx: commands.GuildContext, member: discord.Member) -> bool:
        """
        Graduate a user to the next tier in the registry.
        """

        registry : typing.List[RegisteredRole] = await self.config.guild(ctx.guild).registry()
        member_role_ids = [role.id for role in member.roles]

        found_entry = next((entry for entry in registry if entry["role_id"] in member_role_ids and len(entry["next_ids"]) > 0), None)

        if found_entry is None:
            return False
        
        try:
            await member.remove_roles(discord.utils.get(ctx.guild.roles, id=int(found_entry['role_id'])), reason="Graduating to next tier.")
        except discord.Forbidden:
            await self.bot.send_to_owners(f"Graduation: I do not have permission to remove roles for {member.mention} in {ctx.guild.name}.")

        roles_to_be_added = [r for r in [discord.utils.get(ctx.guild.roles, id=int(next_id)) for next_id in found_entry["next_ids"]] if r is not None]

        try:
            await member.add_roles(*roles_to_be_added, reason="Graduating to next tier.", atomic=True)
        except discord.Forbidden:
            await self.bot.send_to_owners(f"Graduation: I do not have permission to add roles for {member.mention} in {ctx.guild.name}.")

        await self.config.member(member).last_promotion_timestamp.set(ctx.message.created_at.timestamp())

        return True


    @commands.group()
    @commands.has_guild_permissions(manage_roles=True)
    @commands.guild_only()
    async def graduation(self, ctx: commands.Context):
        """
        Commands for managing the graduation system.
        """
        pass

    @graduation.command()
    @commands.has_guild_permissions(manage_roles=True)
    @commands.guild_only()
    async def enabled(self, ctx: commands.GuildContext, is_enabled: typing.Optional[bool] = None):
        """
        Enable or disable the graduation system.
        """
        if is_enabled is None:
            is_enabled = await self.config.guild(ctx.guild).enabled()

        await self.config.guild(ctx.guild).enabled.set(is_enabled)
        await ctx.send(f"Graduation system is now {'ENABLED' if is_enabled else 'DISABLED'}.")

    @graduation.command()
    @commands.has_guild_permissions(manage_roles=True)
    @commands.guild_only()
    async def registry(self, ctx: commands.GuildContext):
        """
        Display the current registry of roles.
        """
        guild_config = await self.config.guild(ctx.guild).all()
        embed = GraduationConfigEmbed(guild_config=guild_config, guild=ctx.guild)
        await ctx.send(embed=await embed.ready())

    @graduation.command()
    @commands.is_owner()
    @commands.guild_only()
    async def clear_all(self, ctx: commands.GuildContext):
        """
        Clear all guild data.
        """
        await self.config.guild(ctx.guild).clear()
        await ctx.send("All guild data has been cleared.")

    @graduation.command(aliases=['convert', 'promote', 'convertusers'])
    @commands.has_guild_permissions(manage_roles=True)
    @commands.guild_only()
    async def graduate(self, ctx: commands.GuildContext):
        """Convert all users to their next tier in the graduation registry."""
        view = ConfirmationView(author=ctx.author)

        found = await self.__find_users(ctx)

        ## Filter out users who are already at the end of the registry.
        found = [f for f in found if any([len(entry["next_ids"]) > 0 for entry in f[1]])]

        if len(found) == 0:
            await ctx.send("No users found to graduate.")
            return
        
        confirmation_message = await ctx.send(f"Would you like to promote `{len(found)}` users?", view=view)

        if not await view.wait() and view.value:
            await confirmation_message.edit(content="Promoting users...", view=None)
            count = 0

            for member, _ in found:
                if await self.__promote(ctx, member):
                    count += 1

            await ctx.send(f"Promoted `{count}` users.")

        await confirmation_message.delete()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """
        Checks to make sure after a user receives new roles that it fits within the 
        role registry linked list in the proper structure.
        """
        if after.bot:
            return
        
        if before.roles == after.roles:
            return
        
        if len(before.roles) >= len(after.roles):
            return
        
        if not await self.config.guild(before.guild).enabled():
            return
        
        exclusive_registered_role_ids = []
        registry : typing.List[RegisteredRole] = await self.config.guild(before.guild).registry()
        head_id = await self.config.guild(before.guild).head_id()

        for entry in registry:
            if entry["exclusive"]:
                exclusive_registered_role_ids.append(entry["role_id"])

        roles_to_be_removed : typing.List[discord.Role] = []

        found_roles = [role for role in after.roles if role.id in exclusive_registered_role_ids]
        best_depth = 0
        best_role = None

        if len(found_roles) > 1:
            for role in found_roles:        
                depth = get_role_depth(head_id, role.id, registry)
                if depth > best_depth:
                    best_depth = depth
                    best_role = role

            for role in found_roles:
                if role != best_role:
                    roles_to_be_removed.append(role)

        if len(roles_to_be_removed) > 0:
            try:
                await after.remove_roles(*roles_to_be_removed, reason="Removing extra registered roles.", atomic=True)
            except discord.Forbidden:
                await self.bot.send_to_owners(f"Graduation: I do not have permission to remove roles from {after.mention} in {before.guild.name}.")