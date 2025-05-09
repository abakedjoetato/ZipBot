"""
Player Links Cog for Tower of Temptation PvP Statistics Discord Bot

This cog provides commands for linking Discord users to in-game players:
1. Link your Discord user to an in-game player
2. Verify your player link
3. View your linked players
4. Remove a player link
"""
import logging
import asyncio
from datetime import datetime
import discord
import discord
# Use app_commands via discord.app_commands for py-cord compatibility
from discord.ext import commands
# Ensure discord_compat is imported for py-cord compatibility
from utils.discord_compat import get_app_commands_module
app_commands = get_app_commands_module()
from typing import Optional, Dict, List, Any

from models.player import Player
from models.player_link import PlayerLink
from models.guild import Guild
from utils.embed_builder import EmbedBuilder
from utils.decorators import premium_tier_required
from utils.discord_utils import server_id_autocomplete  # Import standardized autocomplete function

logger = logging.getLogger(__name__)

class PlayerLinksCog(commands.Cog):
    """Commands for linking Discord users to in-game players"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.server_autocomplete_cache = {}

    async def cog_unload(self) -> None:
        """Called when the cog is unloaded"""
        logger.info("Unloading PlayerLinksCog")

    @app_commands.command(name="link")
    @app_commands.describe(
        player_name="The in-game player name to link to your Discord account",
        server_id="The server ID (default: first available server)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(0)  # Available to all users (Tier 0+)
    async def link_player(
        self,
        interaction: discord.Interaction,
        player_name: str,
        server_id: Optional[str] = None
    ) -> None:
        """Link your Discord account to an in-game player

        Args:
            interaction: Discord interaction
            player_name: In-game player name
            server_id: Server ID (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Check if player exists
        player = await Player.get_by_player_name(server_id, player_name)
        if not player:
            embed = EmbedBuilder.error(
                title="Player Not Found",
                description=f"Player `{player_name}` not found on server `{server_id}`."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check if player is already linked to another Discord user
        existing_link = await PlayerLink.get_by_player_id(server_id, player.player_id)
        if existing_link and existing_link.discord_id != str(interaction.user.id):
            embed = EmbedBuilder.warning(
                title="Player Already Linked",
                description=f"Player `{player_name}` is already linked to another Discord user."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Create link
        link = await PlayerLink.create_or_update(
            server_id=server_id,
            guild_id=interaction.guild.id,
            discord_id=str(interaction.user.id),
            player_id=player.player_id,
            player_name=player.player_name,
            verify_code=None  # No verification needed for now
        )

        # Update player Discord ID
        await player.set_discord_id(str(interaction.user.id))

        embed = EmbedBuilder.success(
            title="Player Linked",
            description=f"Successfully linked your Discord account to player `{player_name}` on server `{server_id}`."
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="unlink")
    @app_commands.describe(
        player_name="The in-game player name to unlink from your Discord account",
        server_id="The server ID (default: first available server)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(0)  # Available to all users (Tier 0+)
    async def unlink_player(
        self,
        interaction: discord.Interaction,
        player_name: str,
        server_id: Optional[str] = None
    ) -> None:
        """Unlink an in-game player from your Discord account

        Args:
            interaction: Discord interaction
            player_name: In-game player name
            server_id: Server ID (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Check if player exists
        player = await Player.get_by_player_name(server_id, player_name)
        if not player:
            embed = EmbedBuilder.error(
                title="Player Not Found",
                description=f"Player `{player_name}` not found on server `{server_id}`."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Check if link exists and belongs to this user
        link = await PlayerLink.get_by_player_id(server_id, player.player_id)
        if not link:
            embed = EmbedBuilder.error(
                title="No Link Found",
                description=f"Player `{player_name}` is not linked to any Discord user."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if link.discord_id != str(interaction.user.id):
            embed = EmbedBuilder.error(
                title="Not Your Link",
                description=f"Player `{player_name}` is linked to another Discord user."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Delete link
        success = await link.delete()

        # Update player Discord ID if successful
        if success:
            await player.set_discord_id(None)

            embed = EmbedBuilder.success(
                title="Player Unlinked",
                description=f"Successfully unlinked your Discord account from player `{player_name}` on server `{server_id}`."
            )
        else:
            embed = EmbedBuilder.error(
                title="Unlink Failed",
                description=f"Failed to unlink player `{player_name}`. Please try again later."
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="myplayers")
    @app_commands.describe(
        server_id="The server ID (default: first available server)"
    )
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    @premium_tier_required(0)  # Available to all users (Tier 0+)
    async def view_linked_players(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None
    ) -> None:
        """View all players linked to your Discord account

        Args:
            interaction: Discord interaction
            server_id: Server ID (optional)
        """
        await interaction.response.defer(ephemeral=True)

        # Get server ID from guild config if not provided
        if not server_id or server_id == "":
            # For now, hardcode a test server ID
            server_id = "test_server"

        # Get linked players
        links = await PlayerLink.get_by_discord_id(server_id, str(interaction.user.id))

        if not links:
            embed = EmbedBuilder.info(
                title="No Linked Players",
                description=f"You don't have any players linked on server `{server_id}`."
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Create embed
        embed = discord.Embed(
            title="Your Linked Players",
            description=f"Players linked to your Discord account on server `{server_id}`",
            color=discord.Color.blue()
        )

        # Add player info
        for link in links:
            player = await Player.get_by_player_id(server_id, link.player_id)
            if player:
                embed.add_field(
                    name=player.player_name,
                    value=f"Kills: {player.kills}\nDeaths: {player.deaths}\nK/D: {player.kd_ratio:.2f}",
                    inline=True
                )

        await interaction.followup.send(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot) -> None:
    """Set up the player links cog"""
    await bot.add_cog(PlayerLinksCog(bot))