"""Help commands for displaying bot documentation and command usage"""

import discord
import logging
import asyncio
from typing import Optional, Dict, List, Any, cast, Protocol, TypeVar, Union

from discord.ext import commands
from discord import app_commands
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection

# Define a protocol for PvPBot to handle database access properly
T = TypeVar('T')
class MotorDatabase(Protocol):
    """Protocol for MongoDB motor database"""
    @property
    def guilds(self) -> AsyncIOMotorCollection: ...

class PvPBot(Protocol):
    """Protocol for PvPBot with database property"""
    @property
    def db(self) -> Optional[MotorDatabase]: ...

from utils.embed_builder import EmbedBuilder
from models.guild import Guild
from utils.helpers import is_feature_enabled, get_guild_premium_tier, has_admin_permission


class CommandSelect(discord.ui.Select):
    """Dropdown select for command categories"""

    def __init__(self, bot, author_id: int, guild_id: int):
        self.bot = cast(PvPBot, bot)
        self.author_id = author_id
        self.guild_id = guild_id

        # Define command categories
        options = [
            discord.SelectOption(label="Admin", description="Server administration commands", emoji="🛡️"),
            discord.SelectOption(label="Setup", description="Server setup and configuration", emoji="⚙️"),
            discord.SelectOption(label="Killfeed", description="Killfeed monitoring commands", emoji="☠️"),
            discord.SelectOption(label="Events", description="Server events monitoring", emoji="🔔"),
            discord.SelectOption(label="Stats", description="Player and server statistics", emoji="📊"),
            discord.SelectOption(label="Economy", description="Economy and gambling features", emoji="💰"),
            discord.SelectOption(label="Premium", description="Premium features and upgrades", emoji="✨"),
            discord.SelectOption(label="Parser", description="Data parsing system information", emoji="📋"),
        ]

        super().__init__(placeholder="Select a category", options=options)

    async def callback(self, interaction: discord.Interaction):
        # Only allow the original command user to use the dropdown
        if interaction.user and interaction.user.id != self.author_id:
            await interaction.response.send_message("This menu is not for you.", ephemeral=True)
            return

        # Defer the response to avoid timeout
        try:
            await interaction.response.defer(ephemeral=False)
        except Exception as e:
            # Continue execution even if defer fails
            logging.warning(f"Error deferring response in CommandSelect callback: {e}")

        # Get the guild model for theme with timeout protection
        guild_model = None
        try:
            # Use a short timeout to prevent blocking
            guild_model = await asyncio.wait_for(
                Guild.get_by_guild_id(self.bot.db, str(self.guild_id)),
                timeout=1.0
            )
        except asyncio.TimeoutError:
            logging.warning(f"Timeout getting guild model in CommandSelect callback for guild {self.guild_id}")
        except Exception as e:
            logging.error(f"Error getting guild model in CommandSelect callback: {e}")

        # Create help embed based on selection
        category = self.values[0]
        embed = await self.create_category_embed(category, guild_model)

        # Use followup.edit_message since we deferred the response
        try:
            await interaction.edit_original_response(embed=embed, view=self.view)
        except Exception as e:
            logging.error(f"Error editing response in CommandSelect callback: {e}")
            try:
                # Fallback to followup if edit fails
                await interaction.followup.send("Error displaying help. Please try again.", ephemeral=True)
            except:
                pass

    async def create_category_embed(self, category, guild_model) -> discord.Embed:
        """Create help embed for the selected category
        
        Args:
            category: The selected category from the dropdown
            guild_model: The guild model for theming
            
        Returns:
            discord.Embed: The embed to display
        """
        # Cast to string to ensure proper typing
        category_str = str(category)

        if category_str == "Admin":
            title = "🛡️ Admin Commands"
            description = "Server administration commands"
            fields = [
                {"name": "/admin setrole", "value": "Set the admin role for server management", "inline": False},
                {"name": "/admin premium", "value": "Set premium tier for a guild (Home Guild Admins only)", "inline": False},
                {"name": "/admin status", "value": "View bot status information", "inline": False},
                {"name": "/admin sethomeguild", "value": "Set the current guild as the home guild (Bot Owner only)", "inline": False},
                {"name": "/admin help", "value": "Show help for admin commands", "inline": False},
            ]

        elif category_str == "Setup":
            title = "⚙️ Setup Commands"
            description = "Server setup and configuration commands"
            fields = [
                {"name": "/setup add_server <n> <host> <port> <user> <pass> <id>", "value": "Add a new server to track", "inline": False},
                {"name": "/setup remove_server <server>", "value": "Remove a server from tracking", "inline": False},
                {"name": "/setup channels <server> [channels...]", "value": "Configure notification channels for a server", "inline": False},
                {"name": "/setup list_servers", "value": "List all configured servers for this guild", "inline": False},
                {"name": "/setup historical_parse <server>", "value": "Parse all historical data for a server", "inline": False},
            ]

        elif category_str == "Killfeed":
            title = "☠️ Killfeed Commands"
            description = "Killfeed monitoring commands"
            fields = [
                {"name": "/killfeed start <server>", "value": "Start the killfeed monitor for a server", "inline": False},
                {"name": "/killfeed stop <server>", "value": "Stop the killfeed monitor for a server", "inline": False},
                {"name": "/killfeed status", "value": "Check the status of killfeed monitors for this guild", "inline": False},
            ]

        elif category_str == "Events":
            title = "🔔 Events Commands"
            description = "Server events monitoring commands"
            fields = [
                {"name": "/events start <server>", "value": "Start the events monitor for a server", "inline": False},
                {"name": "/events stop <server>", "value": "Stop the events monitor for a server", "inline": False},
                {"name": "/events status", "value": "Check the status of events monitors for this guild", "inline": False},
                {"name": "/events list_events <server> [type] [limit]", "value": "List recent events for a server", "inline": False},
                {"name": "/events online_players <server>", "value": "List online players for a server", "inline": False},
                {"name": "/events configure_events <server> [options]", "value": "Configure which event notifications are enabled", "inline": False},
                {"name": "/events configure_connections <server> [options]", "value": "Configure which connection notifications are enabled", "inline": False},
                {"name": "/events configure_suicides <server> [options]", "value": "Configure which suicide notifications are enabled", "inline": False},
            ]

        elif category_str == "Stats":
            title = "📊 Stats Commands"
            description = "Player and server statistics commands"
            premium_note = "\n\n**Note:** Basic stats require Survivor tier, enhanced stats require Mercenary or higher"
            description += premium_note
            fields = [
                {"name": "/stats player <server> <player>", "value": "View statistics for a player", "inline": False},
                {"name": "/stats server <server>", "value": "View statistics for a server", "inline": False},
                {"name": "/stats leaderboard <server> <stat>", "value": "View leaderboards for a specific stat", "inline": False},
                {"name": "/stats weapon_categories <server>", "value": "View statistics by weapon category", "inline": False},
                {"name": "/stats weapon <server> <weapon>", "value": "View statistics for a specific weapon", "inline": False},
                {"name": "/stats rivalry <server> <player>", "value": "View a player's rivalries (Warlord+ tier)", "inline": False},
                {"name": "/stats top_rivalries <server>", "value": "View the top rivalries on the server (Warlord+ tier)", "inline": False},
            ]

        elif category_str == "Economy":
            title = "💰 Economy Commands"
            description = "Economy and gambling features"
            premium_note = "\n\n**Note:** Basic economy requires Mercenary tier, gambling features available on Mercenary+ tiers"
            description += premium_note
            fields = [
                {"name": "/economy balance <server>", "value": "Check your balance", "inline": False},
                {"name": "/economy daily <server>", "value": "Claim your daily reward", "inline": False},
                {"name": "/economy leaderboard <server>", "value": "View the richest players", "inline": False},
                {"name": "/economy give <server> <user> <amount>", "value": "Give credits to another player", "inline": False},
                {"name": "/economy stats <server>", "value": "View server economy statistics", "inline": False},
                {"name": "/gambling blackjack <server> [bet]", "value": "Play blackjack (Mercenary+ tiers)", "inline": False},
                {"name": "/gambling slots <server> [bet]", "value": "Play slots (Mercenary+ tiers)", "inline": False},
                {"name": "/gambling roulette <server> [bet] [bet_type]", "value": "Play roulette (Mercenary+ tiers)", "inline": False},
            ]

        elif category_str == "Premium":
            title = "✨ Premium Commands"
            description = "Premium features and management commands"
            fields = [
                {"name": "/premium status", "value": "Check the premium status of this guild", "inline": False},
                {"name": "/premium upgrade", "value": "Request a premium upgrade", "inline": False},
                {"name": "/premium features", "value": "View available premium features", "inline": False},
                {"name": "/premium tiers", "value": "Display available premium tiers and their features", "inline": False},
                {"name": "/premium set_theme", "value": "Set the theme for embed displays (Warlord+ tiers only)", "inline": False},
            ]
            # Add premium tiers information
            fields.append({"name": "Premium Tiers", "value": "**Scavenger** (Free): Basic server management, killfeed (1 server)\n**Survivor** (£5): Basic stats, enhanced killfeeds (1 server)\n**Mercenary** (£10): Enhanced stats, basic economy, simple gambling (2 servers)\n**Warlord** (£20): Full stats, economy, rivalries, basic bounties (3 servers)\n**Overseer** (£50): All features including advanced bounties, all gambling games (unlimited servers)", "inline": False})

        elif category_str == "Parser":
            title = "📋 Parser System"
            description = "Tower of Temptation PvP Stats uses a sophisticated three-part parsing system for comprehensive data collection"
            fields = [
                {"name": "Historical CSV Parser", "value": "Processes historical data from CSV files to establish baseline statistics", "inline": False},
                {"name": "5-Minute CSV Parser", "value": "Processes new CSV data every 5 minutes for regular updates", "inline": False},
                {"name": "Real-time Log Parser", "value": "Monitors server logs in real-time for immediate event notifications", "inline": False},
                {"name": "Commands", "value": "/setup historical_parse <server> - Process all historical data\n/setup test_connection <server> - Test SFTP connectivity", "inline": False},
                {"name": "Event Normalization", "value": "All three parsers normalize data to ensure consistent event handling and deduplication", "inline": False},
                {"name": "Data Deduplication", "value": "System automatically detects and prevents duplicate event processing across all three parsers", "inline": False},
            ]

        else:
            # Default fallback
            title = "Bot Commands"
            description = "Use the dropdown to view different command categories"
            fields = []

        # Create embed
        try:
            embed = await EmbedBuilder.create_base_embed(
                title=title,
                description=description,
                guild=guild_model
            )

            # Add fields
            for field in fields:
                embed.add_field(name=field["name"], value=field["value"], inline=field.get("inline", False))

            # Add footer
            embed.set_footer(text="Use /commands to see this help menu again")

            return embed
        except Exception as e:
            # Log the error
            logging.error(f"Error creating category embed: {e}")

            # Create a fallback embed
            fallback_embed = discord.Embed(
                title=title,
                description=description,
                color=discord.Color.blue()
            )

            # Add fields
            for field in fields:
                fallback_embed.add_field(name=field["name"], value=field["value"], inline=field.get("inline", False))

            # Add footer
            fallback_embed.set_footer(text="Use /commands to see this help menu again")

            return fallback_embed


class CommandsView(discord.ui.View):
    """View with select dropdown for command categories"""

    def __init__(self, bot, author_id: int, guild_id: Optional[int]):
        super().__init__(timeout=600)  # 10 minute timeout

        # Add the select menu if guild_id is provided
        if guild_id is not None:
            self.add_item(CommandSelect(bot, author_id, guild_id))


class Help(commands.Cog):
    """Help commands for displaying bot documentation and command usage"""

    def __init__(self, bot):
        """Initialize the Help cog
        
        Args:
            bot: The bot instance
        """
        self.bot = cast(PvPBot, bot)
        self.guild_cache = {}  # Simple cache to store guild models
        self.logger = logging.getLogger(__name__)

    @app_commands.command(
        name="commands",
        description="View comprehensive help for all bot commands"
    )
    async def commands(self, interaction: discord.Interaction):
        """Displays a comprehensive help system with all available commands"""
        try:
            # Defer the response to avoid timeout
            try:
                await interaction.response.defer(ephemeral=False)
            except Exception as e:
                self.logger.error(f"Error deferring response in commands command: {e}")
                return

            # First prepare a default theme in case we can't get the guild model
            guild_id = interaction.guild_id if interaction.guild else None
            guild_model = None

            # Try to get the guild model with timeout protection
            try:
                guild_model = await asyncio.wait_for(
                    Guild.get_by_guild_id(self.bot.db, str(guild_id)), 
                    timeout=1.0  # Short timeout to prevent blocking
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout getting guild model for /commands in guild {guild_id}")
            except Exception as e:
                self.logger.error(f"Error getting guild model for /commands: {e}")

            # Create initial embed - use default theme if no guild model
            try:
                embed = await EmbedBuilder.create_base_embed(
                    title="Powered By Discord.gg/EmeraldServers",
                    description="Use the dropdown menu below to navigate through different command categories.",
                    guild=guild_model
                )

                # Add general info fields
                embed.add_field(
                    name="Getting Started",
                    value="1️⃣ Use `/setup add_server <n> <host> <port> <user> <pass> <id>` to add a server\n2️⃣ Configure channels with `/setup channels <server>`\n3️⃣ Start monitoring with `/killfeed start <server>` and `/events start <server>`",
                    inline=False
                )

                # Add premium tip
                embed.add_field(
                    name="Premium Features",
                    value="Upgrade to premium for advanced statistics, economy features, and custom themes. Use `/premium features` to learn more.",
                    inline=False
                )

                # Add footer
                embed.set_footer(text="Select a category to see detailed command information")
            except Exception as e:
                self.logger.error(f"Error creating embed: {e}")
                # Fallback to a basic embed if the themed one fails
                embed = discord.Embed(
                    title="Powered By Discord.gg/EmeraldServers",
                    description="Use the dropdown menu below to navigate through different command categories.",
                    color=discord.Color.blue()
                )
                embed.add_field(
                    name="Getting Started",
                    value="1️⃣ Use `/setup add_server <n> <host> <port> <user> <pass> <id>` to add a server\n2️⃣ Configure channels with `/setup channels <server>`\n3️⃣ Start monitoring with `/killfeed start <server>` and `/events start <server>`",
                    inline=False
                )
                embed.add_field(
                    name="Premium Features",
                    value="Upgrade to premium for advanced statistics, economy features, and custom themes. Use `/premium features` to learn more.",
                    inline=False
                )
                embed.set_footer(text="Select a category to see detailed command information")

            # Create and send view with dropdown - ensure user is validated
            user_id = interaction.user.id if interaction.user else 0
            view = CommandsView(self.bot, user_id, guild_id)

            # Check if embed is a coroutine (shouldn't happen but let's be safe)
            try:
                if hasattr(embed, '__await__'):
                    try:
                        embed = await embed  # Await the coroutine
                    except Exception as e:
                        # Use bot.logger if self.logger is not defined
                        logger = getattr(self, 'logger', getattr(self.bot, 'logger', None))
                        if logger:
                            logger.error(f"Error awaiting embed coroutine: {e}")
                        else:
                            print(f"Error awaiting embed coroutine: {e}")
                        # Create a simple error embed as fallback
                        try:
                            embed = await EmbedBuilder.create_error_embed(
                                "Error Loading Help",
                                "There was an error loading the help information. Please try again later.",
                                guild=guild_model
                            )
                        except Exception as embed_error:
                            if logger:
                                logger.error(f"Failed to create error embed: {embed_error}")
                            else:
                                print(f"Failed to create error embed: {embed_error}")
                            embed = discord.Embed(
                                title="Error Loading Help",
                                description="There was an error loading the help information. Please try again later.",
                                color=discord.Color.red()
                            )


                # Send the help message
                await interaction.followup.send(embed=embed, view=view)
            except Exception as e:
                self.logger.error(f"Error sending help message: {e}")
                # Try to send a simpler message
                try:
                    await interaction.followup.send("An error occurred loading the help menu. Please try again later.")
                except:
                    pass

        except Exception as e:
            # Use bot.logger if self.logger is not defined
            logger = getattr(self, 'logger', getattr(self.bot, 'logger', None))
            if logger:
                logger.error(f"Unhandled error in commands command: {e}", exc_info=True)
            else:
                print(f"Unhandled error in commands command: {e}")
            # Try to send a basic error message
            try:
                await interaction.followup.send("An error occurred processing your request. Please try again later.")
            except Exception:
                pass


async def setup(bot):
    """Set up the Help cog
    
    Args:
        bot: The Discord bot instance
    """
    # Cast the bot to the correct type
    await bot.add_cog(Help(bot))