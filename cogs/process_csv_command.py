"""
Process CSV command cog - removed duplicate command registration
"""
import discord
from discord.ext import commands
from discord import app_commands

class ProcessCSVCommandCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Remove duplicate /process_csv command since it exists in csv_processor.py

async def setup(bot):
    # Disable this cog since functionality exists in csv_processor.py
    pass