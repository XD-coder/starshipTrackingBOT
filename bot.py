import os
import discord
from discord.ext import commands
from dotenv import load_dotenv


import sys
sys.dont_write_bytecode = True
load_dotenv()

# Bot configuration
TOKEN = os.getenv('DISCORD_TOKEN')
PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Set up intents
intents = discord.Intents.default()
intents.message_content = True  # Required for message content access
intents.members = True         # Required for member events
intents.guilds = True         # Required for guild-related events
intents.presences = True      # Required for presence updates

# Initialize bot with prefix and intents
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    

# Load cogs
async def load_cogs():
    # Create cogs directory if it doesn't exist
    if not os.path.exists('./cogs'):
        os.makedirs('./cogs')
    
    # Load all cogs from the cogs directory
    for filename in os.listdir('./cogs'):
        if filename.endswith('.py'):
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                print(f'Loaded cog: {filename[:-3]}')
            except Exception as e:
                print(f'Failed to load cog {filename[:-3]}: {str(e)}')

# Error handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send(f'Command not found. Use {PREFIX}help to see available commands.')
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send('You do not have permission to use this command.')
    else:
        await ctx.send(f'An error occurred: {str(error)}')

# Run the bot
async def main():
    async with bot:
        await load_cogs()
        await bot.start(TOKEN)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())