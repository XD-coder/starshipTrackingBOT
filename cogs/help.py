import discord
from discord.ext import commands
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

class CustomHelp(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Remove default help command
        self.bot.remove_command('help')
        log.info("CustomHelp Cog initialized - Default help command removed")

    @commands.command(name='help')
    async def help(self, ctx, command_name: str = None):
        """Shows this help message or detailed help for a specific command"""
        log.info(f"Help command invoked by {ctx.author} (ID: {ctx.author.id}) - Command: {command_name}")

        if command_name:
            # Show detailed help for specific command
            command = self.bot.get_command(command_name)
            if command:
                embed = discord.Embed(
                    title=f"Command: {command.name}",
                    description=command.help or "No description available.",
                    color=discord.Color.blue()
                )
                
                # Add usage info
                aliases = ", ".join(command.aliases) if command.aliases else "None"
                embed.add_field(name="Aliases", value=aliases, inline=False)
                
                # Add any additional command info
                if hasattr(command, 'brief'):
                    embed.add_field(name="Brief", value=command.brief or "No brief description.", inline=False)
                
                await ctx.send(embed=embed)
                log.info(f"Sent detailed help for command '{command_name}' to {ctx.author}")
            else:
                await ctx.send(f"❌ Command '{command_name}' not found.")
                log.warning(f"User {ctx.author} requested help for unknown command '{command_name}'")
            return

        # Show general help menu
        embed = discord.Embed(
            title="🚀 Starship Tracking Bot Commands",
            description="Here are all available commands. Use `!help <command>` for detailed info.",
            color=discord.Color.blue()
        )

        # Group commands by cog
        cog_commands = {}
        for command in self.bot.commands:
            cog_name = command.cog_name or "No Category"
            if cog_name not in cog_commands:
                cog_commands[cog_name] = []
            cog_commands[cog_name].append(command)

        # Add fields for each category
        for cog_name, commands_list in sorted(cog_commands.items()):
            # Skip showing CustomHelp commands in the list
            if cog_name == "CustomHelp":
                continue
                
            commands_text = ""
            for cmd in sorted(commands_list, key=lambda x: x.name):
                brief = cmd.brief or cmd.help or "No description available."
                if len(brief) > 50:  # Truncate long descriptions
                    brief = brief[:47] + "..."
                commands_text += f"`{cmd.name}` - {brief}\n"

            if commands_text:  # Only add non-empty categories
                embed.add_field(
                    name=f"📌 {cog_name}",
                    value=commands_text,
                    inline=False
                )

        embed.set_footer(text=f"Type {ctx.prefix}help <command> for more info on a command.")
        await ctx.send(embed=embed)
        log.info(f"Sent general help menu to {ctx.author}")

    def cog_unload(self):
        """Restore default help command when cog is unloaded"""
        self.bot.remove_command('help')
        self.bot._default_help_command = commands.DefaultHelpCommand()
        self.bot.help_command = self.bot._default_help_command
        log.info("CustomHelp Cog unloaded - Default help command restored")

async def setup(bot):
    await bot.add_cog(CustomHelp(bot))
    log.info("CustomHelp Cog setup complete")