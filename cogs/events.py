import discord
from discord.ext import commands, tasks
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import json
import traceback # Keep for explicit error logging if needed

class Events(commands.Cog):
    # --- Initialization & Persistence ---
    def __init__(self, bot):
        self.bot = bot
        self.current_closures = []
        self.monitoring_channels = set()
        self.allowed_roles = {"Moderator", "Admin", "Road Closure Manager"}

        self.load_monitoring_channels()

        self._check_closures_started = False
        if not self._check_closures_started:
             self.fetch_initial_closures()
             self.check_closures.start()
             self._check_closures_started = True
        print("Events Cog Initialized. Check Closures task started.")

    def load_monitoring_channels(self, filename="monitoring_channels.json"):
        try:
            with open(filename, 'r') as f:
                channel_ids = json.load(f)
                self.monitoring_channels = set(int(cid) for cid in channel_ids)
                print(f"Loaded {len(self.monitoring_channels)} monitoring channels.")
        except FileNotFoundError:
            print("Monitoring channels file not found, starting fresh.")
            self.monitoring_channels = set()
        except (json.JSONDecodeError, ValueError, TypeError) as e:
             print(f"Error loading monitoring channels: {e}. Starting fresh.")
             self.monitoring_channels = set()

    def save_monitoring_channels(self, filename="monitoring_channels.json"):
        try:
            with open(filename, 'w') as f:
                json.dump(list(self.monitoring_channels), f)
        except IOError as e:
            print(f"Error saving monitoring channels: {e}")

    def fetch_initial_closures(self):
        """Fetches initial closures to prevent notifying about existing ones on start."""
        try:
            print("Fetching initial road closures...")
            response = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
            response.raise_for_status()
            self.current_closures = response.json()
            print(f"Fetched {len(self.current_closures)} initial closures.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch initial closures: {e}")
            self.current_closures = []
        except Exception as e:
            print(f"Unexpected error fetching initial closures: {e}")
            self.current_closures = []

    # --- Listeners ---
    @commands.Cog.listener()
    async def on_guild_join(self, guild):
         """Event triggered when the bot joins a new server"""
         print(f'Joined new guild: {guild.name} (id: {guild.id})')

    @commands.Cog.listener()
    async def on_member_join(self, member):
         """Welcome new members to the server"""
         channel = member.guild.system_channel
         if channel is not None:
            await channel.send(f'Welcome {member.mention} to the Starship Tracking server! 🚀')

    # --- Basic Commands ---
    @commands.command(name='ping')
    async def ping(self, ctx):
        """Check the bot's latency"""
        latency = round(self.bot.latency * 1000)
        await ctx.send(f'Pong! Latency: {latency}ms')

    @commands.command(name='serverinfo')
    async def server_info(self, ctx):
        """Display information about the server"""
        guild = ctx.guild
        embed = discord.Embed(
            title=f'{guild.name} Server Information',
            color=discord.Color.blue()
        )
        embed.add_field(name='Server ID', value=guild.id)
        embed.add_field(name='Member Count', value=guild.member_count)
        created_at_ts = int(guild.created_at.timestamp())
        embed.add_field(name='Created At', value=f'<t:{created_at_ts}:F>')
        await ctx.send(embed=embed)

    # --- Road Closure Command ---
    @commands.command(name='roadclosure')
    async def road_closure(self, ctx):
         """Display current road closures using live API data."""
         try:
             response = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
             response.raise_for_status()
             closures = response.json()

             embed = discord.Embed(
                 title="Current Road Closures",
                 color=discord.Color.blue(),
                 timestamp=discord.utils.utcnow()
             )
             embed.set_footer(text="Data from starbase.nerdpg.live")

             if not closures:
                 embed.description = "✅ No active road closures reported."
             else:
                 closures_by_status = defaultdict(list)
                 for closure in closures:
                     try:
                         start_ts = int(closure['timestamps']['start'])
                         end_ts = int(closure['timestamps']['end'])
                         status = closure.get('status', 'Unknown Status')
                         time_msg = f"<t:{start_ts}:f> to <t:{end_ts}:f>"
                         closures_by_status[status].append(time_msg)
                     except (KeyError, ValueError, TypeError) as e:
                         print(f"Skipping closure due to error: {e}. Data: {closure}")
                         continue

                 status_emoji = {
                     "Possible Closure": "⚠️", "Closure Scheduled": "✅",
                     "Closure Revoked": "❌", "HWY 4 Road Delay": "⏳", "TFR": "✈️"
                 }
                 default_emoji = "ℹ️"

                 for status, time_messages in closures_by_status.items():
                     value_str = ""
                     for msg in time_messages:
                         line = f"• {msg}\n"
                         if len(value_str) + len(line) > 1024:
                             value_str += "• ... (more entries truncated)\n"
                             break
                         value_str += line
                     value_str = value_str.strip()
                     if value_str:
                         embed.add_field(
                             name=f"{status_emoji.get(status, default_emoji)} {status}",
                             value=value_str,
                             inline=False
                         )

             if len(embed) > 6000:
                 await ctx.send("Error: Closure info too long.")
                 return
             if len(embed.fields) > 25:
                  await ctx.send("Warning: Too many categories. Showing first 25.")

             await ctx.send(embed=embed)

         except requests.exceptions.RequestException as e:
              await ctx.send(f"❌ Error fetching road closures from API: {e}")
         except Exception as e:
             await ctx.send(f"❌ An unexpected error occurred: {str(e)}")

    # --- Monitoring Management Commands ---
    def check_permissions(self, ctx):
        """Helper function to check if user has allowed roles."""
        author_roles = {role.name for role in ctx.author.roles}
        return not self.allowed_roles.isdisjoint(author_roles) or ctx.guild.owner_id == ctx.author.id

    @commands.command(name='monitorclosures')
    @commands.guild_only()
    async def monitor_closures(self, ctx, channel: discord.TextChannel = None):
        """Adds a channel to the road closure monitoring list.

        Defaults to the current channel if none is specified.
        Requires roles: Moderator, Admin, Road Closure Manager (or Server Owner).
        """
        if not self.check_permissions(ctx):
            await ctx.send("⛔ You don't have permission to use this command.")
            return

        target_channel = channel or ctx.channel

        if target_channel.id in self.monitoring_channels:
            await ctx.send(f"⚠️ Channel {target_channel.mention} is already being monitored.")
        else:
            self.monitoring_channels.add(target_channel.id)
            self.save_monitoring_channels() # Save the change
            await ctx.send(f"✅ Okay, I will now send road closure updates to {target_channel.mention}.")
            print(f"Added channel {target_channel.id} to monitoring list by {ctx.author}.")

    @commands.command(name='unmonitorclosures')
    @commands.guild_only()
    async def unmonitor_closures(self, ctx, channel: discord.TextChannel = None):
        """Removes a channel from the road closure monitoring list.

        Defaults to the current channel if none is specified.
        Requires roles: Moderator, Admin, Road Closure Manager (or Server Owner).
        """
        if not self.check_permissions(ctx):
            await ctx.send("⛔ You don't have permission to use this command.")
            return

        target_channel = channel or ctx.channel

        if target_channel.id in self.monitoring_channels:
            self.monitoring_channels.remove(target_channel.id)
            self.save_monitoring_channels() # Save the change
            await ctx.send(f"✅ Okay, I will no longer send road closure updates to {target_channel.mention}.")
            print(f"Removed channel {target_channel.id} from monitoring list by {ctx.author}.")
        else:
            await ctx.send(f"⚠️ Channel {target_channel.mention} is not currently being monitored.")

    @commands.command(name='listmonitored')
    @commands.guild_only()
    async def list_monitored(self, ctx):
        """Lists the channels currently monitored for road closures."""
        if not self.check_permissions(ctx):
            await ctx.send("⛔ You don't have permission to use this command.")
            return

        if not self.monitoring_channels:
            await ctx.send("ℹ️ No channels are currently being monitored for road closures.")
            return

        description_lines = []
        for channel_id in self.monitoring_channels:
            ch = self.bot.get_channel(channel_id)
            if ch and ch.guild == ctx.guild:
                description_lines.append(f"- {ch.mention} (`{channel_id}`)")
            elif ch:
                 description_lines.append(f"- Channel in another server (`{channel_id}`)")
            else:
                description_lines.append(f"- *Unknown/Deleted Channel* (`{channel_id}`)")

        embed = discord.Embed(
            title="Monitored Road Closure Channels",
            description="\n".join(description_lines) or "No channels found in this server.",
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)

    # --- Background Task ---
    def cog_unload(self):
        """Called when the Cog is unloaded."""
        self.check_closures.cancel()
        print("Events Cog Unloaded. Check Closures task cancelled.")

    @tasks.loop(hours=1)
    async def check_closures(self):
        """Background task to check for new road closures."""
        if not self.monitoring_channels:
            return

        print(f"check_closures: Task running. Monitoring {len(self.monitoring_channels)} channels.")
        try:
            response = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
            response.raise_for_status()
            latest_closures = response.json()

            current_ids = {c.get('id') for c in self.current_closures if c.get('id')}
            new_closures = [cl for cl in latest_closures if cl.get('id') and cl.get('id') not in current_ids]

            if new_closures:
                print(f"Found {len(new_closures)} new closures. Processing...")
                processed_new_ids = set()

                for channel_id in list(self.monitoring_channels):
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        print(f"Sending {len(new_closures)} updates to channel {channel.name} ({channel_id})")
                        for closure in new_closures:
                            try:
                                start = int(closure['timestamps']['start'])
                                end = int(closure['timestamps']['end'])
                                time_msg = f'<t:{start}:f> to <t:{end}:f>'
                                status = closure.get('status', 'Unknown Status')
                                closure_id = closure.get('id', 'N/A')

                                status_emoji = {
                                    "Possible Closure": "⚠️", "Closure Scheduled": "✅",
                                    "Closure Revoked": "❌", "HWY 4 Road Delay": "⏳", "TFR": "✈️"
                                }.get(status, "ℹ️")

                                embed = discord.Embed(
                                    title=f"{status_emoji} New Road Closure Update",
                                    description=f"**Status:** {status}\n"
                                                f"**Time:** {time_msg}\n"
                                                f"*ID: `{closure_id}`*",
                                    color=discord.Color.orange(),
                                    timestamp=discord.utils.utcnow()
                                )
                                await channel.send(embed=embed)
                                processed_new_ids.add(closure_id)

                            except (KeyError, ValueError, TypeError) as e:
                                print(f"Error processing new closure {closure.get('id')} for channel {channel_id}: {e}")
                                try:
                                    await channel.send(f"⚠️ Error processing a closure update (ID: {closure.get('id', 'N/A')}). Details logged.")
                                except Exception as send_error:
                                     print(f"Failed to send error message to channel {channel_id}: {send_error}")
                            except discord.Forbidden:
                                print(f"Permission error sending to channel {channel_id}. Removing from monitoring.")
                                self.monitoring_channels.discard(channel_id)
                                self.save_monitoring_channels() # Save removal
                                break
                            except discord.HTTPException as e:
                                print(f"HTTP error sending to channel {channel_id}: {e.status} {e.text}")
                            except Exception as e:
                                print(f"Unexpected error sending closure {closure.get('id')} to channel {channel_id}: {e}")
                                print(traceback.format_exc()) # Keep traceback for unexpected errors

                    else:
                        print(f"Channel {channel_id} not found. Removing from monitoring list.")
                        self.monitoring_channels.discard(channel_id)
                        self.save_monitoring_channels() # Save removal

                # Add successfully processed new closures to the current list
                new_closures_to_add = [cl for cl in new_closures if cl.get('id') in processed_new_ids]
                self.current_closures.extend(new_closures_to_add)
                print(f"Updated current closures list. Total: {len(self.current_closures)}")

        except requests.exceptions.RequestException as e:
            print(f"check_closures: Error fetching API: {e}")
        except Exception as e:
            print(f"check_closures: Error in task loop: {e}")
            print(traceback.format_exc()) # Keep traceback for task loop errors

    @check_closures.before_loop
    async def before_check_closures(self):
        """Wait for the bot to be ready before starting the loop."""
        await self.bot.wait_until_ready()
        print('Bot is ready. Starting check_closures loop.')

# --- Cog Setup ---
async def setup(bot):
    """Loads the Events cog."""
    await bot.add_cog(Events(bot))
    print("Events Cog Loaded.")