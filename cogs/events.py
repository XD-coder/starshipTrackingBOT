import discord
from discord.ext import commands, tasks
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import json
import traceback
import os
import uuid # Import the UUID module

STATE_FILENAME = "bot_state.json"
# Define a consistent namespace UUID for generating closure IDs.
# Using NAMESPACE_DNS is arbitrary; you could generate a random one once
# and hardcode it for a more unique namespace if desired.
CLOSURE_NAMESPACE = uuid.NAMESPACE_DNS

class Events(commands.Cog):
    # --- Initialization & State Persistence ---
    def __init__(self, bot):
        self.bot = bot
        self.monitoring_channels = set()
        self.seen_closure_ids = set() # Will store generated UUIDs (as strings)
        self.allowed_roles = {"Moderator", "Admin", "Road Closure Manager"}

        self.load_state()

        self._check_closures_started = False
        if not self._check_closures_started:
             self.check_closures.start()
             self._check_closures_started = True
        print(f"Events Cog Initialized. Monitoring: {len(self.monitoring_channels)}, Seen IDs: {len(self.seen_closure_ids)}. Task Started: {self.check_closures.is_running()}")

    # --- load_state and save_state remain the same, they handle strings ---
    def load_state(self, filename=STATE_FILENAME):
        """Loads monitoring channels and seen closure IDs (UUID strings) from the state file."""
        print(f"Attempting to load state from '{filename}'...")
        if not os.path.exists(filename):
             print(f"State file '{filename}' not found. Initializing empty state.")
             self.monitoring_channels = set()
             self.seen_closure_ids = set()
             return
        try:
            with open(filename, 'r') as f:
                state_data = json.load(f)
                print(f"Loaded raw state data: {state_data}")
                raw_channels = state_data.get('monitoring_channels', [])
                self.monitoring_channels = set(int(cid) for cid in raw_channels)
                # Load seen UUIDs (should be strings)
                raw_ids = state_data.get('seen_closure_ids', [])
                self.seen_closure_ids = set(str(id_val) for id_val in raw_ids)
                print(f"Successfully loaded state: Monitoring Channels={len(self.monitoring_channels)}, Seen IDs={len(self.seen_closure_ids)}")
        except (json.JSONDecodeError, ValueError, TypeError, IOError) as e:
             print(f"Error loading state from '{filename}': {e}. Initializing empty state.")
             self.monitoring_channels = set()
             self.seen_closure_ids = set()
        except Exception as e:
             print(f"Unexpected error loading state: {e}. Initializing empty state.")
             print(traceback.format_exc())
             self.monitoring_channels = set()
             self.seen_closure_ids = set()

    def save_state(self, filename=STATE_FILENAME):
        """Saves monitoring channels and seen closure IDs (UUID strings) to the state file."""
        print(f"Attempting to save state (Monitoring: {len(self.monitoring_channels)}, Seen IDs: {len(self.seen_closure_ids)}) to '{filename}'...")
        try:
            state_data = {
                'monitoring_channels': list(self.monitoring_channels),
                'seen_closure_ids': list(self.seen_closure_ids) # Store the generated UUID strings
            }
            with open(filename, 'w') as f:
                json.dump(state_data, f, indent=4)
            print(f"Successfully saved state to '{filename}'.")
        except IOError as e:
            print(f"Error saving state to '{filename}': {e}")
        except Exception as e:
             print(f"Unexpected error saving state: {e}")
             print(traceback.format_exc())

    # --- Listeners (Keep as is) ---
    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        print(f'Joined new guild: {guild.name} (id: {guild.id})')

    @commands.Cog.listener()
    async def on_member_join(self, member):
        channel = member.guild.system_channel
        if channel is not None:
           await channel.send(f'Welcome {member.mention} to the Starship Tracking server! 🚀')

    # --- Basic Commands (Keep as is) ---
    @commands.command(name='ping')
    async def ping(self, ctx):
        latency = round(self.bot.latency * 1000)
        await ctx.send(f'Pong! Latency: {latency}ms')

    @commands.command(name='serverinfo')
    async def server_info(self, ctx):
        # ... (no changes needed) ...
        guild = ctx.guild
        embed = discord.Embed(title=f'{guild.name} Server Information', color=discord.Color.blue())
        embed.add_field(name='Server ID', value=guild.id)
        embed.add_field(name='Member Count', value=guild.member_count)
        created_at_ts = int(guild.created_at.timestamp())
        embed.add_field(name='Created At', value=f'<t:{created_at_ts}:F>')
        await ctx.send(embed=embed)

    # --- Road Closure Command (Keep as is) ---
    @commands.command(name='roadclosure')
    async def road_closure(self, ctx):
        """Display current road closures using live API data."""
        # ... (no changes needed) ...
        try:
            response = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
            response.raise_for_status()
            closures = response.json()
            embed = discord.Embed(title="Current Road Closures", color=discord.Color.blue(), timestamp=discord.utils.utcnow())
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
                    except (KeyError, ValueError, TypeError) as e: continue
                status_emoji = {"Possible Closure": "⚠️", "Closure Scheduled": "✅", "Closure Revoked": "❌", "HWY 4 Road Delay": "⏳", "TFR": "✈️"}
                default_emoji = "ℹ️"
                for status, time_messages in closures_by_status.items():
                    value_str = ""
                    for msg in time_messages:
                        line = f"• {msg}\n";
                        if len(value_str) + len(line) > 1024: value_str += "• ... (more entries truncated)\n"; break
                        value_str += line
                    value_str = value_str.strip()
                    if value_str: embed.add_field(name=f"{status_emoji.get(status, default_emoji)} {status}", value=value_str, inline=False)
            if len(embed) > 6000: await ctx.send("Error: Closure info too long."); return
            if len(embed.fields) > 25: await ctx.send("Warning: Too many categories. Showing first 25.")
            await ctx.send(embed=embed)
        except requests.exceptions.RequestException as e: await ctx.send(f"❌ Error fetching road closures from API: {e}")
        except Exception as e: await ctx.send(f"❌ An unexpected error occurred: {str(e)}")

    # --- Monitoring Management Commands (Keep save_state call) ---
    def check_permissions(self, ctx):
        """Helper function to check if user has allowed roles."""
        author_roles = {role.name for role in ctx.author.roles}
        return not self.allowed_roles.isdisjoint(author_roles) or ctx.guild.owner_id == ctx.author.id

    @commands.command(name='monitorclosures')
    @commands.guild_only()
    async def monitor_closures(self, ctx, channel: discord.TextChannel = None):
        """Adds a channel to the road closure monitoring list."""
        # ... (permission check as before) ...
        if not self.check_permissions(ctx): await ctx.send("⛔ You don't have permission."); return
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels: await ctx.send(f"⚠️ Channel {target_channel.mention} is already monitored."); return
        self.monitoring_channels.add(target_channel.id)
        self.save_state()
        await ctx.send(f"✅ Okay, I will now send closure updates to {target_channel.mention}.")
        print(f"Added channel {target_channel.id} to monitoring list by {ctx.author}.")

    @commands.command(name='unmonitorclosures')
    @commands.guild_only()
    async def unmonitor_closures(self, ctx, channel: discord.TextChannel = None):
        """Removes a channel from the road closure monitoring list."""
        # ... (permission check as before) ...
        if not self.check_permissions(ctx): await ctx.send("⛔ You don't have permission."); return
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels:
            self.monitoring_channels.discard(target_channel.id)
            self.save_state()
            await ctx.send(f"✅ Okay, I will no longer send closure updates to {target_channel.mention}.")
            print(f"Removed channel {target_channel.id} from monitoring list by {ctx.author}.")
        else: await ctx.send(f"⚠️ Channel {target_channel.mention} is not monitored.")

    @commands.command(name='listmonitored')
    @commands.guild_only()
    async def list_monitored(self, ctx):
        """Lists the channels currently monitored for road closures."""
        # ... (permission check as before) ...
        if not self.check_permissions(ctx): await ctx.send("⛔ You don't have permission."); return
        if not self.monitoring_channels: await ctx.send("ℹ️ No channels monitored."); return
        description_lines = []; channels_to_remove = set()
        for channel_id in self.monitoring_channels:
            ch = self.bot.get_channel(channel_id)
            if ch and ch.guild == ctx.guild: description_lines.append(f"- {ch.mention} (`{channel_id}`)")
            elif ch: description_lines.append(f"- Channel in other server (`{channel_id}`)")
            else: description_lines.append(f"- *Unknown/Deleted Channel* (`{channel_id}`)"); channels_to_remove.add(channel_id)
        if channels_to_remove: print(f"Removing {len(channels_to_remove)} unknown channels."); self.monitoring_channels -= channels_to_remove; self.save_state()
        embed = discord.Embed(title="Monitored Channels", description="\n".join(description_lines) or "None.", color=discord.Color.blue())
        await ctx.send(embed=embed)

    # --- Background Task ---
    def cog_unload(self):
        """Called when the Cog is unloaded."""
        self.check_closures.cancel()
        print("Events Cog Unloaded. Check Closures task cancelled.")

    @tasks.loop(hours=1) # DEBUG: Check every 60 seconds - CHANGE BACK TO hours=1 FOR PRODUCTION
    async def check_closures(self):
        """Background task to check for new road closures."""
        if not self.monitoring_channels: return

        print(f"\n--- Running check_closures at {datetime.now()} ---")
        print(f"Current seen_closure_ids (start): {self.seen_closure_ids}")

        try:
            response = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
            response.raise_for_status()
            latest_closures = response.json()

            if not isinstance(latest_closures, list):
                 print(f"API Error: Expected list, got {type(latest_closures)}. Skipping."); return

            # --- Generate UUIDs and Compare ---
            latest_generated_ids = set()
            closures_with_generated_ids = {} # Map generated_id -> closure_data

            print(f"Processing {len(latest_closures)} items from API to generate IDs...")
            for cl in latest_closures:
                try:
                    # Extract key components for generating a stable ID
                    status = cl.get('status', 'Unknown Status')
                    start_ts = cl['timestamps']['start']
                    end_ts = cl['timestamps']['end']
                    type_val = cl.get('type', 'Unknown Type') # Include type for more uniqueness

                    # Create a consistent string representation for the UUID name
                    # Using '|' as a delimiter
                    name_str = f"{status}|{start_ts}|{end_ts}|{type_val}"

                    # Generate the deterministic UUID using namespace and name string
                    generated_uuid = str(uuid.uuid5(CLOSURE_NAMESPACE, name_str))

                    latest_generated_ids.add(generated_uuid)
                    closures_with_generated_ids[generated_uuid] = cl # Store original data mapped by UUID

                except (KeyError, TypeError) as e:
                     print(f"Warning: Skipping closure due to missing data for ID generation: {cl} | Error: {e}")
                     continue # Skip if essential data for ID is missing

            print(f"Generated {len(latest_generated_ids)} unique UUIDs from API data.")
            print(f"Comparing against {len(self.seen_closure_ids)} seen UUIDs.")

            new_closure_generated_ids = latest_generated_ids - self.seen_closure_ids
            print(f"Found {len(new_closure_generated_ids)} new closures based on generated UUIDs.") # Log the actual new UUIDs {new_closure_generated_ids}


            if new_closure_generated_ids:
                processed_successfully_ids = set() # Track generated UUIDs successfully notified

                # --- Notification Loop ---
                for generated_uuid in new_closure_generated_ids:
                    closure = closures_with_generated_ids[generated_uuid] # Get original data
                    print(f"Processing new closure with generated UUID: {generated_uuid}")

                    for channel_id in list(self.monitoring_channels):
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            # Double check if already processed in *this specific run* for *this channel* (overkill but safe)
                            # if generated_uuid in processed_successfully_ids: continue # This logic is slightly flawed, process per channel

                            try:
                                start = int(closure['timestamps']['start'])
                                end = int(closure['timestamps']['end'])
                                time_msg = f'<t:{start}:f> to <t:{end}:f>'
                                status = closure.get('status', 'Unknown Status')
                                status_emoji = {"Possible Closure": "⚠️", "Closure Scheduled": "✅", "Closure Revoked": "❌", "HWY 4 Road Delay": "⏳", "TFR": "✈️"}.get(status, "ℹ️")

                                embed = discord.Embed(
                                    title=f"{status_emoji} New Road Closure Update",
                                    description=f"**Status:** {status}\n"
                                                f"**Time:** {time_msg}\n"
                                                , # Show part of generated UUID
                                    color=discord.Color.orange(), timestamp=discord.utils.utcnow()
                                )
                                await channel.send(embed=embed)
                                print(f"Successfully sent notification for GenUUID {generated_uuid} to channel {channel_id}")
                                # Mark this UUID as successfully processed *overall* after first successful send
                                processed_successfully_ids.add(generated_uuid)

                            except (KeyError, ValueError, TypeError) as e: print(f"Error processing data for GenUUID {generated_uuid} in channel {channel_id}: {e}")
                            except discord.Forbidden: print(f"PERMISSION ERROR sending to channel {channel_id}. Removing."); self.monitoring_channels.discard(channel_id); self.save_state(); break # Stop trying this channel
                            except discord.HTTPException as e: print(f"HTTP Error sending to channel {channel_id}: {e.status} {e.text}")
                            except Exception as e: print(f"Unexpected error sending GenUUID {generated_uuid} to channel {channel_id}: {e}"); print(traceback.format_exc())
                        # else: # No need for 'else' here, loop continues or breaks based on Forbidden error
                    # else: # Channel not found logic (already handled by listmonitored implicitly, but can be added here too)
                        # print(f"Channel {channel_id} not found during notification. Will be removed on next list.")


                # --- State Update Logic ---
                if processed_successfully_ids:
                    print(f"Adding {len(processed_successfully_ids)} successfully processed generated UUIDs to seen list: {processed_successfully_ids}")
                    self.seen_closure_ids.update(processed_successfully_ids)
                    self.save_state()
                else:
                     print("No new closures were successfully processed and notified.")
            else:
                 print("No new closures detected based on generated UUIDs.")

        except requests.exceptions.RequestException as e: print(f"check_closures: Error fetching API: {e}")
        except Exception as e: print(f"check_closures: Error in task loop: {e}"); print(traceback.format_exc())
        finally:
             print(f"Current seen_closure_ids (end): {self.seen_closure_ids}")
             print("--- check_closures finished ---")


    @check_closures.before_loop
    async def before_check_closures(self):
        """Wait for the bot to be ready before starting the loop."""
        print('check_closures: Waiting for bot to be ready...')
        await self.bot.wait_until_ready()
        print('check_closures: Bot is ready. Loop will start.')

# --- Cog Setup ---
async def setup(bot):
    """Loads the Events cog."""
    await bot.add_cog(Events(bot))
    print("Events Cog Loaded.")