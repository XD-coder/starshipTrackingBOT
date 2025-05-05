import discord
from discord.ext import commands, tasks
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import json
import traceback
import os
import uuid
import asyncio
import copy
import logging # Import logging module

# --- Constants ---
STATE_FILENAME = "bot_state.json"
CLOSURE_NAMESPACE = uuid.NAMESPACE_DNS

# --- Setup Logger for this Cog ---
# It's generally recommended to configure logging globally in your main bot file,
# but setting up a logger here works for a self-contained example.
log = logging.getLogger(__name__)
# Set default level - change to logging.DEBUG for more verbose output
log.setLevel(logging.INFO)
# Example basic handler if not configured globally - might print duplicates if global handler exists
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
# handler.setFormatter(formatter)
# if not log.handlers: # Avoid adding multiple handlers if reloaded
#     log.addHandler(handler)

# --- Cog Definition ---

class Events(commands.Cog):
    # --- Initialization & State Persistence ---
    def __init__(self, bot):
        self.bot = bot
        self.monitoring_channels = set()
        self.seen_closure_ids = set()
        self.managed_closures = []
        self.allowed_roles = {"Moderator", "Admin", "Road Closure Manager"}

        log.info("Initializing Events Cog...")
        self.load_state() # Load ALL state from the single file

        self._check_closures_started = False
        if not hasattr(self.check_closures, 'start'):
             log.error("check_closures task is not properly defined or decorated!")
        elif not self.check_closures.is_running():
             try:
                 self.check_closures.start()
                 self._check_closures_started = True
                 log.info("check_closures task started.")
             except RuntimeError as e:
                  log.error(f"Failed to start check_closures task: {e} - Possibly already started or loop error.")
                  self._check_closures_started = self.check_closures.is_running() # Re-check status
        else:
             self._check_closures_started = True # Already running (e.g., cog reload)
             log.warning("check_closures task was already running during init.")

        log.info(f"Events Cog Initialized. Monitoring: {len(self.monitoring_channels)}, Seen API IDs: {len(self.seen_closure_ids)}, Managed Closures: {len(self.managed_closures)}. Task Running: {self.check_closures.is_running()}")

    # --- Unified State Methods ---
    def load_state(self, filename=STATE_FILENAME):
        """Loads ALL bot state from the state file."""
        log.info(f"Attempting to load state from '{filename}'...")
        if not os.path.exists(filename):
             log.warning(f"State file '{filename}' not found. Initializing empty state.")
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []
             return
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
                log.debug(f"Loaded raw state data: {state_data}")

                raw_channels = state_data.get('monitoring_channels', [])
                self.monitoring_channels = set(int(cid) for cid in raw_channels)

                raw_api_ids = state_data.get('seen_closure_ids', [])
                self.seen_closure_ids = set(str(id_val) for id_val in raw_api_ids)

                raw_managed = state_data.get('managed_closures', [])
                if isinstance(raw_managed, list): self.managed_closures = raw_managed
                else: log.warning("'managed_closures' key is not a list in state file. Resetting."); self.managed_closures = []

                log.info(f"Successfully loaded state: Monitoring={len(self.monitoring_channels)}, Seen API IDs={len(self.seen_closure_ids)}, Managed={len(self.managed_closures)}")
        except (json.JSONDecodeError, ValueError, TypeError) as e:
             log.error(f"Error processing state from '{filename}': {e}. Initializing empty state.")
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []
        except IOError as e:
             log.error(f"Error reading state file '{filename}': {e}. Initializing empty state.")
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []
        except Exception as e:
             log.exception(f"Unexpected error loading state from '{filename}'. Initializing empty state.") # Includes traceback
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []

    def save_state(self, filename=STATE_FILENAME):
        """Saves ALL bot state to the state file."""
        log.info(f"Attempting to save state (Mon:{len(self.monitoring_channels)}, Seen:{len(self.seen_closure_ids)}, Man:{len(self.managed_closures)}) to '{filename}'...")
        try:
            state_data = {
                'monitoring_channels': list(self.monitoring_channels),
                'seen_closure_ids': list(self.seen_closure_ids),
                'managed_closures': self.managed_closures
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=4, ensure_ascii=False)
            log.info(f"Successfully saved state to '{filename}'.")
        except (IOError, TypeError) as e:
            log.error(f"Error saving state to '{filename}': {e}")
        except Exception as e:
             log.exception(f"Unexpected error saving state to '{filename}'.") # Includes traceback

    # --- Cog Lifecycle Methods ---
    def cog_unload(self):
        """Called when the Cog is unloaded."""
        self.check_closures.cancel()
        log.info("Events Cog Unloaded. check_closures task cancelled.")

    async def cog_load(self):
        """Called when the Cog is loaded (alternative to __init__ for some setup)."""
        log.info("Events Cog Loaded (cog_load method).")
        # Ensure task is started if not already (useful for reloads)
        if not self.check_closures.is_running():
             try:
                 self.check_closures.start()
                 log.info("check_closures task started from cog_load.")
             except RuntimeError as e:
                  log.error(f"Failed to start check_closures task from cog_load: {e}")


    # --- Listeners ---
    @commands.Cog.listener()
    async def on_ready(self):
        # This specific on_ready within the Cog runs when the Cog is ready.
        await asyncio.sleep(3) # Give time for bot to fully connect and register potentially
        log.info("\n--- Commands Registered (from Events Cog on_ready) ---")
        if self.bot.commands:
            for cmd in sorted(self.bot.commands, key=lambda c: c.name): # Sort for easier reading
                log.info(f"- Name: {cmd.name:<20} | Aliases: {cmd.aliases:<25} | Cog: {cmd.cog_name}")
        else:
            log.warning("!!! No commands seem to be registered with the bot object !!!")
        log.info("------------------------------------------------------\n")


    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        log.info(f'Joined new guild: {guild.name} (id: {guild.id})')

    # @commands.Cog.listener()
    # async def on_member_join(self, member):
    #     log.info(f'Member joined: {member} (id: {member.id}) in guild {member.guild.name}')
    #     channel = member.guild.system_channel
    #     if channel is not None:
    #        try:
    #            await channel.send(f'Welcome {member.mention} to the Starship Tracking server! 🚀')
    #            log.info(f"Sent welcome message to {member} in {channel.name}")
    #        except discord.Forbidden:
    #             log.warning(f"Missing permissions to send welcome message in {channel.name} (guild: {member.guild.name})")
    #        except Exception as e:
    #             log.exception(f"Error sending welcome message for {member}")

    # --- Basic Commands ---
    @commands.command(name='ping')
    async def ping(self, ctx):
        """Check the bot's latency"""
        log.info(f"Command 'ping' invoked by {ctx.author} (ID: {ctx.author.id})")
        latency = round(self.bot.latency * 1000)
        await ctx.send(f'Pong! Latency: {latency}ms')
        log.info(f"Command 'ping' completed for {ctx.author}")

    @commands.command(name='serverinfo')
    async def server_info(self, ctx):
        """Display information about the server"""
        log.info(f"Command 'serverinfo' invoked by {ctx.author} (ID: {ctx.author.id}) in guild {ctx.guild.id}")
        guild = ctx.guild; embed = discord.Embed(title=f'{guild.name} Info', color=discord.Color.blue())
        embed.add_field(name='ID', value=guild.id); embed.add_field(name='Members', value=guild.member_count)
        created_at_ts = int(guild.created_at.timestamp()); embed.add_field(name='Created', value=f'<t:{created_at_ts}:F>')
        await ctx.send(embed=embed)
        log.info(f"Command 'serverinfo' completed for {ctx.author}")

    # --- API Road Closure Command ---
    @commands.command(name='roadclosure', aliases=['closures'])
    async def road_closure(self, ctx):
        """Displays current road closures reported by the API."""
        log.info(f"Command 'roadclosure' invoked by {ctx.author} (ID: {ctx.author.id})")
        api_url = "https://starbase.nerdpg.live/api/json/roadClosures" # Or the live URL
        log.debug(f"Fetching closures from API: {api_url}")
        try:
            response = requests.get(api_url, timeout=10); # Add timeout
            response.raise_for_status()
            closures = response.json()
            timestamp = datetime.now().timestamp()
            print(f'request at timestamp {timestamp}')
            filtered = [x for x in self.managed_closures if x['timestamps']['end'] > timestamp]
            print(type(closures) , type(filtered))
            closures = closures + filtered
            
            
            embed = discord.Embed(title="Current Road Closures", color=discord.Color.blue(), timestamp=discord.utils.utcnow())
            embed.set_footer(text=f"Data from Cameron County")
            if not closures: embed.description = "✅ No active road closures reported by API."
            else:
                # ... (embed building logic remains the same) ...
                closures_by_status = defaultdict(list)
                for closure in closures:
                    try:
                        time = closure['time']
                        start_ts=int(closure['timestamps']['start']); end_ts=int(closure['timestamps']['end'])
                        status=closure.get('status','?'); time_msg=f"<t:{start_ts}:f> to <t:{end_ts}:f>"
                        closures_by_status[status].append(time_msg)
                    except (KeyError, ValueError, TypeError): log.warning(f"Skipping malformed API closure entry: {closure}"); continue
                status_emoji = {"Possible Closure":"⚠️","Closure Scheduled":"✅","Closure Revoked":"❌","HWY 4 Road Delay":"⏳","TFR":"✈️"}; default_emoji="ℹ️"
                for status, time_messages in closures_by_status.items():
                    value_str = ""
                    for msg in time_messages:
                        line = f"• {msg}\n"
                        line2 = f"• {time} | Local Time\n"
                        if len(value_str)+len(line)+len(line2)>1024: value_str += "• ...\n"; break
                        value_str =value_str+line+ line2
                    value_str = value_str.strip()
                    if value_str: embed.add_field(name=f"{status_emoji.get(status,default_emoji)} {status}", value=value_str, inline=False)
            if len(embed) > 6000: log.warning("Embed length > 6000, cannot send."); await ctx.send("Error: Closure info too long."); return
            if len(embed.fields) > 25: log.warning("Embed fields > 25, will be truncated."); await ctx.send("Warning: Too many categories.");
            await ctx.send(embed=embed)
            log.info(f"Command 'roadclosure' completed for {ctx.author}")
        except requests.exceptions.Timeout:
             log.error(f"API Error: Request timed out fetching {api_url}")
             await ctx.send(f"❌ API Error: Timed out connecting to the closure service.")
        except requests.exceptions.RequestException as e:
             log.error(f"API Error fetching {api_url}: {e}")
             await ctx.send(f"❌ API Error: Could not fetch data ({e})")
        except Exception as e:
            log.exception(f"Unexpected error in 'roadclosure' command for {ctx.author}") # Includes traceback
            await ctx.send(f"❌ An unexpected error occurred processing closures.")


    # --- Monitoring Management Commands ---
    def check_permissions(self, ctx):
        """Checks if user has allowed roles or is guild owner."""
        # Add logging to check_permissions if needed (use log.debug usually)
        log.debug(f"Checking permissions for {ctx.author} in {ctx.command.name}")
        if not isinstance(ctx.author, discord.Member) or not hasattr(ctx.author, 'roles'): return False
        if ctx.guild.owner_id == ctx.author.id: return True
        author_roles = {role.name.lower() for role in ctx.author.roles}
        allowed_roles_lower = {role.lower() for role in self.allowed_roles}
        has_perm = not allowed_roles_lower.isdisjoint(author_roles)
        log.debug(f"Permission check for {ctx.author} in {ctx.command.name}: Has required role/owner? {has_perm}")
        return has_perm

    @commands.command(name='monitorclosures')
    @commands.guild_only()
    async def monitor_closures(self, ctx, channel: discord.TextChannel = None):
        """(Mod Only) Adds a channel for API road closure monitoring notices."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'monitorclosures' invoked by {ctx.author} (ID: {ctx.author.id})")
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels:
            log.warning(f"Attempt to monitor already monitored channel {target_channel.id} by {ctx.author}")
            await ctx.send(f"⚠️ {target_channel.mention} is already monitored."); return
        self.monitoring_channels.add(target_channel.id)
        self.save_state()
        await ctx.send(f"✅ Will send API closure updates to {target_channel.mention}.")
        log.info(f"Added channel {target_channel.id} to monitoring by {ctx.author}. Current monitored: {self.monitoring_channels}")

    @commands.command(name='unmonitorclosures')
    @commands.guild_only()
    async def unmonitor_closures(self, ctx, channel: discord.TextChannel = None):
        """(Mod Only) Removes a channel from API closure monitoring."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'unmonitorclosures' invoked by {ctx.author} (ID: {ctx.author.id})")
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels:
            self.monitoring_channels.discard(target_channel.id)
            self.save_state()
            await ctx.send(f"✅ Stopped sending API closure updates to {target_channel.mention}.")
            log.info(f"Removed channel {target_channel.id} from monitoring by {ctx.author}. Current monitored: {self.monitoring_channels}")
        else:
            log.warning(f"Attempt to unmonitor non-monitored channel {target_channel.id} by {ctx.author}")
            await ctx.send(f"⚠️ {target_channel.mention} is not monitored.")

    @commands.command(name='listmonitored')
    @commands.guild_only()
    async def list_monitored(self, ctx):
        """(Mod Only) Lists channels monitored for API closures."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'listmonitored' invoked by {ctx.author} (ID: {ctx.author.id})")
        if not self.monitoring_channels: await ctx.send("ℹ️ No channels monitored for API closures."); return
        description_lines = []; channels_to_remove = set()
        for channel_id in self.monitoring_channels:
            ch = self.bot.get_channel(channel_id)
            if ch and ch.guild == ctx.guild: description_lines.append(f"- {ch.mention} (`{channel_id}`)")
            elif not ch: channels_to_remove.add(channel_id); description_lines.append(f"- *Removed Unknown/Deleted Channel* (`{channel_id}`)")
        if channels_to_remove:
             log.warning(f"Removing {len(channels_to_remove)} unknown channels from monitoring: {channels_to_remove}")
             self.monitoring_channels -= channels_to_remove; self.save_state()
        embed = discord.Embed(title="API Closure Monitoring Channels", description="\n".join(description_lines) or "None.", color=discord.Color.blue())
        await ctx.send(embed=embed)
        log.info(f"Command 'listmonitored' completed for {ctx.author}")

    # --- Commands for Managing Local/Managed Closures ---

    @commands.command(name='listroadclosures', aliases=['listmyclosures', 'lmc'])
    @commands.guild_only()
    async def list_managed_road_closures(self, ctx):
        """(Mod Only) Lists road closures managed locally by the bot."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'listroadclosures' invoked by {ctx.author} (ID: {ctx.author.id})")
        closures = self.managed_closures
        if not closures:
            await ctx.send("ℹ️ There are no road closures currently managed by the bot.")
            log.info("'listroadclosures': No managed closures found.")
            return

        log.info(f"'listroadclosures': Found {len(closures)} managed closures.")
        output_lines = ["**Managed Road Closures (Local Bot List):**"]
        for i, closure in enumerate(closures):
            closure_id = closure.get('id', 'N/A')
            status = closure.get('status', 'N/A')
            date = closure.get('date', 'N/A')
            output_lines.append(f"`{i+1}`. **ID:** `{closure_id}`\n   - **Status:** {status}\n   - **Date:** {date}")

        current_message = ""
        for line in output_lines:
            if len(current_message) + len(line) + 2 > 2000:
                await ctx.send(current_message); current_message = line + "\n"
            else: current_message += line + "\n"
        if current_message: await ctx.send(current_message)
        log.info(f"Command 'listroadclosures' completed for {ctx.author}")

    @commands.command(name='addroadclosure', aliases=['addmyclosure', 'amc'])
    @commands.guild_only()
    async def add_managed_road_closure(self, ctx):
        """(Mod Only) Interactively adds a new road closure to the bot's local list."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'addroadclosure' invoked by {ctx.author} (ID: {ctx.author.id})")
        new_closure_input = {}; cancelled = False; timeout_duration = 180.0
        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        prompts = { # Use more descriptive keys if needed
            "status": "📝 **Status?**", "date": "📅 **Date(s)?**", "time": "⏰ **Time range?**",
            "type": "🏷️ **Type?**", "start_timestamp": "▶️ **Start Unix Timestamp?**",
            "end_timestamp": "⏹️ **End Unix Timestamp?**", "notes": "📄 **Notes?** (`none` if empty)"
        }
        await ctx.send(f"Adding a new **locally managed** closure.\nType `cancel` to stop.")

        for key, prompt in prompts.items():
            log.debug(f"'addroadclosure': Prompting for '{key}'")
            await ctx.send(prompt)
            try:
                msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                log.debug(f"'addroadclosure': Received response for '{key}': '{msg.content[:50]}...'")
            except asyncio.TimeoutError:
                 log.warning(f"'addroadclosure' timed out waiting for {key} from {ctx.author}")
                 await ctx.send(f"⏰ Timed out."); return

            if msg.content.lower() == 'cancel': cancelled = True; break
            value = msg.content.strip()

            if key.endswith("_timestamp"):
                if not value.isdigit():
                    log.warning(f"'addroadclosure': Invalid timestamp '{value}' for {key} by {ctx.author}")
                    await ctx.send(f"❌ Invalid Timestamp. Aborting."); return
                new_closure_input[key] = int(value)
            elif key == "notes" and value.lower() == 'none': new_closure_input[key] = None
            else: new_closure_input[key] = value

        if cancelled: log.info(f"'addroadclosure' cancelled by {ctx.author}"); await ctx.send("❌ Addition cancelled."); return

        closure_data = {
            "id": str(uuid.uuid4()), "date": new_closure_input.get("date"), "status": new_closure_input.get("status"),
            "time": new_closure_input.get("time"), "timestamps": {"start": new_closure_input.get("start_timestamp"), "end": new_closure_input.get("end_timestamp")},
            "type": new_closure_input.get("type"), "notes": new_closure_input.get("notes")
        }
        log.info(f"'addroadclosure': Preparing to add closure: {closure_data}")

        self.managed_closures.append(closure_data)
        self.save_state() # Assumes save_state logs its own success/failure
        await ctx.send(f"✅ **Locally managed** closure added!\n**ID:** `{closure_data['id']}`")
        log.info(f"Command 'addroadclosure' completed by {ctx.author}, new ID: {closure_data['id']}")


    @commands.command(name='editroadclosure', aliases=['editmyclosure', 'emc'])
    @commands.guild_only()
    async def edit_managed_road_closure(self, ctx, closure_id: str):
        """(Mod Only) Interactively edits a locally managed closure by its ID."""

        # Check for missing permissions
        if not self.check_permissions( ctx): 
            log.warning(f"Permission denied for {ctx.author} in {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'editroadclosure' invoked by {ctx.author} (ID: {ctx.author.id}) for closure ID: {closure_id}")
        target_closure = None; target_index = -1

        for i, closure in enumerate(self.managed_closures):
            if closure.get('id') == closure_id:
                target_closure = copy.deepcopy(closure); target_index = i; break

        if target_closure is None:
            log.warning(f"'editroadclosure': Closure ID '{closure_id}' not found by {ctx.author}")
            await ctx.send(f"❌ Locally managed closure ID `{closure_id}` not found."); return

        # ... (embed display logic as before) ...
        embed = discord.Embed(title=f"Editing Managed Closure ID: {closure_id}", color=discord.Color.orange())
        for k, v in target_closure.items(): val_str = f"`{json.dumps(v)}`" if isinstance(v, dict) else f"`{v}`"; embed.add_field(name=k.capitalize(), value=val_str, inline=False)
        await ctx.send(embed=embed)
        await ctx.send("Edit fields: Enter new value, `skip`, or `cancel`.")

        edited_closure = target_closure; cancelled = False; timeout_duration = 180.0
        def check(m): return m.author == ctx.author and m.channel == ctx.channel
        editable_fields = ["status", "date", "time", "type", "notes", "start_timestamp", "end_timestamp"]

        for key in editable_fields:
            is_timestamp = key.endswith("_timestamp")
            prompt_key = key.replace("_timestamp", "")
            current_value = edited_closure.get("timestamps", {}).get(prompt_key) if is_timestamp else edited_closure.get(key)
            prompt = f"✏️ Edit **'{prompt_key if is_timestamp else key}'** (Current: `{current_value}`)? Enter new value, `skip`, or `cancel`:"
            log.debug(f"'editroadclosure': Prompting for '{key}' (ID: {closure_id})")
            await ctx.send(prompt)
            try:
                msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                log.debug(f"'editroadclosure': Received response for '{key}': '{msg.content[:50]}...'")
            except asyncio.TimeoutError: log.warning(f"'editroadclosure' timed out for ID {closure_id}"); await ctx.send("⏰ Timed out."); return

            content_lower=msg.content.lower(); content_strip=msg.content.strip()
            if content_lower == 'cancel': cancelled = True; break
            if content_lower == 'skip': log.debug(f"'editroadclosure': Skipped '{key}'"); continue

            if is_timestamp:
                if not content_strip.isdigit(): log.warning(f"'editroadclosure': Invalid timestamp '{content_strip}' for {key}"); await ctx.send(f"❌ Invalid Timestamp. Keeping `{current_value}`."); continue
                if "timestamps" not in edited_closure: edited_closure["timestamps"] = {}
                edited_closure["timestamps"][prompt_key] = int(content_strip); log.debug(f"Updated timestamp {prompt_key}")
            elif key == "notes" and content_lower == 'none': edited_closure[key] = None; log.debug(f"Updated {key} to None")
            else: edited_closure[key] = content_strip; log.debug(f"Updated {key}")

        if cancelled: log.info(f"'editroadclosure' cancelled by {ctx.author} for ID {closure_id}"); await ctx.send("❌ Edit cancelled."); return

        log.info(f"'editroadclosure': Finished editing for ID {closure_id}, attempting save.")
        self.managed_closures[target_index] = edited_closure
        self.save_state()
        await ctx.send(f"✅ Locally managed closure `{closure_id}` updated!")
        # ... (final embed display as before) ...
        embed.title=f"Updated Managed Closure ID: {closure_id}"; embed.color=discord.Color.green(); embed.clear_fields()
        for k, v in edited_closure.items(): val_str = f"`{json.dumps(v)}`" if isinstance(v, dict) else f"`{v}`"; embed.add_field(name=k.capitalize(), value=val_str, inline=False)
        await ctx.send(embed=embed)
        log.info(f"Command 'editroadclosure' completed by {ctx.author} for ID {closure_id}")


    # --- Background Task (API Checking) ---
    @tasks.loop(hours=1) # Production interval
    async def check_closures(self):
        """Background task to check external API for new road closures."""
        if not self.monitoring_channels: return
        api_url = "https://starbase.nerdpg.live/api/json/roadClosures"
        log.info(f"Task 'check_closures': Running check against API: {api_url}")
        try:
            response = requests.get(api_url, timeout=15); response.raise_for_status(); latest_closures = response.json()
            if not isinstance(latest_closures, list): log.warning(f"API Error: Expected list. Skipping."); return

            # Generate UUIDs and Compare
            latest_generated_ids = set(); closures_with_generated_ids = {}
            log.debug(f"Task 'check_closures': Processing {len(latest_closures)} items from API...")
            for cl in latest_closures:
                try:
                    # ... (UUID generation logic as before) ...
                    status=cl.get('status','?'); start_ts=cl['timestamps']['start']
                    end_ts=cl['timestamps']['end']; type_val=cl.get('type','?')
                    name_str = f"{status}|{start_ts}|{end_ts}|{type_val}"
                    generated_uuid = str(uuid.uuid5(CLOSURE_NAMESPACE, name_str))
                    latest_generated_ids.add(generated_uuid); closures_with_generated_ids[generated_uuid] = cl
                except (KeyError, TypeError) as e: log.warning(f"Task 'check_closures': Skipping API item due to missing data for ID gen: {cl} | Error: {e}"); continue

            new_closure_generated_ids = latest_generated_ids - self.seen_closure_ids
            log.info(f"Task 'check_closures': Found {len(new_closure_generated_ids)} new closures via API.")

            if new_closure_generated_ids:
                processed_successfully_ids = set()
                log.info(f"Task 'check_closures': Processing notifications for new generated IDs: {new_closure_generated_ids}")
                # --- Notification Loop ---
                for generated_uuid in new_closure_generated_ids:
                    closure = closures_with_generated_ids[generated_uuid]
                    log.debug(f"Task 'check_closures': Notifying for GenUUID {generated_uuid}")
                    for channel_id in list(self.monitoring_channels): # Iterate copy
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            try:
                                # ... (Embed creation and sending logic as before) ...
                                start=int(closure['timestamps']['start']); end=int(closure['timestamps']['end'])
                                time_msg=f'<t:{start}:f> to <t:{end}:f>'; status=closure.get('status','?')
                                status_emoji={"Possible Closure":"⚠️","Closure Scheduled":"✅","Closure Revoked":"❌","HWY 4 Road Delay":"⏳","TFR":"✈️"}.get(status,"ℹ️")
                                embed = discord.Embed(title=f"{status_emoji} New API Closure Update", description=f"**Status:** {status}\n**Time:** {time_msg}\n*Event ID: `{generated_uuid[:8]}`*", color=discord.Color.orange(), timestamp=discord.utils.utcnow())

                                await channel.send(embed=embed)
                                log.info(f"Task 'check_closures': Sent notification for GenUUID {generated_uuid} to channel {channel_id}")
                                processed_successfully_ids.add(generated_uuid) # Only add after at least one success
                            except discord.Forbidden: log.error(f"Task 'check_closures': PERMISSION ERROR sending to channel {channel_id}. Removing."); self.monitoring_channels.discard(channel_id); self.save_state(); # Remove bad channel
                            except discord.HTTPException as e: log.error(f"Task 'check_closures': HTTP Error sending to channel {channel_id}: {e.status} {e.text}")
                            except Exception as e: log.exception(f"Task 'check_closures': Unexpected error sending GenUUID {generated_uuid} to channel {channel_id}")
                        # else: log.warning(f"Task 'check_closures': Channel {channel_id} not found during notification.") # Already handled by listmonitored

                # --- State Update Logic ---
                if processed_successfully_ids:
                    log.info(f"Task 'check_closures': Adding {len(processed_successfully_ids)} new seen UUIDs to state.")
                    self.seen_closure_ids.update(processed_successfully_ids); self.save_state()

        except requests.exceptions.RequestException as e: log.error(f"Task 'check_closures': API fetch error: {e}")
        except Exception as e: log.exception(f"Task 'check_closures': Error in task loop") # Includes traceback
        # finally: log.debug(f"Task 'check_closures': Finished run. Seen IDs: {self.seen_closure_ids}")

    @check_closures.before_loop
    async def before_check_closures(self):
        """Wait for the bot to be ready before starting the loop."""
        log.info('Task \'check_closures\': Waiting for bot readiness...')
        await self.bot.wait_until_ready()
        log.info('Task \'check_closures\': Bot ready. Loop starting.')

# --- Cog Setup Function ---
async def setup(bot):
    """Loads the Events cog."""
    # Configure logging basic settings IF NOT DONE GLOBALLY in main bot file
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    await bot.add_cog(Events(bot))
    log.info("Events Cog Added to Bot.") # Log success of adding cog