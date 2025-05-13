import discord
from discord.ext import commands, tasks
# Replace 'requests' with 'aiohttp' for asynchronous HTTP requests
import aiohttp
from datetime import datetime, timedelta
from collections import defaultdict
import json
import traceback
import os
import uuid
import asyncio
import copy
import logging
# Use asyncio.to_thread for blocking file I/O
import functools

# --- Constants ---
STATE_FILENAME = "bot_state.json"
# CLOSURE_NAMESPACE is defined within the cog class

# --- Setup Logger for this Cog ---
# Use a specific logger name for this cog
log = logging.getLogger('EventsCog')
# Configure logging basic settings IF NOT DONE GLOBALLY in main bot file
# If you configure logging in your main bot.py, you can remove this block
# logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
# log.setLevel(logging.DEBUG) # Uncomment for more verbose output from this cog


# --- Define potential status emojis and colors (Based on provided API data) ---
STATUS_EMOJIS = {
    "Possible Closure": "⚠️",
    "Closure Scheduled": "✅",
    "Closure Revoked": "❌",
    "HWY 4 Road Delay": "⏳",
    "TFR": "✈️"
}

STATUS_COLORS = {
    "Possible Closure": discord.Color.orange(),
    "Closure Scheduled": discord.Color.green(),
    "Closure Revoked": discord.Color.red(),
    "HWY 4 Road Delay": discord.Color.gold(),
    "TFR": discord.Color.blue(),
    "Default": discord.Color.greyple() # Fallback color
}

# --- Helper function to get color based on status ---
def get_status_color(status):
    """Returns a color based on the closure status."""
    return STATUS_COLORS.get(status, STATUS_COLORS["Default"])

# --- Helper function to get emoji based on status ---
def get_status_emoji(status):
    """Returns an emoji based on the closure status."""
    return STATUS_EMOJIS.get(status, "ℹ️") # Default info emoji


# --- Cog Definition ---

class Events(commands.Cog):
    # Define CLOSURE_NAMESPACE as a class attribute
    CLOSURE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'starbase.nerdpg.live.closures')

    def __init__(self, bot):
        self.bot = bot
        self.monitoring_channels = set()
        self.seen_closure_ids = set()
        self.managed_closures = []
        self.allowed_roles = {"Moderator", "Admin", "Road Closure Manager"}

        # Cache for the latest API data
        self._cached_api_closures = []
        self._last_api_fetch_time = None # type: Optional[datetime]
        self._api_fetch_lock = asyncio.Lock() # Prevent multiple simultaneous fetches

        log.info("Initializing Events Cog...")
        # Load state using async method, but call it in __init__ and handle potential non-awaitability
        # The state will actually be loaded when the bot is ready in before_check_closures
        # This just sets up the state attributes.
        self.load_state_initial()


    # --- Asynchronous State Persistence Methods ---
    # Use asyncio.to_thread for blocking file I/O
    async def load_state(self, filename=STATE_FILENAME):
        """Loads ALL bot state from the state file asynchronously."""
        log.info(f"Attempting to load state from '{filename}'...")
        # Run blocking file operation in a separate thread
        loop = asyncio.get_running_loop()
        try:
            state_data = await loop.run_in_executor(
                 None, # Use default thread pool executor
                 functools.partial(self._blocking_load_state, filename)
            )

            # Process loaded data in the event loop thread
            if state_data is None:
                 log.warning(f"State file '{filename}' not found or empty. Initializing empty state.")
                 self.monitoring_channels = set()
                 self.seen_closure_ids = set()
                 self.managed_closures = []
                 # Also reset cache on state load failure/empty
                 self._cached_api_closures = []
                 self._last_api_fetch_time = None
                 return

            raw_channels = state_data.get('monitoring_channels', [])
            self.monitoring_channels = set(int(cid) for cid in raw_channels)
            log.debug(f"Loaded monitoring_channels: {self.monitoring_channels}")

            raw_api_ids = state_data.get('seen_closure_ids', [])
            self.seen_closure_ids = set(str(id_val) for id_val in raw_api_ids)
            log.debug(f"Loaded seen_closure_ids: {self.seen_closure_ids}")

            raw_managed = state_data.get('managed_closures', [])
            if isinstance(raw_managed, list):
                self.managed_closures = raw_managed
                log.debug(f"Loaded managed_closures: {len(self.managed_closures)} items")
            else:
                log.warning("'managed_closures' key is not a list in state file. Resetting managed closures.")
                self.managed_closures = []

            # Load cached data if present (optional, depending on how fresh you need it)
            # For this example, we won't persist the cache itself, just load core state.
            # self._cached_api_closures = state_data.get('_cached_api_closures', [])
            # last_fetch_ts = state_data.get('_last_api_fetch_time')
            # self._last_api_fetch_time = datetime.fromtimestamp(last_fetch_ts) if last_fetch_ts else None


            log.info(f"Successfully loaded state: Monitoring={len(self.monitoring_channels)}, Seen API IDs={len(self.seen_closure_ids)}, Managed={len(self.managed_closures)}")

        except (json.JSONDecodeError, ValueError, TypeError) as e:
             log.error(f"Error processing state from '{filename}': {e}. Initializing empty state.")
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []
             self._cached_api_closures = []; self._last_api_fetch_time = None # Reset cache on state error
        except Exception as e:
             log.exception(f"Unexpected error loading state from '{filename}'. Initializing empty state.")
             self.monitoring_channels = set(); self.seen_closure_ids = set(); self.managed_closures = []
             self._cached_api_closures = []; self._last_api_fetch_time = None # Reset cache on unexpected error


    # Synchronous helper for load_state
    def _blocking_load_state(self, filename):
         """Synchronous file loading part for load_state."""
         if not os.path.exists(filename):
              return None # Indicate file not found
         try:
              with open(filename, 'r', encoding='utf-8') as f:
                   # Check if file is empty before loading JSON
                   content = f.read()
                   if not content:
                        return None # Indicate empty file
                   return json.loads(content)
         except IOError as e:
              log.error(f"Error reading state file '{filename}': {e}")
              raise # Re-raise to be caught by the async part


    async def save_state(self, filename=STATE_FILENAME):
        """Saves ALL bot state to the state file asynchronously."""
        log.info(f"Attempting to save state (Mon:{len(self.monitoring_channels)}, Seen:{len(self.seen_closure_ids)}, Man:{len(self.managed_closures)}) to '{filename}'...")
        # Prepare data in the event loop thread
        state_data = {
            'monitoring_channels': list(self.monitoring_channels),
            'seen_closure_ids': list(self.seen_closure_ids),
            'managed_closures': self.managed_closures
            # We are not persisting the cache itself
        }

        # Run blocking file operation in a separate thread
        loop = asyncio.get_running_loop()
        try:
            # Use functools.partial to pass filename and kwargs to the sync function
            await loop.run_in_executor(
                 None, # Use default thread pool executor
                 functools.partial(self._blocking_save_state, filename, state_data)
            )
            log.info(f"Successfully saved state to '{filename}'.")
        except Exception as e: # Catch exceptions from the executor thread
             log.exception(f"Error saving state to '{filename}'.")


    # Synchronous helper for save_state
    def _blocking_save_state(self, filename, state_data):
         """Synchronous file saving part for save_state."""
         try:
             with open(filename, 'w', encoding='utf-8') as f:
                 json.dump(state_data, f, indent=4, ensure_ascii=False)
         except IOError as e:
             log.error(f"Error writing state file '{filename}': {e}")
             raise # Re-raise to be caught by the async part


    # Called during __init__ - not async, just sets up initial empty state
    def load_state_initial(self, filename=STATE_FILENAME):
        """Initial (non-async) state load attempt for basic setup in __init__."""
        # This is a simplified sync load just to populate attributes,
        # the full async load happens later in before_loop.
        # This prevents needing await in __init__.
        self.monitoring_channels = set()
        self.seen_closure_ids = set()
        self.managed_closures = []
        # We don't load from file here, just set empty defaults.
        # The actual loading happens in the async load_state.
        log.debug("Initial state attributes set in __init__.")


    # --- Cog Lifecycle Methods ---
    def cog_unload(self):
        """Called when the Cog is unloaded."""
        # It's safe to call cancel even if not running, it just does nothing.
        self.check_closures.cancel()
        log.info("Events Cog Unloaded. check_closures task cancelled.")
        # Consider saving state one last time here in case of graceful shutdown
        # However, cog_unload is synchronous, so it would need to schedule the save
        # asyncio.create_task(self.save_state()) # This might not run reliably during unload


    # --- Listeners ---
    @commands.Cog.listener()
    async def on_ready(self):
        """Called when the bot is ready."""
        log.info("Events Cog 'on_ready' listener fired.")
        # The check_closures task will be started by its before_loop method
        # after bot.wait_until_ready() completes, which happens around/after on_ready.
        # We could add a log here to confirm the task status after on_ready if needed.
        # log.info(f"check_closures task running after on_ready: {self.check_closures.is_running()}")


    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        log.info(f'Joined new guild: {guild.name} (id: {guild.id})')


    # --- Async API Fetch Helper (using aiohttp) ---
    async def fetch_closures_from_api(self, url):
        """Fetches closure data from API asynchronously using aiohttp."""
        log.debug(f"Fetching closures from API: {url} (async)")
        async with self._api_fetch_lock: # Use a lock to prevent concurrent API fetches
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as response: # Use aiohttp timeout
                        response.raise_for_status() # Raise for 400s/500s
                        log.debug(f"API response status: {response.status}")
                        # Use response.json() directly as it's async
                        latest_closures = await response.json()

                if not isinstance(latest_closures, list):
                    log.error(f"Async API fetch: Expected JSON list, but got {type(latest_closures)}. Treating as empty.")
                    return []

                log.debug(f"Successfully fetched {len(latest_closures)} items from API.")
                self._cached_api_closures = latest_closures # Cache the successful fetch
                self._last_api_fetch_time = datetime.now() # Record fetch time
                return latest_closures

            except aiohttp.ClientTimeout:
                log.error(f"Async API fetch Error: Request timed out fetching {url}")
                # Return cached data if available on timeout
                if self._cached_api_closures:
                     log.info("Returning cached API data due to timeout.")
                     return self._cached_api_closures
                return [] # Return empty list if no cache

            except aiohttp.ClientError as e:
                log.error(f"Async API fetch Error: {e}")
                 # Return cached data if available on other aiohttp errors
                if self._cached_api_closures:
                     log.info("Returning cached API data due to API error.")
                     return self._cached_api_closures
                return [] # Return empty list if no cache

            except json.JSONDecodeError:
                 log.error(f"Async API fetch Error: Could not decode JSON from {url}.")
                 # Return cached data if available on JSON error
                 if self._cached_api_closures:
                     log.info("Returning cached API data due to JSON decode error.")
                     return self._cached_api_closures
                 return [] # Return empty list if no cache

            except Exception as e:
                log.exception(f"Async API fetch Error: Unexpected error fetching {url}")
                 # Return cached data if available on unexpected errors
                if self._cached_api_closures:
                     log.info("Returning cached API data due to unexpected error.")
                     return self._cached_api_closures
                return [] # Return empty list if no cache


    # --- Basic Commands ---
    @commands.command(name='ping')
    async def ping(self, ctx):
        """Check the bot's latency"""
        log.info(f"Command 'ping' invoked by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild} - {ctx.channel}")
        latency = round(self.bot.latency * 1000)
        await ctx.send(f'Pong! Latency: {latency}ms')
        log.info(f"Command 'ping' completed for {ctx.author}")

    @commands.command(name='serverinfo')
    async def server_info(self, ctx):
        """Display information about the server"""
        log.info(f"Command 'serverinfo' invoked by {ctx.author} (ID: {ctx.author.id}) in guild {ctx.guild.id}")
        guild = ctx.guild
        embed = discord.Embed(title=f'{guild.name} Info', color=discord.Color.blue())
        embed.add_field(name='ID', value=guild.id, inline=True)
        embed.add_field(name='Members', value=guild.member_count, inline=True)
        # Use Discord timestamp for creation date
        created_at_ts = int(guild.created_at.timestamp())
        embed.add_field(name='Created', value=f'<t:{created_at_ts}:F>', inline=False)
        # Add icon if available
        if guild.icon:
             embed.set_thumbnail(url=guild.icon.url)

        await ctx.send(embed=embed)
        log.info(f"Command 'serverinfo' completed for {ctx.author}")

    # --- API Road Closure Command ---
    @commands.command(name='roadclosure', aliases=['closures'])
    @commands.guild_only() # Ensure command is used in a guild
    @commands.cooldown(1, 10, commands.BucketType.guild) # Add a cooldown
    async def road_closure(self, ctx, force_fetch: bool = False):
        """
        Displays current road closures reported by the API and managed locally.
        Use `!roadclosure force_fetch` to get the latest data from the API immediately.
        """
        log.info(f"Command 'roadclosure' invoked by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild.name} - {ctx.channel.name}. Force Fetch: {force_fetch}")

        api_url = "https://starbase.nerdpg.live/api/json/roadClosures"

        # Fetch API data or use cache
        closures_from_api = []
        # Check if cache is valid (e.g., not older than 5 minutes or if force_fetch is True)
        cache_is_stale = self._last_api_fetch_time is None or (datetime.now() - self._last_api_fetch_time) > timedelta(minutes=5)
        
        if force_fetch or cache_is_stale or not self._cached_api_closures:
             log.debug("Cache is stale or force_fetch requested. Fetching from API.")
             # Use the async fetch helper
             closures_from_api = await self.fetch_closures_from_api(api_url)
             if not closures_from_api and (cache_is_stale or not self._cached_api_closures):
                  # If fetch failed AND cache was stale/empty, notify user
                  await ctx.send(f"⚠️ Could not fetch fresh data from API. Displaying potentially outdated cached data or only local closures.")
                  closures_from_api = self._cached_api_closures # Fallback to cache if fetch failed but cache exists
             elif closures_from_api and not cache_is_stale and not force_fetch:
                  # This case means fetch_closures_from_api returned cache even though we thought it was stale
                  # It likely means the API fetch failed within the helper. The helper already logs this.
                  pass # No extra message needed here, helper sent warning if it returned cache on error
        else:
             log.debug("Using cached API data.")
             closures_from_api = self._cached_api_closures


        # Combine API closures with locally managed ones that are still in the future
        current_timestamp = int(datetime.now().timestamp())
        active_managed_closures = [
            c for c in self.managed_closures
            if isinstance(c.get('timestamps', {}).get('end'), int) and c['timestamps']['end'] > current_timestamp
        ]

        all_closures = closures_from_api + active_managed_closures

        log.debug(f"Combined {len(closures_from_api)} API closures and {len(active_managed_closures)} active managed closures for display.")


        # Build the embed
        embed = discord.Embed(title="Current Road Closures & Updates", color=discord.Color.blue(), timestamp=discord.utils.utcnow()) # More general title
        embed.set_footer(text=f"API Data Source: Cameron County | Local Data Managed by Bot") # Updated footer

        if not all_closures:
            embed.description = "✅ No active road closures or updates reported."
        else:
            # Sort closures by start time (earliest first)
            try:
                 all_closures.sort(key=lambda x: x.get('timestamps', {}).get('start', 0))
            except Exception as e:
                 log.warning(f"Failed to sort closures for display: {e}. Displaying unsorted.")
                 # Continue without sorting

            closures_by_status = defaultdict(list)
            for closure in all_closures:
                try:
                    # Use .get for safety for all fields
                    status = closure.get('status', 'N/A')
                    start_ts = closure.get('timestamps', {}).get('start')
                    end_ts = closure.get('timestamps', {}).get('end')
                    # closure_type = closure.get('type', 'Update') # Not directly used in the field value line
                    event_date = closure.get('date', 'Date N/A')

                    # Ensure timestamps are valid integers before formatting
                    if start_ts is None or end_ts is None:
                         log.warning(f"Skipping closure entry in command display due to missing timestamp: {closure}")
                         continue
                    try:
                         start_ts_int = int(start_ts)
                         end_ts_int = int(end_ts)
                    except (ValueError, TypeError):
                         log.warning(f"Skipping closure entry in command display due to invalid timestamp format: {closure}")
                         continue

                    # Format time using Discord's timestamps
                    time_msg = f"<t:{start_ts_int}:f> to <t:{end_ts_int}:f>"
                    time_msg += f" (<t:{end_ts_int}:R>)" # Add relative time

                    # Check if it's a managed closure to label the source
                    is_managed = isinstance(closure.get('id'), str) and len(closure['id']) == 36

                    source_label = " (Local)" if is_managed else " (API)"

                    # Combine information for the field value line
                    value_line = f"• **{event_date}{source_label}:** {time_msg}\n"

                    closures_by_status[status].append(value_line)

                except Exception as e:
                    log.exception(f"Error processing closure entry for command display: {closure}", exc_info=True)
                    # Continue processing other entries


            # Add fields to the embed based on status groups
            status_order = ["TFR", "HWY 4 Road Delay", "Closure Scheduled", "Possible Closure", "Closure Revoked", "N/A"] # Add N/A for safety
            added_fields = set() # Track statuses already added to prevent duplicates

            for status in status_order:
                if status in closures_by_status:
                    time_messages = closures_by_status[status]
                    if time_messages: # Only add field if there are entries for this status
                        value_str = "".join(time_messages) # Join all messages for this status

                        # Truncate field value if necessary (max 1024 chars)
                        if len(value_str) > 1024:
                            value_str = value_str[:1020] + "...\n" # Truncate and add indicator
                            log.warning(f"Truncating field value for status '{status}' in roadclosure command embed.")

                        status_emoji = get_status_emoji(status)
                        embed.add_field(name=f"{status_emoji} {status}", value=value_str.strip(), inline=False) # .strip() to remove trailing newline
                        added_fields.add(status)

            # Add any statuses not in the preferred order (handles unexpected statuses from API)
            for status, time_messages in closures_by_status.items():
                 if status not in added_fields and time_messages:
                     value_str = "".join(time_messages)
                     if len(value_str) > 1024:
                          value_str = value_str[:1020] + "...\n"
                          log.warning(f"Truncating field value for unknown status '{status}' in roadclosure command embed.")
                     status_emoji = get_status_emoji(status)
                     embed.add_field(name=f"{status_emoji} {status}", value=value_str.strip(), inline=False) # .strip()

            # Check embed limits before sending (Discord max 6000 total chars, 25 fields)
            if len(embed) > 5800: # Use a slightly lower threshold than 6000 for safety
                 log.warning(f"Roadclosure command embed length ({len(embed)}) > 5800, may not send.")
                 # Discord will truncate the embed fields if it's too long

            if len(embed.fields) > 25:
                 log.warning(f"Roadclosure command embed fields ({len(embed.fields)}) > 25. Display might be incomplete.")
                 # Discord automatically truncates fields beyond 25. User sees the first 25.
                 await ctx.send("Warning: Too many closure categories to display. Some may not appear.")


        try:
             await ctx.send(embed=embed)
             log.info(f"Command 'roadclosure' embed sent successfully for {ctx.author}.")
        except discord.Forbidden:
             log.error(f"Missing permissions to send embed in channel {ctx.channel.id} ({ctx.channel.name}) for roadclosure command.")
             await ctx.send("❌ Error: I do not have permissions to send embeds in this channel.")
        except discord.HTTPException as e:
             log.error(f"HTTP error sending embed for roadclosure command in channel {ctx.channel.id}: {e.status} {e.text}", exc_info=True)
             await ctx.send("❌ Error: Failed to send the embed due to a Discord API error.")
        except Exception as e:
             log.exception(f"Unexpected error sending embed for roadclosure command in channel {ctx.channel.id}")
             await ctx.send("❌ An unexpected error occurred after preparing the closure embed.")


    # --- Monitoring Management Commands ---
    # (Permissions check remains the same - assuming allowed_roles are set)
    def check_permissions(self, ctx):
        """Checks if user has allowed roles or is guild owner."""
        log.debug(f"Checking permissions for {ctx.author} in {ctx.command.name}")
        # Ensure ctx.author is a Member when used in a guild
        if not isinstance(ctx.author, discord.Member) or not hasattr(ctx.author, 'roles'):
             log.warning(f"Permission check failed for non-guild user {ctx.author} in {ctx.command.name}")
             return False
        # Check if the command is used in a guild context if it's guild_only
        if hasattr(ctx.command, 'guild_only') and ctx.command.guild_only and ctx.guild is None:
             log.warning(f"Permission check failed for guild_only command {ctx.command.name} outside a guild by {ctx.author}")
             return False # Should be caught by @commands.guild_only already, but extra safety

        if ctx.guild.owner_id == ctx.author.id:
             log.debug(f"Permission granted for owner {ctx.author}")
             return True

        author_roles = {role.name.lower() for role in ctx.author.roles}
        allowed_roles_lower = {role.lower() for role in self.allowed_roles}
        has_perm = not allowed_roles_lower.isdisjoint(author_roles)
        log.debug(f"Permission check for {ctx.author} in {ctx.command.name}: Has required role/owner? {has_perm}")
        return has_perm

    @commands.command(name='monitorclosures')
    @commands.guild_only()
    async def monitor_closures(self, ctx, channel: discord.TextChannel = None):
        """(Mod Only) Adds a channel for API road closure monitoring notices."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'monitorclosures' invoked by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild.name} - {ctx.channel.name}")
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels:
            log.warning(f"Attempt to monitor already monitored channel {target_channel.id} ({target_channel.name}) by {ctx.author}")
            await ctx.send(f"⚠️ {target_channel.mention} is already monitored."); return

        # Check if the bot has permissions to send messages and embeds in the target channel
        if not isinstance(target_channel, discord.TextChannel):
             log.warning(f"Monitor target is not a text channel: {target_channel.id}")
             await ctx.send("❌ Can only monitor text channels.")
             return

        


        self.monitoring_channels.add(target_channel.id)
        # Use await for async save_state
        await self.save_state()
        await ctx.send(f"✅ Will send API closure updates to {target_channel.mention}.")
        log.info(f"Added channel {target_channel.id} ({target_channel.name}) to monitoring by {ctx.author}. Current monitored count: {len(self.monitoring_channels)}")


    @commands.command(name='unmonitorclosures')
    @commands.guild_only()
    async def unmonitor_closures(self, ctx, channel: discord.TextChannel = None):
        """(Mod Only) Removes a channel from API closure monitoring."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'unmonitorclosures' invoked by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild.name} - {ctx.channel.name}")
        target_channel = channel or ctx.channel
        if target_channel.id in self.monitoring_channels:
            self.monitoring_channels.discard(target_channel.id)
            # Use await for async save_state
            await self.save_state()
            await ctx.send(f"✅ Stopped sending API closure updates to {target_channel.mention}.")
            log.info(f"Removed channel {target_channel.id} ({target_channel.name}) from monitoring by {ctx.author}. Current monitored count: {len(self.monitoring_channels)}")
        else:
            log.warning(f"Attempt to unmonitor non-monitored channel {target_channel.id} ({target_channel.name}) by {ctx.author}")
            await ctx.send(f"⚠️ {target_channel.mention} is not monitored.")

    @commands.command(name='listmonitored')
    @commands.guild_only()
    async def list_monitored(self, ctx):
        """(Mod Only) Lists channels monitored for API closures."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'listmonitored' invoked by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild.name} - {ctx.channel.name}")

        if not self.monitoring_channels:
            await ctx.send("ℹ️ No channels monitored for API closures.");
            log.info("'listmonitored': No channels monitored.")
            return

        description_lines = []
        channels_to_remove = set() # Collect IDs of channels that are no longer valid

        # Iterate over a copy in case we modify the original set
        for channel_id in list(self.monitoring_channels):
             ch = self.bot.get_channel(channel_id)
             if ch:
                  # Check if the channel belongs to the current guild context (optional but good)
                  if ch.guild == ctx.guild:
                       description_lines.append(f"- {ch.mention} (`{channel_id}`)")
                  # else:
                       # Channel exists but is in a different guild.
                       # We keep it in the monitoring list as it's valid for that guild.
                       # log.debug(f"Channel {channel_id} found but in different guild {ch.guild.id}")
             else:
                 # Channel object not found means it's deleted or bot left the guild
                 channels_to_remove.add(channel_id)
                 description_lines.append(f"- *Unknown/Deleted Channel* (`{channel_id}`)")
                 log.warning(f"Channel ID {channel_id} in monitoring list not found by bot. Marked for removal.")

        # Remove invalid channels after the loop
        if channels_to_remove:
             self.monitoring_channels -= channels_to_remove
             # Use await for async save_state
             await self.save_state()
             log.info(f"Removed {len(channels_to_remove)} unknown/deleted channels from monitoring list.")

        # If all monitored channels were in other guilds or removed
        if not description_lines and self.monitoring_channels:
             # There are still channels monitored, but none were in this guild or found
             description_lines = ["None listed for this server (channels exist but may be in other servers or inaccessible)."]
             log.info("'listmonitored': Monitored channels exist, but none found/listed for this server.")
        elif not description_lines and not self.monitoring_channels:
             # If the set is now empty after removing dead channels
              description_lines = ["No channels currently monitored for API closures."]
              log.info("'listmonitored': Monitoring list is empty.")


        embed = discord.Embed(
            title="API Closure Monitoring Channels",
            description="\n".join(description_lines),
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)
        log.info(f"Command 'listmonitored' completed for {ctx.author}")


    # --- Commands for Managing Local/Managed Closures ---
    # (Permissions check remains the same)

    @commands.command(name='removeclosure', aliases=['removemyclosure', 'rmc'])
    @commands.guild_only()
    async def remove_managed_road_closure(self, ctx):
        """(Mod Only) Removes a road closure managed locally by the bot."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'removeclosure' invoked by {ctx.author} (ID: {ctx.author.id})")

        if not self.managed_closures:
             await ctx.send("ℹ️ No local closures to remove.")
             log.info("'removeclosure': No managed closures to remove.")
             return

        # Display list of managed closures to the user for selection
        list_embed = discord.Embed(title="Select Local Closure to Remove", color=discord.Color.orange())
        list_embed.description = "Reply with the **full ID** of the closure you want to remove.\nType `cancel` to abort.\n\n**Local Closures:**"

        # Sort closures by start time (earliest first) for display
        try:
            managed_closures_sorted = sorted(self.managed_closures, key=lambda x: x.get('timestamps', {}).get('start', 0))
        except Exception as e:
            log.warning(f"Failed to sort managed closures for display: {e}. Displaying unsorted.")
            managed_closures_sorted = self.managed_closures # Fallback to unsorted


        if not managed_closures_sorted:
             await ctx.send("ℹ️ No local closures found.")
             log.info("'removeclosure': No local closures found to display after potential sorting issue.")
             return


        for closure in managed_closures_sorted:
             closure_id = closure.get('id', 'ID N/A')
             status = closure.get('status', 'Status N/A')
             date = closure.get('date', 'Date N/A')
             closure_type = closure.get('type', 'Type N/A')

             # Get timestamps for formatting if available
             start_ts = closure.get('timestamps', {}).get('start')
             end_ts = closure.get('timestamps', {}).get('end')

             time_info = ""
             if start_ts is not None and end_ts is not None:
                  try:
                      start_ts_int = int(start_ts)
                      end_ts_int = int(end_ts)
                      # Display local time using Discord timestamp formatting
                      time_info = f"\n**Time:** <t:{start_ts_int}:f> to <t:{end_ts_int}:f> (<t:{end_ts_int}:R>)"
                  except (ValueError, TypeError):
                      time_info = "\n**Time:** Invalid Timestamps"
             elif closure.get('time'):
                  # Fallback to raw time string if timestamps are bad
                  time_info = f"\n**Time:** {closure['time']} (Raw String)"

             # Include Notes if available
             notes = closure.get('notes')
             notes_info = f"\n**Notes:** {notes}" if notes else ""


             list_embed.add_field(
                  name=f"ID: `{closure_id}`", # Display full ID for copy-paste
                  value=f"**Status:** {status}\n**Date:** {date}\n**Type:** {closure_type}{time_info}{notes_info}",
                  inline=False
             )

        # Check embed length limits for the list display itself
        if len(list_embed) > 5800:
            log.warning(f"Remove closure list embed length ({len(list_embed)}) > 5800. May be truncated.")
            # Discord will truncate the embed fields if it's too long

        await ctx.send(embed=list_embed)
        await ctx.send("Reply with the **full ID** of the closure to remove.\nType `cancel` to abort.")


        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        timeout_duration = 60.0 # Shorter timeout for interactive commands

        try:
             msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
             response_content = msg.content.strip()
             log.debug(f"'removeclosure': Received response: '{response_content[:50]}...'")
        except asyncio.TimeoutError:
             log.warning(f"'removeclosure' timed out waiting for ID from {ctx.author}")
             await ctx.send("⏰ Timed out. Aborting removal.")
             return

        if response_content.lower() == 'cancel':
             log.info(f"'removeclosure' cancelled by {ctx.author}")
             await ctx.send("❌ Removal cancelled.")
             return

        closure_id_to_remove = response_content

        # Find the closure by ID and remove it
        original_count = len(self.managed_closures)
        # Filter the list, keeping only closures whose ID does *not* match the one to remove
        self.managed_closures = [c for c in self.managed_closures if c.get('id') != closure_id_to_remove]
        new_count = len(self.managed_closures)

        if new_count < original_count:
            # Removal successful
            # **FIX**: Do NOT remove from seen_closure_ids here.
            # seen_closure_ids is ONLY for tracking API events that have been notified.
            # Locally managed closures do not go into seen_closure_ids.

            try:
                 # Use await for async save_state
                 await self.save_state()
                 await ctx.send(f"✅ Locally managed closure with ID `{closure_id_to_remove}` removed.")
                 log.info(f"Removed managed closure ID {closure_id_to_remove} by {ctx.author}.")
            except Exception as e:
                 log.exception(f"Error saving state after removing managed closure by {ctx.author} (ID: {closure_id_to_remove})")
                 await ctx.send("❌ Error saving bot state after removing closure. Closure might not be fully removed from persistence.")

        else:
            # ID not found
            log.warning(f"Attempt to remove non-existent managed closure ID '{closure_id_to_remove}' by {ctx.author}")
            await ctx.send(f"⚠️ Locally managed closure with ID `{closure_id_to_remove}` not found.")

        log.info(f"Command 'removeclosure' completed for {ctx.author}.")


    @commands.command(name='listroadclosures', aliases=['listmyclosures', 'lmc'])
    @commands.guild_only()
    async def list_managed_road_closures(self, ctx):
        """(Mod Only) Lists road closures managed locally by the bot."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'listroadclosures' invoked by {ctx.author} (ID: {ctx.author.id})")

        if not self.managed_closures:
            await ctx.send("ℹ️ There are no road closures currently managed by the bot.")
            log.info("'listroadclosures': No managed closures found.")
            return

        log.info(f"'listroadclosures': Found {len(self.managed_closures)} managed closures.")
        output_lines = ["**Locally Managed Road Closures:**"]

        # Sort closures by start time (earliest first) for display
        try:
            managed_closures_sorted = sorted(self.managed_closures, key=lambda x: x.get('timestamps', {}).get('start', 0))
        except Exception as e:
            log.warning(f"Failed to sort managed closures for display: {e}. Displaying unsorted.")
            managed_closures_sorted = self.managed_closures # Fallback to unsorted


        for i, closure in enumerate(managed_closures_sorted):
            # Use .get for safety
            closure_id = closure.get('id', 'ID N/A')
            status = closure.get('status', 'Status N/A')
            date = closure.get('date', 'Date N/A')
            closure_type = closure.get('type', 'Type N/A')
            notes = closure.get('notes') # Get notes field

            # Get timestamps for formatting if available
            start_ts = closure.get('timestamps', {}).get('start')
            end_ts = closure.get('timestamps', {}).get('end')

            time_info = ""
            if start_ts is not None and end_ts is not None:
                 try:
                     start_ts_int = int(start_ts)
                     end_ts_int = int(end_ts)
                     # Display local time using Discord timestamp formatting
                     time_info = f"\n   **Time:** <t:{start_ts_int}:f> to <t:{end_ts_int}:f> (<t:{end_ts_int}:R>)"
                 except (ValueError, TypeError):
                     time_info = "\n   **Time:** Invalid Timestamps"
            elif closure.get('time'):
                 # Fallback to raw time string if timestamps are bad
                 time_info = f"\n   **Time:** {closure['time']} (Raw String)"

            notes_info = f"\n   **Notes:** {notes}" if notes else ""

            # Add fields for display - use .get for safety on original closure dict if needed
            output_lines.append(
                f"`{i+1}`. **ID:** `{closure_id[:8]}`...\n" # Truncate ID for list view
                f"   - **Status:** {status}\n"
                f"   - **Date:** {date}\n"
                f"   - **Type:** {closure_type}"
                f"{time_info}" # Add time info
                f"{notes_info}" # Add notes info
            )

        # Send output in chunks if it's too long for one message
        current_message = ""
        for line in output_lines:
            # Check message length (max 2000 chars)
            if len(current_message) + len(line) + 2 > 2000: # +2 for newline and potential list item start
                await ctx.send(current_message)
                current_message = line + "\n"
            else:
                current_message += line + "\n"

        # Send any remaining content
        if current_message:
            await ctx.send(current_message)

        log.info(f"Command 'listroadclosures' completed for {ctx.author}")


    @commands.command(name='addroadclosure', aliases=['addmyclosure', 'amc'])
    @commands.guild_only()
    async def add_managed_road_closure(self, ctx):
        """(Mod Only) Interactively adds a new road closure to the bot's local list."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'addroadclosure' invoked by {ctx.author} (ID: {ctx.author.id})")

        new_closure_input = {}
        cancelled = False
        timeout_duration = 120.0 # Shorter timeout for interactive commands


        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        # Prompts matching the keys in the expected API/managed data structure
        prompts = {
             "status": "📝 **Status?** (e.g., Closure Scheduled, HWY 4 Road Delay)\n*Reply 'cancel' to abort.*",
             "date": "📅 **Date(s)?** (e.g., May 12, 2025)\n*Reply 'cancel' to abort.*",
             "time": "⏰ **Time range string?** (e.g., 10:00 a.m. to 4:00 p.m.)\n*Reply 'cancel' to abort.*",
             "type": "🏷️ **Event Type?** (e.g., Primary Date, Backup Date, Test)\n*Reply 'cancel' to abort.*",
             "start_timestamp": "▶️ **Start Unix Timestamp?** (Integer. Get from https://www.unixtimestamp.com/)\n*Reply 'cancel' to abort.*",
             "end_timestamp": "⏹️ **End Unix Timestamp?** (Integer. Get from https://www.unixtimestamp.com/)\n*Reply 'cancel' to abort.*",
             "notes": "📄 **Optional Notes?** (Type `none` if empty, or `cancel` to abort.)"
        }
        await ctx.send(f"Adding a new **locally managed** closure interactively.") # Initial instruction


        for key, prompt in prompts.items():
            log.debug(f"'addroadclosure': Prompting for '{key}'")
            # Send the prompt message
            prompt_message = await ctx.send(prompt)

            try:
                # Use a separate timeout for each step
                msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                log.debug(f"'addroadclosure': Received response for '{key}': '{msg.content[:50]}...'") # Log truncated message

            except asyncio.TimeoutError:
                 log.warning(f"'addroadclosure' timed out waiting for '{key}' from {ctx.author}")
                 await ctx.send(f"⏰ Timed out waiting for response for '{key}'. Aborting.")
                 cancelled = True # Mark as cancelled due to timeout
                 break # Exit the prompt loop

            content_lower = msg.content.lower().strip()
            if content_lower == 'cancel':
                cancelled = True
                log.info(f"'addroadclosure' cancelled by {ctx.author} at prompt for '{key}'")
                break # Exit the prompt loop

            value = msg.content.strip() # Use the original stripped value

            # Special handling for timestamp fields
            if key in ["start_timestamp", "end_timestamp"]:
                if not value.isdigit():
                    log.warning(f"'addroadclosure': Invalid timestamp input '{value}' for '{key}' by {ctx.author}")
                    await ctx.send(f"❌ Invalid input for {key}. Expected an integer Unix timestamp. Aborting.")
                    cancelled = True # Mark as cancelled due to invalid input
                    break # Exit the prompt loop
                new_closure_input[key] = int(value)
            # Special handling for optional notes
            elif key == "notes" and value.lower() == 'none':
                 new_closure_input[key] = None
            else:
                 new_closure_input[key] = value

        # After the loop, if not cancelled, process and save
        if cancelled:
             await ctx.send("❌ Addition cancelled.")
             return

        # Basic validation - ensure timestamps are present and valid integers after interaction
        # This is a final check after the loop
        start_ts_final = new_closure_input.get('start_timestamp')
        end_ts_final = new_closure_input.get('end_timestamp')

        if not (isinstance(start_ts_final, int) and isinstance(end_ts_final, int)):
            log.error(f"'addroadclosure': Missing or invalid timestamps after interaction for {ctx.author}. Start: {start_ts_final}, End: {end_ts_final}")
            await ctx.send("❌ Error: Missing or invalid timestamps provided. Closure not added.")
            return


        # Construct the closure data dictionary, matching the expected structure
        closure_data = {
             "id": str(uuid.uuid4()), # Assign a unique ID for local management
             "date": new_closure_input.get("date", "Date N/A"),
             "status": new_closure_input.get("status", "Status N/A"),
             "time": new_closure_input.get("time", "Time N/A"),
             "timestamps": {
                  "start": start_ts_final,
                  "end": end_ts_final
             },
             "type": new_closure_input.get("type", "Type N/A"),
             "notes": new_closure_input.get("notes")
         }


        log.info(f"'addroadclosure': Preparing to add locally managed closure: {closure_data}")

        self.managed_closures.append(closure_data)

        try:
             # Use await for async save_state
             await self.save_state()
             await ctx.send(f"✅ **Locally managed** closure added!\n**ID:** `{closure_data['id']}`")
             log.info(f"Command 'addroadclosure' completed by {ctx.author}, new ID: {closure_data['id']}")
        except Exception as e: # Catching exceptions during save_state as well
             log.exception(f"Error saving state after adding managed closure by {ctx.author}")
             await ctx.send("❌ Error saving bot state after adding closure. Closure might not be saved.")


    @commands.command(name='editroadclosure', aliases=['editmyclosure', 'emc'])
    @commands.guild_only()
    async def edit_managed_road_closure(self, ctx, closure_id: str = None):
        """(Mod Only) Interactively edits a locally managed closure by its ID."""

        if not self.check_permissions(ctx):
            log.warning(f"Permission denied for {ctx.author} to use {ctx.command.name}")
            await ctx.send("❌ You do not have permission to use this command.")
            return
        log.info(f"Command 'editroadclosure' invoked by {ctx.author} (ID: {ctx.author.id}) for closure ID: {closure_id}")

        if not self.managed_closures:
             await ctx.send("ℹ️ No local closures to edit.")
             log.info("'editroadclosure': No managed closures to edit.")
             return

        # If no ID is provided, list closures and prompt for ID
        if closure_id is None:
             await self.list_managed_road_closures(ctx) # Use the existing list command
             await ctx.send("Reply with the **full ID** of the closure you want to edit (copy from the list above).\nType `cancel` to abort.")

             def check(m): return m.author == ctx.author and m.channel == ctx.channel
             timeout_duration = 60.0
             try:
                  msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                  response_content = msg.content.strip()
                  log.debug(f"'editroadclosure': Received ID response: '{response_content[:50]}...'")
             except asyncio.TimeoutError:
                  log.warning(f"'editroadclosure' timed out waiting for ID from {ctx.author}")
                  await ctx.send("⏰ Timed out. Aborting edit.")
                  return

             if response_content.lower() == 'cancel':
                  log.info(f"'editroadclosure' cancelled by {ctx.author}")
                  await ctx.send("❌ Edit cancelled.")
                  return
             closure_id = response_content # Use the ID provided by the user

        # Find the closure by ID
        target_closure = None
        target_index = -1

        # Iterate with index to know where to update later
        for i, closure in enumerate(self.managed_closures):
             if closure.get('id') == closure_id:
                  # Create a deep copy to edit, leaving original in list until save
                  target_closure = copy.deepcopy(closure)
                  target_index = i
                  break

        if target_closure is None:
             log.warning(f"'editroadclosure': Closure ID '{closure_id}' not found by {ctx.author}")
             await ctx.send(f"❌ Locally managed closure ID `{closure_id}` not found.")
             return

        # Display current closure details in an embed
        embed = discord.Embed(title=f"Editing Managed Closure ID: {closure_id}", color=discord.Color.orange())
        embed.description = "Current values. Reply with new value, `skip`, or `cancel`."
        # List fields to display (matching the structure)
        fields_to_display = ["status", "date", "time", "type", "timestamps", "notes"]
        for key in fields_to_display:
             value = target_closure.get(key)
             if key == "timestamps":
                  start = value.get('start') if isinstance(value, dict) else 'N/A'
                  end = value.get('end') if isinstance(value, dict) else 'N/A'
                  val_str = f"Start: `{start}`\nEnd: `{end}`"
             elif value is None:
                  val_str = "*None*"
             elif isinstance(value, (dict, list)):
                  # Use json.dumps for complex structures if needed, or just repr
                  val_str = f"```json\n{json.dumps(value, indent=2, ensure_ascii=False)[:500]}...```" # Truncate
             else:
                  val_str = f"`{value}`" # Display simple values in code block

             embed.add_field(name=key.capitalize(), value=val_str, inline=False)


        await ctx.send(embed=embed)


        edited_closure = target_closure # Start editing the copy
        cancelled = False
        timeout_duration = 60.0 # Shorter timeout for interactive commands

        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        # Fields that are editable interactively
        editable_fields = ["status", "date", "time", "type", "notes"]
        # Handle timestamps separately as they are nested
        editable_timestamps = ["start", "end"]


        for key in editable_fields:
            # Get current value for display in prompt
            current_value = edited_closure.get(key)
            prompt = f"✏️ Edit **'{key}'** (Current: `{current_value if current_value is not None else 'None'}`)? Enter new value, `skip`, or `cancel`:" # Add 'None' display

            log.debug(f"'editroadclosure': Prompting for '{key}' (ID: {closure_id})")
            await ctx.send(prompt)

            try:
                 msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                 log.debug(f"'editroadclosure': Received response for '{key}': '{msg.content[:50]}...'")
            except asyncio.TimeoutError:
                 log.warning(f"'editroadclosure' timed out waiting for '{key}' from {ctx.author} (ID: {closure_id})")
                 await ctx.send(f"⏰ Timed out editing '{key}'. Aborting edit.")
                 return # Exit command handler

            content_lower = msg.content.lower().strip()
            if content_lower == 'cancel':
                 cancelled = True
                 log.info(f"'editroadclosure' cancelled by {ctx.author} at prompt for '{key}' (ID: {closure_id})")
                 break # Exit the prompt loop
            if content_lower == 'skip':
                 log.debug(f"'editroadclosure': Skipped editing '{key}' for ID {closure_id}")
                 continue # Skip to next field

            value = msg.content.strip()
            if key == "notes" and value.lower() == 'none':
                 edited_closure[key] = None
            else:
                 edited_closure[key] = value # Update the value in the edited_closure dict

            log.debug(f"Updated '{key}' to '{value}' for ID {closure_id}")


        # Handle editing timestamps separately
        if not cancelled:
            # Ensure timestamps dict exists and is a dict
            if "timestamps" not in edited_closure or not isinstance(edited_closure["timestamps"], dict):
                 log.warning(f"Timestamps key missing or invalid in edited_closure for ID {closure_id}. Re-initializing.")
                 edited_closure["timestamps"] = {}

            for ts_key in editable_timestamps:
                 current_ts = edited_closure["timestamps"].get(ts_key)
                 prompt = f"⏱️ Edit **'{ts_key} timestamp'** (Current: `{current_ts}`)? Enter new Unix timestamp, `skip`, or `cancel`:"
                 log.debug(f"'editroadclosure': Prompting for '{ts_key}_timestamp' (ID: {closure_id})")
                 await ctx.send(prompt)

                 try:
                     msg = await self.bot.wait_for('message', check=check, timeout=timeout_duration)
                     log.debug(f"'editroadclosure': Received response for '{ts_key}_timestamp': '{msg.content[:50]}...'")
                 except asyncio.TimeoutError:
                      log.warning(f"'editroadclosure' timed out waiting for '{ts_key}_timestamp' from {ctx.author} (ID: {closure_id})")
                      await ctx.send(f"⏰ Timed out editing '{ts_key} timestamp'. Aborting edit.")
                      return

                 content_lower = msg.content.lower().strip()
                 if content_lower == 'cancel':
                      cancelled = True
                      log.info(f"'editroadclosure' cancelled by {ctx.author} at prompt for '{ts_key}_timestamp' (ID: {closure_id})")
                      break # Exit timestamp loop
                 if content_lower == 'skip':
                      log.debug(f"'editroadclosure': Skipped editing '{ts_key}_timestamp'")
                      continue # Skip to next timestamp

                 value = msg.content.strip()
                 if not value.isdigit():
                     log.warning(f"'editroadclosure': Invalid timestamp input '{value}' for '{ts_key}_timestamp' by {ctx.author} (ID: {closure_id})")
                     await ctx.send(f"❌ Invalid input for '{ts_key} timestamp'. Keeping `{current_ts}`.")
                     # Continue the loop, letting the user try the other timestamp or finish
                     continue

                 # Update the timestamp value
                 edited_closure["timestamps"][ts_key] = int(value)
                 log.debug(f"Updated '{ts_key}_timestamp' to {value} for ID {closure_id}")


        if cancelled:
             await ctx.send("❌ Edit cancelled.")
             return

        log.info(f"'editroadclosure': Finished interactive editing for ID {closure_id}. Attempting to save state.")

        # Replace the old closure data with the edited version in the managed_closures list
        if 0 <= target_index < len(self.managed_closures): # Double-check index validity
             self.managed_closures[target_index] = edited_closure
             try:
                 # Use await for async save_state
                 await self.save_state()
                 await ctx.send(f"✅ Locally managed closure `{closure_id}` updated!")
                 log.info(f"Command 'editroadclosure' completed by {ctx.author}. Managed closure ID {closure_id} updated.")

                 # Display the updated closure details
                 updated_embed = discord.Embed(title=f"Updated Managed Closure ID: {closure_id}", color=discord.Color.green())
                 updated_embed.description = "New values:"
                 for key in fields_to_display: # Use the same fields for display
                     value = edited_closure.get(key)
                     if key == "timestamps":
                          # Handle cases where timestamps might be missing or not a dict after edit
                          if isinstance(value, dict):
                              start = value.get('start')
                              end = value.get('end')
                              val_str = f"Start: `{start}`\nEnd: `{end}`"
                          else:
                              val_str = "*Invalid Timestamps Structure*"

                     elif value is None:
                          val_str = "*None*"
                     elif isinstance(value, (dict, list)):
                          val_str = f"```json\n{json.dumps(value, indent=2, ensure_ascii=False)[:500]}...```" # Truncate complex types
                     else:
                          val_str = f"`{value}`" # Display simple values

                     updated_embed.add_field(name=key.capitalize(), value=val_str, inline=False)

                 await ctx.send(embed=updated_embed)

             except Exception as e:
                  log.exception(f"Error saving state after editing managed closure by {ctx.author} (ID: {closure_id})")
                  await ctx.send("❌ Error saving bot state after editing closure. Changes might not be saved.")
        else:
             log.error(f"Internal Error: Target index {target_index} out of bounds for managed_closures list during edit save for ID {closure_id}.")
             await ctx.send("❌ Internal error when saving changes.")


    # --- Background Task (API Checking) ---
    # Removed the duplicate @tasks.loop decorator
    @tasks.loop(seconds=30)
    async def check_closures(self):
        """Background task to check external API for new road closures."""
        # Ensure there are channels to monitor
        if not hasattr(self, 'monitoring_channels') or not self.monitoring_channels:
            log.debug("Task 'check_closures': No monitoring channels set. Skipping.")
            return

        api_url = "https://starbase.nerdpg.live/api/json/roadClosures"

        # Use the async fetch helper - it handles caching and errors
        latest_closures = await self.fetch_closures_from_api(api_url)

        if not latest_closures:
             log.debug("Task 'check_closures': No closures returned from API (fetch failed or API empty). Skipping notification check.")
        
             return # No new closures if the list is empty


        # Generate UUIDs for the latest closures fetched
        latest_generated_ids = set()
        closures_by_generated_id = {} # Store closures keyed by their generated ID
        new_closures_to_notify = [] # List to hold (uuid, closure_data) tuples for new items

        log.debug(f"Task 'check_closures': Processing {len(latest_closures)} items from API for new notifications...")
        for cl in latest_closures:
            try:
                # --- UUID generation logic ---
                # Use essential, expected fields from the API example for robust ID generation
                timestamps = cl.get('timestamps')
                # Ensure timestamps are valid and convertible before using them in UUID
                if not timestamps or not isinstance(timestamps, dict):
                     log.warning(f"Task 'check_closures': Skipping API item due to missing or invalid 'timestamps': {cl}")
                     continue

                start_ts = timestamps.get('start')
                end_ts = timestamps.get('end')
                status = cl.get('status', '?')
                type_val = cl.get('type', '?')
                api_date = cl.get('date', '?')
                api_time_str = cl.get('time', '?')

                if start_ts is None or end_ts is None:
                     log.warning(f"Task 'check_closures': Skipping API item due to missing start/end timestamp under 'timestamps': {cl}")
                     continue

                try:
                     start_ts_int = int(start_ts)
                     end_ts_int = int(end_ts)
                except (ValueError, TypeError):
                     log.warning(f"Task 'check_closures': Skipping API item due to invalid timestamp format: {cl}")
                     continue

                # Construct a string using key identifying parts of the closure event
                name_str = f"{api_date}|{status}|{api_time_str}|{start_ts_int}|{end_ts_int}|{type_val}"

                # Generate a deterministic UUID based on the namespace and the event string
                generated_uuid = str(uuid.uuid5(self.CLOSURE_NAMESPACE, name_str))
                # --- End UUID generation logic ---


                latest_generated_ids.add(generated_uuid)
                # Store the original closure data keyed by the generated UUID
                closures_by_generated_id[generated_uuid] = cl

                # Check if this generated UUID is new to our seen list
                if generated_uuid not in self.seen_closure_ids:
                     # Add the original closure data along with the generated UUID
                     new_closures_to_notify.append((generated_uuid, cl))


            except Exception as e: # Catch any exception during item processing
                 log.exception(f"Task 'check_closures': Unexpected error processing API item for ID generation: {cl}")


        log.info(f"Task 'check_closures': Identified {len(new_closures_to_notify)} new closures for notification.")

        if new_closures_to_notify:
            processed_successfully_ids = set() # Track IDs successfully notified to at least one channel
            log.info(f"Task 'check_closures': Processing notifications for new closures...")

            # --- Notification Loop ---
            # Iterate through the identified new closures
            for generated_uuid, closure in new_closures_to_notify:
                log.debug(f"Task 'check_closures': Preparing notification for GenUUID {generated_uuid}")

                try:
                    # Extract data for the embed using .get() with fallbacks based on the API example
                    status = closure.get('status', 'N/A')
                    type_val = closure.get('type', 'Update')
                    api_date = closure.get('date', 'N/A')
                    # api_time_str = closure.get('time', 'Time N/A') # Not directly used in embed value

                    # Get timestamps - guaranteed to exist here because UUID was validated for generation
                    start_ts = int(closure['timestamps']['start'])
                    end_ts = int(closure['timestamps']['end'])

                    # Get emoji and color based on status
                    status_emoji = get_status_emoji(status)
                    embed_color = get_status_color(status)

                    # --- Enhanced Embed Creation (Based *exactly* on provided API data fields) ---
                    embed = discord.Embed(
                        title=f"{status_emoji} New Road Update: {status}", # Title using emoji and status
                        description=f"A new road access update has been reported.", # Simple description
                        color=embed_color,
                        timestamp=discord.utils.utcnow() # Embed creation time (UTC)
                    )

                    # Add fields for key information based on API structure
                    embed.add_field(name="Status", value=status, inline=True)
                    embed.add_field(name="Event Type", value=type_val, inline=True) # e.g., Primary Date

                    # Using Discord's built-in timestamp markdown for Time Period
                    # This displays time in the user's local timezone!
                    # F = Full date/time (e.g., Monday, January 1, 2024 7:00 AM)
                    # R = Relative time (e.g., 2 hours ago)
                    time_period_formatted = f"<t:{start_ts}:F> to <t:{end_ts}:F>"
                    time_period_formatted += f" (<t:{end_ts}:R>)" # Add relative time

                    embed.add_field(name=f"Time Period ({api_date})", value=time_period_formatted, inline=False) # Include date in field name

                    # Add a footer for the source and a truncated Event ID
                    embed.set_footer(text=f"Source: nerdpg.live API | Event ID: {generated_uuid[:8]}")


                    # Send the embed to all monitoring channels
                    send_success_count = 0
                    channels_to_keep = [] # Build a new list of channel IDs to keep

                    # Iterate over a copy in case we modify the original set
                    for channel_id in list(self.monitoring_channels):
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            # Check bot's permissions in the channel before sending
                            perms = channel.guild.me.permissions_in(channel)
                            if not perms.send_messages or not perms.embed_links:
                                log.warning(f"Task 'check_closures': Missing send_messages or embed_links permissions in channel {channel.guild.name} - {channel.name} (ID: {channel_id}). Cannot send update.")
                                channels_to_keep.append(channel_id) # Keep channel, permissions might be temporary
                                continue # Skip sending to this channel

                            try:
                                await channel.send(embed=embed)
                                log.debug(f"Task 'check_closures': Sent notification for GenUUID {generated_uuid} to channel {channel_id} ({channel.guild.name} - {channel.name}).")
                                send_success_count += 1
                                channels_to_keep.append(channel_id) # Keep the channel

                            except discord.Forbidden:
                                log.error(f"Task 'check_closures': Discord Forbidden error sending to channel {channel_id}. Removing from monitoring list.")
                                # Channel is not appended to channels_to_keep, effectively removing it
                            except discord.NotFound:
                                log.warning(f"Task 'check_closures': Channel not found: {channel_id}. It might have been deleted. Removing from monitoring list.")
                                # Channel is not appended to channels_to_keep
                            except discord.HTTPException as e:
                                log.error(f"Task 'check_closures': HTTP error sending to channel {channel_id}: {e.status} {e.text}", exc_info=True)
                                channels_to_keep.append(channel_id) # Keep channel for temporary error
                            except Exception as e:
                                log.exception(f"Task 'check_closures': Unexpected error sending GenUUID {generated_uuid} to channel {channel_id}")
                                channels_to_keep.append(channel_id) # Keep channel for unexpected error

                        else:
                            log.warning(f"Task 'check_closures': Channel object not found for ID: {channel_id} during notification loop. Removing from monitoring list.")
                            # Channel is not appended to channels_to_keep


                    # After trying to send to all channels for this UUID, update monitoring_channels set
                    channels_removed_in_this_loop = self.monitoring_channels - set(channels_to_keep)
                    if channels_removed_in_this_loop:
                        log.info(f"Task 'check_closures': Removing {len(channels_removed_in_this_loop)} channels from monitoring after send attempts for UUID {generated_uuid}.")
                        self.monitoring_channels = set(channels_to_keep) # Update the set
                        # Save state ONLY AFTER updating the monitoring list based on all sends for this UUID
                        try:
                            # Use await for async save_state
                            await self.save_state()
                        except Exception as e:
                            log.exception(f"Task 'check_closures': Error saving state after removing channels for UUID {generated_uuid}")


                    # Only mark UUID as processed if it was successfully sent to AT LEAST ONE channel
                    if send_success_count > 0:
                        processed_successfully_ids.add(generated_uuid)
                        log.info(f"Task 'check_closures': Notification attempt finished for GenUUID {generated_uuid}. Successfully sent to {send_success_count} channels.")
                    else:
                        # If it failed to send to all channels (due to permission, http error, not found, etc),
                        # DO NOT mark it as seen. It will be attempted again next cycle.
                        log.warning(f"Task 'check_closures': Notification failed to send to any channels for GenUUID {generated_uuid}. Will retry next cycle.")


                except Exception as e: # Catch any exception during embed creation or inner notification loop setup
                    log.exception(f"Task 'check_closures': Unexpected error during notification processing for UUID {generated_uuid}: {e}")


            # --- State Update Logic ---
            # Add all UUIDs that were successfully processed (sent to >=1 channel)
            if processed_successfully_ids:
                log.info(f"Task 'check_closures': Adding {len(processed_successfully_ids)} newly processed UUIDs to seen state.")
                self.seen_closure_ids.update(processed_successfully_ids)
                # Save state after adding new seen IDs based on successful sends
                try:
                    # Use await for async save_state
                    await self.save_state()
                except Exception as e:
                     log.exception(f"Task 'check_closures': Error saving state after adding seen IDs.")


        # --- Pruning old seen_closure_ids (Optional but recommended) ---
        # This requires knowing which of the `latest_generated_ids` correspond to *active* closures.
        # A simple approach is to remove IDs from seen_closure_ids that are NOT in the latest API list.
        # This assumes the API list *only* contains currently active or relevant closures.
        # If the API contains historical data, this will cause re-notifications of old history.
        # Assuming the API only returns active/recent, let's prune IDs not in the latest fetch.
        if latest_generated_ids: # Only prune if the API fetch was successful and returned items
             log.debug(f"Pruning seen_closure_ids. Before: {len(self.seen_closure_ids)}. Latest API count: {len(latest_generated_ids)}")
             original_seen_count = len(self.seen_closure_ids)
             # Keep only the IDs that are both currently "seen" AND were in the latest API fetch
             self.seen_closure_ids = self.seen_closure_ids.intersection(latest_generated_ids)
             if len(self.seen_closure_ids) < original_seen_count:
                  log.info(f"Pruned {original_seen_count - len(self.seen_closure_ids)} old IDs from seen_closure_ids.")
                  # Save state after pruning
                  try:
                      # Use await for async save_state
                      await self.save_state()
                  except Exception as e:
                       log.exception(f"Task 'check_closures': Error saving state after pruning seen IDs.")
             else:
                  log.debug("No IDs pruned from seen_closure_ids.")
        elif not latest_closures and self._cached_api_closures:
             # If the latest fetch failed BUT we have cached data, we *could* prune based on the cache.
             # But this gets complicated. Simpler: only prune if the latest fetch was successful.
             log.debug("Skipping pruning due to failed API fetch.")


    @check_closures.before_loop
    async def before_check_closures(self):
        """Wait for the bot to be ready and load state before starting the loop."""
        log.info('Task \'check_closures\': Waiting for bot readiness...')
        await self.bot.wait_until_ready() # Wait until the bot is connected and ready
        log.info('Task \'check_closures\': Bot ready. Attempting to load state and start loop.')
        # Load state here once the bot is ready and async operations are possible
        await self.load_state()
        log.info('Task \'check_closures\': State loaded. Loop starting.')


    # --- Error Handling for Commands ---
    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        """A local error handler for commands within this cog."""
        # Ignore errors in other cogs
        if ctx.cog != self:
            return

        # Log the error before sending a message
        log.error(f"Command Error: in command '{ctx.command}' by {ctx.author} (ID: {ctx.author.id}) in {ctx.guild} - {ctx.channel}: {error}", exc_info=True)


        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"⏳ This command is on cooldown. Please try again in {error.retry_after:.1f} seconds.")
        elif isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You do not have the necessary permissions to use this command.")
        elif isinstance(error, commands.MissingRequiredArgument):
             await ctx.send(f"❌ Missing required argument: `{error.param.name}`. Usage: `{ctx.prefix}{ctx.command.parent.name + ' ' if ctx.command.parent else ''}{ctx.command.name} {ctx.command.signature}`")
        elif isinstance(error, commands.BadArgument):
             await ctx.send(f"❌ Bad argument: {error}. Please check your input.")
        elif isinstance(error, commands.NoPrivateMessage):
             await ctx.send("❌ This command can only be used in server channels.")
        elif isinstance(error, commands.GuildNotFound):
             await ctx.send("❌ Error: Guild not found.") # Should not happen often with guild_only
        elif isinstance(error, commands.ChannelNotFound):
             await ctx.send("❌ Error: Channel not found.") # For commands that take a channel argument
        elif isinstance(error, commands.MemberNotFound):
             await ctx.send("❌ Error: Member not found.") # For commands that take a member argument
        elif isinstance(error, commands.RoleNotFound):
             await ctx.send("❌ Error: Role not found.") # For commands that take a role argument
        elif isinstance(error, commands.CommandInvokeError):
             # This wraps exceptions raised inside the command's code
             original_error = error.original
             await ctx.send(f"❌ An error occurred while running this command: `{type(original_error).__name__}: {original_error}`")
             # The traceback is already logged by the exc_info=True above
        # Handle our manual permission check failure
        elif isinstance(error, commands.CheckFailure) and "You do not have permission" in str(error):
             # This might catch if check_permissions was used as a check decorator,
             # but we are calling it manually. The manual check handles the message.
             pass # Handled manually in the command
        else:
            # Log all other unhandled errors
            await ctx.send("❌ An unexpected error occurred while processing your command.")


# --- Cog Setup Function ---
async def setup(bot):
    """Loads the Events cog."""
    # It's best practice to configure logging in your main bot file before loading cogs
    # If not already configured, add a basic handler here (will output to console)
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    # Get the logger instance again after potential basicConfig
    setup_log = logging.getLogger('EventsCog')
    setup_log.info("Attempting to add Events Cog to Bot.")

    try:
        await bot.add_cog(Events(bot))
        setup_log.info("Events Cog Added to Bot successfully.")
    except Exception as e:
        setup_log.exception(f"Failed to add Events Cog to Bot: {e}")