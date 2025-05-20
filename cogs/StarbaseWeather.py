import discord
import os
import requests
import json
from discord.ext import commands, tasks
from datetime import datetime, timezone, timedelta
import asyncio
import logging
import math # For wind direction formatting

# --- Configuration ---
OPENWEATHER_API_KEY = "16c0240bc5ad9b52f8d80029df9390de"


# Fixed Location for SpaceX Starbase, Boca Chica, Texas
STARBASE_LAT = 25.9968
STARBASE_LON = -97.1533
STARBASE_LOCATION_NAME = "SpaceX Starbase, Boca Chica, TX"

# We will fetch data in ONE unit system (e.g., imperial) and convert for display
# Imperial is often used in the US, let's fetch imperial and display both.
FETCH_UNITS = "imperial"

# File to store channel IDs for hourly updates
CHANNELS_FILE = 'starbase_weather_channels.json'

# API Endpoints (Using coordinates for more precision)
CURRENT_WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather?"
FORECAST_URL = "http://api.openweathermap.org/data/2.5/forecast?"

# --- Logging ---
# Configure basic logging to console. For file logging, you'd expand this.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('StarbaseWeatherCog') # Use a specific logger name

# --- Conversion Constants (based on Imperial fetch) ---
# 1 Fahrenheit = (Celsius * 9/5) + 32
# 1 Celsius = (Fahrenheit - 32) * 5/9
# 1 mph = 0.44704 m/s
# 1 m/s = 2.23694 mph
# 1 hPa = 0.02953 inHg
# 1 meter = 0.000621371 miles
# 1 km = 0.621371 miles

# --- Helper Functions ---

def load_hourly_channels(file_path):
    """Loads channel IDs from a JSON file, with error handling."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                channel_ids = json.load(f)
                logger.info(f"Loaded {len(channel_ids)} channel IDs from {file_path}")
                return channel_ids
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load channel IDs from {file_path}: {e}")
            return []
    logger.info(f"Channel file not found: {file_path}. Starting with empty list.")
    return []

def save_hourly_channels(file_path, channel_ids):
    """Saves channel IDs to a JSON file, with error handling."""
    try:
        with open(file_path, 'w') as f:
            json.dump(channel_ids, f)
            logger.info(f"Saved {len(channel_ids)} channel IDs to {file_path}")
    except IOError as e:
        logger.error(f"Failed to save channel IDs to {file_path}: {e}")

def get_weather_icon_url(icon_code):
    """Generates the URL for the weather icon."""
    return f"http://openweathermap.org/img/wn/{icon_code}@2x.png"

def format_temperature_both(temp_imperial):
    """Formats temperature string in both Fahrenheit and Celsius."""
    if temp_imperial is None: return "N/A"
    temp_celsius = (temp_imperial - 32) * 5/9
    return f"{temp_imperial:.1f}°F ({temp_celsius:.1f}°C)"

def format_speed_both(speed_imperial):
    """Formats speed string in both mph and m/s."""
    if speed_imperial is None: return "N/A"
    speed_metric = speed_imperial * 0.44704
    return f"{speed_imperial:.1f} mph ({speed_metric:.1f} m/s)"

def format_pressure_both(pressure_hPa):
    """Formats pressure string in both hPa and inHg."""
    if pressure_hPa is None: return "N/A"
    pressure_inHg = pressure_hPa * 0.02953
    return f"{pressure_hPa:.1f} hPa ({pressure_inHg:.2f} inHg)"

def format_visibility_both(visibility_meters):
    """Formats visibility string in both meters/km and miles."""
    if visibility_meters is None:
        return "N/A"
    visibility_miles = visibility_meters * 0.000621371
    if visibility_meters >= 1000: # Display in km if 1km or more
         visibility_km = visibility_meters / 1000
         return f"{visibility_km:.1f} km ({visibility_miles:.1f} miles)"
    return f"{visibility_meters:.1f} m ({visibility_miles:.2f} miles)"


def degrees_to_cardinal(degrees):
    """Converts wind direction degrees to cardinal direction."""
    if degrees is None:
        return "N/A"
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    # Ensure degrees is within 0-360
    degrees = degrees % 360
    # Calculate index (add 11.25 for offset, divide by 22.5, floor)
    index = math.floor((degrees + 11.25) / 22.5)
    return directions[index % 16]


def format_time_with_offset(timestamp, offset_seconds):
    """Formats a Unix timestamp to HH:MM AM/PM based on timezone offset."""
    if timestamp is None or offset_seconds is None: return "N/A"
    try:
        # Create a naive datetime object from the UTC timestamp
        dt_utc = datetime.utcfromtimestamp(timestamp)
        # Apply the offset to get a naive datetime object representing local time
        dt_local = dt_utc + timedelta(seconds=offset_seconds)
        return dt_local.strftime('%I:%M %p')
    except Exception as e:
        logger.error(f"Error formatting time with offset {timestamp}, {offset_seconds}: {e}")
        return "N/A"


def format_datetime_with_offset(timestamp, offset_seconds):
    """Formats a Unix timestamp to TIMESTAMP-MM-DD HH:MM AM/PM based on timezone offset."""
    if timestamp is None or offset_seconds is None: return "N/A"
    try:
        dt_utc = datetime.utcfromtimestamp(timestamp)
        dt_local = dt_utc + timedelta(seconds=offset_seconds)
        return dt_local.strftime('%Y-%m-%d %I:%M %p')
    except Exception as e:
         logger.error(f"Error formatting datetime with offset {timestamp}, {offset_seconds}: {e}")
         return "N/A"


# --- Cog Class ---

class StarbaseWeatherCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.api_key = OPENWEATHER_API_KEY
        self.is_ready = False # Assume not ready until API key is confirmed

        if not self.api_key:
            logger.critical("OPENWEATHER_API_KEY environment variable not set. Weather cog will not function.")
            # Don't raise error, just mark as not ready.
        else:
            self.is_ready = True
            self.hourly_update_channels = load_hourly_channels(CHANNELS_FILE)
            # Start the hourly update task ONLY if API key is present
            self.hourly_weather_update_task.start()
            logger.info("StarbaseWeatherCog initialized and hourly task started.")

    # --- API Fetching Helper ---
    def fetch_weather_data(self, endpoint_url):
        """Fetches weather data from OpenWeatherMap API using fixed coordinates and FETCH_UNITS, with logging."""
        if not self.is_ready:
             logger.warning("Attempted to fetch weather data but API key is missing.")
             return None

        params = {
            "lat": STARBASE_LAT,
            "lon": STARBASE_LON,
            "appid": self.api_key,
            "units": FETCH_UNITS # Use the configured fetch units
        }
        try:
            response = requests.get(endpoint_url, params=params)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            logger.debug(f"Successfully fetched data from {endpoint_url} for Starbase.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data from {endpoint_url} for Starbase: {e}")
            return None
        except json.JSONDecodeError as e:
             logger.error(f"Failed to decode JSON response from {endpoint_url}: {e}")
             return None


    # --- Current Weather Command ---
    @commands.command(name='starbaseweather', aliases=['sw', 'starbase'])
    @commands.cooldown(1, 15, commands.BucketType.guild) # Add a cooldown
    async def current_starbase_weather(self, ctx):
        """Gets current weather for SpaceX Starbase, showing both Metric and Imperial units."""
        logger.info(f"Command received: {ctx.command} from {ctx.author} in {ctx.guild} - {ctx.channel}")
        if not self.is_ready:
             logger.warning(f"Command {ctx.command} failed: API key missing.")
             await ctx.send("Weather service is not configured. Please set the OPENWEATHER_API_KEY.")
             return

        data = self.fetch_weather_data(CURRENT_WEATHER_URL)

        if data is None:
            await ctx.send(f"Could not fetch current weather data for {STARBASE_LOCATION_NAME}. API error occurred.")
            return

        try:
            # Extract relevant data (assuming FETCH_UNITS was used)
            weather_info = data.get('weather', [{}])[0] # Handle potential empty list
            main_data = data.get('main', {})
            clouds_data = data.get('clouds', {})
            wind_data = data.get('wind', {})
            sys_data = data.get('sys', {})
            rain_data = data.get('rain', {})
            snow_data = data.get('snow', {})
            dt_timestamp = data.get('dt')
            timezone_offset = data.get('timezone')
            visibility_meters = data.get('visibility')

            # Format data for embed using combined formats
            temp = format_temperature_both(main_data.get('temp'))
            feels_like = format_temperature_both(main_data.get('feels_like'))
            humidity = f"{main_data.get('humidity', 'N/A')}%"
            cloudiness = f"{clouds_data.get('all', 'N/A')}%"

            wind_speed = format_speed_both(wind_data.get('speed'))
            wind_deg = wind_data.get('deg')
            wind_gust = wind_data.get('gust')

            wind_info = f"Speed: {wind_speed}"
            if wind_deg is not None:
                cardinal_direction = degrees_to_cardinal(wind_deg)
                wind_info += f" from {cardinal_direction} ({wind_deg}°)"
            if wind_gust is not None:
                 wind_info += f" | Gusts: {format_speed_both(wind_gust)}"


            weather_description = weather_info.get('description', 'N/A').capitalize()
            weather_icon_code = weather_info.get('icon')
            weather_icon_url = get_weather_icon_url(weather_icon_code) if weather_icon_code else None

            # Precipitation (always mm in OWM standard API)
            precipitation = "None"
            mm_value = None
            if '1h' in rain_data:
                mm_value = rain_data['1h']
                precipitation = f"{mm_value} mm"
            elif '3h' in rain_data:
                mm_value = rain_data['3h']
                precipitation = f"{mm_value} mm"
            elif '1h' in snow_data:
                 mm_value = snow_data['1h']
                 precipitation = f"{mm_value} mm"
            elif '3h' in snow_data:
                 mm_value = snow_data['3h']
                 precipitation = f"{mm_value} mm"
            if mm_value is not None:
                inches_value = mm_value * 0.0393701
                precipitation += f" ({inches_value:.2f} inches)"


            # Times (Convert timestamps considering timezone offset)
            current_time_str = format_datetime_with_offset(dt_timestamp, timezone_offset)
            sunrise_str = format_time_with_offset(sys_data.get('sunrise'), timezone_offset)
            sunset_str = format_time_with_offset(sys_data.get('sunset'), timezone_offset)

            # Pressure and Visibility (Visibility always meters)
            pressure = format_pressure_both(main_data.get('pressure'))
            visibility = format_visibility_both(visibility_meters)


            # Build the embed
            embed = discord.Embed(
                title=f"Current Weather at {STARBASE_LOCATION_NAME}",
                description=f"**{weather_description}**",
                color=discord.Color.blue()
            )

            if weather_icon_url:
                 embed.set_thumbnail(url=weather_icon_url)

            embed.add_field(name="Temperature", value=temp, inline=True)
            embed.add_field(name="Feels Like", value=feels_like, inline=True)
            embed.add_field(name="Humidity", value=humidity, inline=True)
            embed.add_field(name="Cloudiness", value=cloudiness, inline=True)
            embed.add_field(name="Wind", value=wind_info, inline=False)
            embed.add_field(name="Pressure", value=pressure, inline=True)
            embed.add_field(name="Visibility", value=visibility, inline=True)
            embed.add_field(name="Precipitation (1/3h)", value=precipitation, inline=False)

            embed.add_field(name="Sunrise", value=sunrise_str, inline=True)
            embed.add_field(name="Sunset", value=sunset_str, inline=True)
            embed.add_field(name="Data As Of", value=current_time_str, inline=False)


            embed.set_footer(text="Powered by OpenWeatherMap | Data may be up to 20 minutes old.")

            await ctx.send(embed=embed)
            logger.info(f"Successfully sent current weather embed for Starbase to {ctx.channel} ({ctx.guild}).")

        except Exception as e:
            logger.error(f"An unexpected error occurred processing weather data for current command in {ctx.channel} ({ctx.guild}): {e}", exc_info=True)
            await ctx.send("An unexpected error occurred while processing the weather data.")


    # --- Forecast Command ---
    @commands.command(name='starbaseforecast', aliases=['sf', 'starforecast'])
    @commands.cooldown(1, 30, commands.BucketType.guild) # Add a cooldown
    async def starbase_forecast(self, ctx, hours_ahead: int = 12):
        """Gets the weather forecast for SpaceX Starbase for the next few hours, both Metric and Imperial."""
        logger.info(f"Command received: {ctx.command} {hours_ahead} from {ctx.author} in {ctx.guild} - {ctx.channel}")
        if not self.is_ready:
             logger.warning(f"Command {ctx.command} failed: API key missing.")
             await ctx.send("Weather service is not configured. Please set the OPENWEATHER_API_KEY.")
             return

        # Limit forecast hours to prevent massive embeds
        if hours_ahead > 48:
            hours_ahead = 48
            await ctx.send("Limiting forecast to a maximum of 48 hours.")
        if hours_ahead <= 0:
             await ctx.send("Hours ahead must be a positive number.")
             logger.warning(f"Command {ctx.command} failed: Invalid hours_ahead ({hours_ahead}) from {ctx.author}.")
             return

        data = self.fetch_weather_data(FORECAST_URL)

        if data is None or not data.get('list'):
            await ctx.send(f"Could not fetch forecast data for {STARBASE_LOCATION_NAME}. API error occurred.")
            return

        try:
            forecast_list = data['list']
            timezone_offset = data.get('city', {}).get('timezone', 0)

            embed = discord.Embed(
                title=f"Weather Forecast for {STARBASE_LOCATION_NAME}",
                color=discord.Color.green()
            )

            embed.description = "Showing forecast in both Fahrenheit/Celsius, mph/m/s, hPa/inHg, and mm/inches."


            entries_to_show = (hours_ahead // 3) + 1
            if entries_to_show > len(forecast_list):
                entries_to_show = len(forecast_list)

            if entries_to_show == 0:
                 await ctx.send(f"No forecast data available for the next {hours_ahead} hours.")
                 logger.info(f"No forecast entries found for {hours_ahead} hours for {STARBASE_LOCATION_NAME}.")
                 return

            for i in range(entries_to_show):
                entry = forecast_list[i]
                weather_info = entry.get('weather', [{}])[0]
                main_data = entry.get('main', {})
                wind_data = entry.get('wind', {})
                rain_data = entry.get('rain', {})
                snow_data = entry.get('snow', {})
                dt_timestamp = entry.get('dt')

                # Format entry data using combined formats
                forecast_time_str = format_datetime_with_offset(dt_timestamp, timezone_offset)
                temp = format_temperature_both(main_data.get('temp'))
                feels_like = format_temperature_both(main_data.get('feels_like'))
                description = weather_info.get('description', 'N/A').capitalize()

                wind_speed = format_speed_both(wind_data.get('speed'))
                wind_deg = wind_data.get('deg')
                wind_gust = wind_data.get('gust')

                wind_info = f"Speed: {wind_speed}"
                if wind_deg is not None:
                     cardinal_direction = degrees_to_cardinal(wind_deg)
                     wind_info += f" from {cardinal_direction}"
                if wind_gust is not None:
                     wind_info += f" | Gusts: {format_speed_both(wind_gust)}"

                # Precipitation (always mm in OWM standard API)
                precipitation = "None"
                mm_value = None
                if '1h' in rain_data:
                    mm_value = rain_data['1h']
                    precipitation = f"{mm_value} mm"
                elif '3h' in rain_data:
                    mm_value = rain_data['3h']
                    precipitation = f"{mm_value} mm"
                elif '1h' in snow_data:
                     mm_value = snow_data['1h']
                     precipitation = f"{mm_value} mm"
                elif '3h' in snow_data:
                     mm_value = snow_data['3h']
                     precipitation = f"{mm_value} mm"
                if mm_value is not None:
                    inches_value = mm_value * 0.0393701
                    precipitation += f" ({inches_value:.2f} inches)"


                # Add field for this forecast entry
                embed.add_field(
                    name=f"Time: {forecast_time_str}",
                    value=(
                        f"**{description}**\n"
                        f"Temp: {temp} (Feels like: {feels_like})\n"
                        f"Wind: {wind_info}\n"
                        f"Precipitation: {precipitation}"
                    ),
                    inline=False # Make each forecast entry a new line
                )

            embed.set_footer(text="Powered by OpenWeatherMap | Forecast data in 3-hour steps.")

            await ctx.send(embed=embed)
            logger.info(f"Successfully sent forecast embed for Starbase ({hours_ahead}h, both units) to {ctx.channel} ({ctx.guild}).")

        except Exception as e:
            logger.error(f"An unexpected error occurred processing forecast data for command in {ctx.channel} ({ctx.guild}): {e}", exc_info=True)
            await ctx.send("An unexpected error occurred while processing the forecast data.")


    # --- Hourly Weather Update Task ---
    @tasks.loop(hours=3)
    async def hourly_weather_update_task(self):
        """Task that sends hourly weather updates to registered channels, showing both Metric and Imperial."""
        if not self.is_ready:
             logger.warning("Hourly task tried to run but cog is not ready (API key missing).")
             return # Don't run if API key is missing

        # Only run if there are channels registered
        if not self.hourly_update_channels:
            logger.debug("No channels registered for hourly updates. Skipping task.")
            return

        logger.info(f"Running hourly weather update task for {STARBASE_LOCATION_NAME}")
        data = self.fetch_weather_data(FORECAST_URL)

        if data is None or not data.get('list'):
            logger.error(f"Could not fetch forecast data for {STARBASE_LOCATION_NAME} for hourly update. API error occurred.")
            # Optionally send a non-embed message to channels or log more verbosely?
            # For now, just log the error.
            return

        try:
            # Get the first forecast entry (closest to current time)
            forecast_entry = data.get('list', [{}])[0]
            timezone_offset = data.get('city', {}).get('timezone', 0) # Offset for the city

            # Extract relevant data from the forecast entry (assuming FETCH_UNITS was used)
            weather_info = forecast_entry.get('weather', [{}])[0]
            main_data = forecast_entry.get('main', {})
            clouds_data = forecast_entry.get('clouds', {})
            wind_data = forecast_entry.get('wind', {})
            rain_data = forecast_entry.get('rain', {})
            snow_data = forecast_entry.get('snow', {})
            dt_timestamp = forecast_entry.get('dt')


            # Format data for embed using combined formats
            temp = format_temperature_both(main_data.get('temp'))
            feels_like = format_temperature_both(main_data.get('feels_like'))
            humidity = f"{main_data.get('humidity', 'N/A')}%"
            cloudiness = f"{clouds_data.get('all', 'N/A')}%"

            wind_speed = format_speed_both(wind_data.get('speed'))
            wind_deg = wind_data.get('deg')
            wind_gust = wind_data.get('gust')

            wind_info = f"Speed: {wind_speed}"
            if wind_deg is not None:
                cardinal_direction = degrees_to_cardinal(wind_deg)
                wind_info += f" from {cardinal_direction} ({wind_deg}°)"
            if wind_gust is not None:
                 wind_info += f" | Gusts: {format_speed_both(wind_gust)}"

            weather_description = weather_info.get('description', 'N/A').capitalize()
            weather_icon_code = weather_info.get('icon')
            weather_icon_url = get_weather_icon_url(weather_icon_code) if weather_icon_code else None


            # Precipitation (always mm in OWM standard API)
            precipitation = "None"
            mm_value = None
            if '1h' in rain_data:
                mm_value = rain_data['1h']
                precipitation = f"{mm_value} mm"
            elif '3h' in rain_data:
                mm_value = rain_data['3h']
                precipitation = f"{mm_value} mm"
            elif '1h' in snow_data:
                 mm_value = snow_data['1h']
                 precipitation = f"{mm_value} mm"
            elif '3h' in snow_data:
                 mm_value = snow_data['3h']
                 precipitation = f"{mm_value} mm"
            if mm_value is not None:
                inches_value = mm_value * 0.0393701
                precipitation += f" ({inches_value:.2f} inches)"


            forecast_time_str = format_datetime_with_offset(dt_timestamp, timezone_offset)


            # Build the embed for the update
            embed = discord.Embed(
                title=f"Hourly Weather Update for {STARBASE_LOCATION_NAME}",
                description=f"Forecast for **{forecast_time_str}**\n**{weather_description}**",
                color=discord.Color.orange() # Different color for updates
            )

            if weather_icon_url:
                embed.set_thumbnail(url=weather_icon_url)

            embed.add_field(name="Temperature", value=temp, inline=True)
            embed.add_field(name="Feels Like", value=feels_like, inline=True)
            embed.add_field(name="Humidity", value=humidity, inline=True)
            embed.add_field(name="Cloudiness", value=cloudiness, inline=True)
            embed.add_field(name="Wind", value=wind_info, inline=False)
            embed.add_field(name="Precipitation (1/3h)", value=precipitation, inline=False)

            embed.set_footer(text=f"Update for {STARBASE_LOCATION_NAME} | Powered by OpenWeatherMap | Forecast from nearest 3-hour step | Units: °F/°C, mph/m/s, hPa/inHg, mm/inches.")

            # Send the embed to all registered channels
            successful_sends = 0
            failed_sends = 0
            channels_to_remove = []

            for channel_id in self.hourly_update_channels:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    try:
                        await channel.send(embed=embed)
                        successful_sends += 1
                        
                    except discord.Forbidden:
                        logger.warning(f"Discord Forbidden error sending to channel: {channel_id}. Removing from list.")
                        channels_to_remove.append(channel_id)
                        failed_sends += 1
                    except discord.NotFound:
                        logger.warning(f"Channel not found: {channel_id}. It might have been deleted. Removing from list.")
                        channels_to_remove.append(channel_id)
                        failed_sends += 1
                    except Exception as e:
                        logger.error(f"Error sending message to channel {channel_id}: {e}", exc_info=True)
                        failed_sends += 1
                else:
                    logger.warning(f"Channel object not found for ID: {channel_id} during hourly task. It might have been deleted or bot isn't in guild. Removing from list.")
                    channels_to_remove.append(channel_id)
                    failed_sends += 1

            # Clean up channels that failed permanently
            if channels_to_remove:
                self.hourly_update_channels = [cid for cid in self.hourly_update_channels if cid not in channels_to_remove]
                save_hourly_channels(CHANNELS_FILE, self.hourly_update_channels)
                logger.info(f"Removed {len(channels_to_remove)} invalid channels from hourly updates. {len(self.hourly_update_channels)} channels remaining.")

            logger.info(f"Hourly update task finished. Sent to {successful_sends} channels, failed for {failed_sends}.")

        except Exception as e:
            logger.error(f"An unexpected error occurred during hourly weather update task: {e}", exc_info=True)


    @hourly_weather_update_task.before_loop
    async def _before_hourly_update(self):
        """Waits until the bot is ready and cog is ready before starting the task."""
        await self.bot.wait_until_ready()
        # Wait a bit longer if the cog wasn't ready initially (e.g., missing API key)
        if not self.is_ready:
            logger.warning("StarbaseWeatherCog not ready (API key missing). Hourly task will not start its loop.")
            # Cancel the task if the cog isn't ready
            self.hourly_weather_update_task.cancel()
            return
        logger.info("Hourly weather update task waiting for bot to be ready - Ready. Starting loop.")


    # --- Channel Management Commands ---

    # Use a group for weather channel management commands
    @commands.group(name='starbasechannels', aliases=['swc'], invoke_without_command=True)
    @commands.has_permissions(manage_channels=True) # Restrict to users with manage channels permission
    async def starbase_channels(self, ctx):
        """Manages channels for hourly Starbase weather updates."""
        logger.info(f"Command received: {ctx.command.parent} from {ctx.author} in {ctx.guild} - {ctx.channel}")
        if ctx.invoked_subcommand is None:
            await ctx.send("Invalid subcommand. Use `add`, `remove`, or `list`.")
            logger.warning(f"Invalid subcommand for {ctx.command.parent} from {ctx.author}: No subcommand provided.")

    @starbase_channels.command(name='add')
    async def add_channel(self, ctx, channel: discord.TextChannel):
        """Adds a channel to the hourly Starbase weather update list."""
        logger.info(f"Command received: {ctx.command.parent} add {channel.name} from {ctx.author}")
        if not self.is_ready:
             logger.warning(f"Command {ctx.command.parent} add failed: API key missing.")
             await ctx.send("Weather service is not configured. Cannot add channels.")
             return

        channel_id = channel.id
        if channel_id in self.hourly_update_channels:
            await ctx.send(f"{channel.mention} is already in the hourly update list.")
            logger.info(f"Attempted to add duplicate channel: {channel_id} ({channel.guild.name} - {channel.name}) by {ctx.author}.")
        else:
            self.hourly_update_channels.append(channel_id)
            save_hourly_channels(CHANNELS_FILE, self.hourly_update_channels)
            await ctx.send(f"Added {channel.mention} to the hourly weather update list for {STARBASE_LOCATION_NAME}.")
            logger.info(f"Added channel {channel_id} ({channel.guild.name} - {channel.name}) for hourly Starbase updates by {ctx.author}.")

    @starbase_channels.command(name='remove')
    async def remove_channel(self, ctx, channel: discord.TextChannel):
        """Removes a channel from the hourly Starbase weather update list."""
        logger.info(f"Command received: {ctx.command.parent} remove {channel.name} from {ctx.author}")
        if not self.is_ready:
             logger.warning(f"Command {ctx.command.parent} remove failed: API key missing.")
             await ctx.send("Weather service is not configured. Cannot remove channels.")
             return

        channel_id = channel.id
        try:
            self.hourly_update_channels.remove(channel_id)
            save_hourly_channels(CHANNELS_FILE, self.hourly_update_channels)
            await ctx.send(f"Removed {channel.mention} from the hourly weather update list.")
            logger.info(f"Removed channel {channel_id} ({channel.guild.name} - {channel.name}) from hourly Starbase updates by {ctx.author}.")
        except ValueError:
            await ctx.send(f"{channel.mention} was not in the hourly weather update list.")
            logger.info(f"Attempted to remove non-existent channel: {channel_id} ({channel.guild.name} - {channel.name}) by {ctx.author}.")

    @starbase_channels.command(name='list')
    async def list_channels(self, ctx):
        """Lists channels currently receiving hourly Starbase weather updates."""
        logger.info(f"Command received: {ctx.command.parent} list from {ctx.author}")
        if not self.is_ready:
             logger.warning(f"Command {ctx.command.parent} list failed: API key missing.")
             await ctx.send("Weather service is not configured. Cannot list channels.")
             return

        if not self.hourly_update_channels:
            await ctx.send("No channels are currently configured for hourly Starbase weather updates.")
            logger.info(f"List weather channels requested by {ctx.author}, but list is empty.")
            return

        channel_mentions = []
        for channel_id in self.hourly_update_channels:
            channel = self.bot.get_channel(channel_id)
            if channel:
                channel_mentions.append(f"{channel.mention} (`{channel.guild.name} - {channel.name}`)")
            else:
                channel_mentions.append(f"`Unknown Channel ({channel_id})` - Possibly deleted or bot not in guild.")
                logger.warning(f"Channel with ID {channel_id} found in saved list but not retrievable by bot.")


        list_text = f"Channels configured for hourly weather updates for `{STARBASE_LOCATION_NAME}`:\n"
        list_text += "\n".join(channel_mentions)

        await ctx.send(list_text)
        logger.info(f"Sent list of {len(self.hourly_update_channels)} Starbase weather channels to {ctx.channel} ({ctx.guild}).")

    # --- Cog Cleanup ---
    def cog_unload(self):
        """Stops the background task when the cog is unloaded."""
        if self.is_ready: # Only cancel if the task was started
            self.hourly_weather_update_task.cancel()
            logger.info("StarbaseWeatherCog unloaded and hourly task cancelled.")
        else:
             logger.info("StarbaseWeatherCog unloaded (was not fully ready). Task was not running.")


    # --- Error Handling for Commands ---
    async def cog_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Global error handler for commands within this cog."""
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"This command is on cooldown. Please try again in {error.retry_after:.1f} seconds.")
            logger.warning(f"Command {ctx.command} on cooldown for {ctx.author}. Retry after {error.retry_after:.1f}s.")
        elif isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have the necessary permissions to use this command.")
            logger.warning(f"Permission error for {ctx.author} using {ctx.command}: Missing permissions.")
        elif isinstance(error, commands.MissingRequiredArgument):
             await ctx.send(f"Missing required argument: {error.param.name}. Usage: `{ctx.prefix}{ctx.command.parent or ''}{ctx.command.name} {ctx.command.signature}`")
             logger.warning(f"Missing argument for {ctx.command} from {ctx.author}: {error.param.name}")
        elif isinstance(error, commands.BadArgument):
             await ctx.send(f"Bad argument: {error}. Please check your input.")
             logger.warning(f"Bad argument for {ctx.command} from {ctx.author}: {error}")
        elif isinstance(error, commands.NoPrivateMessage):
             await ctx.send("This command cannot be used in private messages.")
             logger.warning(f"Attempted to use {ctx.command} in DM by {ctx.author}.")
        else:
            # All other errors
            logger.error(f"An unexpected error occurred during command {ctx.command} for {ctx.author}: {error}", exc_info=True)
            await ctx.send("An unexpected error occurred while running this command.")


# --- Setup Function ---
async def setup(bot):
    if not OPENWEATHER_API_KEY:
        print("\nWARNING: OPENWEATHER_API_KEY environment variable is not set. StarbaseWeatherCog will load but weather commands will not function.\n")
        cog = StarbaseWeatherCog(bot)
        # The cog's __init__ already sets is_ready=False if key is missing
        await bot.add_cog(cog)
        logger.warning("StarbaseWeatherCog added to the bot, but not fully ready due to missing API key.")

    else:
        await bot.add_cog(StarbaseWeatherCog(bot))
        logger.info("StarbaseWeatherCog successfully added to the bot.")