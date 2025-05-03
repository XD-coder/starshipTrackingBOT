# Starship Tracking Discord Bot

A Discord bot built with discord.py for tracking and managing starship-related information.

## Setup Instructions

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure the bot:

   - Rename `.env.example` to `.env`
   - Add your Discord bot token to the `.env` file
   - Customize the command prefix (optional)

4. Run the bot:
   ```bash
   python bot.py
   ```

## Features

- Welcome messages for new members
- Server information command
- Latency checking command
- Modular cog-based structure for easy expansion

## Available Commands

- `!ping` - Check bot's latency
- `!serverinfo` - Display server information

## Contributing

Feel free to contribute by creating pull requests or reporting issues.

## License

MIT License