import discord
from discord.ext import commands
from db import get_db_connection
from config import DISCORD_TOKEN

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO discord_messages (message_id, content, timestamp, channel_id, user_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO NOTHING;
            """, (
                str(message.id),
                message.content,
                message.created_at,
                str(message.channel.id),
                str(message.author.id)
            ))
            conn.commit()
            print(f"Stored message {message.id}")
        except Exception as e:
            print(f"Error storing message: {e}")
        finally:
            cursor.close()
            conn.close()

bot.run(DISCORD_TOKEN)
