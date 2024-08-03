import discord
from discord import app_commands
from discord.ext import commands
from config import MZFilePath, NTFilePath
from transaction_data import TransactionData
from datetime import datetime, timedelta
import os
import logging
from gre_commands import (
    revenue_command, 
    investments_command, 
    notional_command, 
    bp_command, 
    positions_command, 
    account_summary_command, 
    ron_command
)
from data_commands import data_expiration, data_strike
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

intents = discord.Intents.default()
intents.message_content = True
current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)

class UnregisteredUserError(Exception):
    pass

# User ID to TransactionData instance mapping
USER_DATA_MAPPING = {
    '719322412138627560': TransactionData(MZFilePath),
    '903135191365734400': TransactionData(NTFilePath)
}

# Process CSV for each TransactionData instance
for data_instance in USER_DATA_MAPPING.values():
    data_instance.process_csv()

class MyClient(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

client = MyClient()

def get_user_data(user_id):
    if str(user_id) not in USER_DATA_MAPPING:
        raise UnregisteredUserError("User not registered!")
    return USER_DATA_MAPPING[str(user_id)]

@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    try:
        synced = await client.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(f"Failed to sync commands: {e}")
    
    print("Registered commands:")
    for command in client.tree.get_commands():
        print(f"- /{command.name}")

@client.tree.command(name="gre", description="Execute GRE commands")
@app_commands.describe(command="Choose a command to execute")
@app_commands.choices(command=[
    app_commands.Choice(name="help", value="help"),
    app_commands.Choice(name="revenue", value="revenue"),
    app_commands.Choice(name="investments", value="investments"),
    app_commands.Choice(name="notional", value="notional"),
    app_commands.Choice(name="bp", value="bp"),
    app_commands.Choice(name="positions", value="positions"),
    app_commands.Choice(name="summary", value="summary"),
    app_commands.Choice(name="ron", value="ron")
])
async def gre(interaction: discord.Interaction, command: str):
    try:
        if command == 'help':
            help_text = """
            Available commands:
            /gre help - Show this help message
            /gre revenue - Analyze revenue (default: lifetime)
            /gre investments - Analyze investments
            /gre notional - Analyze notional value
            /gre bp - Show buying power split between cash, stock and LOC
            /gre positions - List outstanding positions
            /gre summary - Show account summary
            /gre ron - Show Return on Notional
            """
            await interaction.response.send_message(help_text)
            return

        # Defer the response for all other commands
        await interaction.response.defer(thinking=True)
        
        # Get the appropriate TransactionData instance for the user
        try:
            mz_data = get_user_data(interaction.user.id)
        except UnregisteredUserError:
            await interaction.followup.send("User not registered!")
            return
        
        if command == 'revenue':
            result = revenue_command(mz_data)
        elif command == 'investments':
            result = investments_command(mz_data)
        elif command == 'notional':
            result = notional_command(mz_data)
        elif command == 'bp':
            result = bp_command(mz_data)
        elif command == 'positions':
            result = positions_command(mz_data)
        elif command == 'summary':
            result = account_summary_command(mz_data)
        elif command == 'ron':
            result = ron_command(mz_data)
        else:
            result = f"Unknown command: {command}. Use `/gre help` for a list of available commands."
        
        await interaction.followup.send(f"{command.capitalize()} as of {current_date.strftime('%Y-%m-%d')}:\n{result}")

    except Exception as e:
        # If an error occurs after deferring, send it as a followup
        await interaction.followup.send(f"An error occurred while processing the command: {str(e)}")
        logging.error(f"Error in gre command: {str(e)}", exc_info=True)

@client.tree.command()
@app_commands.describe(scope="The scope to sync the commands to (global/guild)")
@app_commands.choices(scope=[
    app_commands.Choice(name="Global", value="global"),
    app_commands.Choice(name="Guild", value="guild")
])
async def sync(interaction: discord.Interaction, scope: str):
    if scope == "global":
        synced = await client.tree.sync()
    else:
        synced = await client.tree.sync(guild=interaction.guild)
    
    await interaction.response.send_message(f"Synced {len(synced)} commands {'globally' if scope == 'global' else 'to the current guild'}")

@client.tree.command(name="userid", description="Display your user ID")
async def userid(interaction: discord.Interaction):
    await interaction.response.send_message(f"Your user ID is: {interaction.user.id}")

@client.tree.command(name="data", description="Execute data commands")
@app_commands.describe(command="Choose a command to execute")
@app_commands.choices(command=[
    app_commands.Choice(name="expiration", value="expiration"),
    app_commands.Choice(name="strike", value="strike"),
])

async def data(interaction: discord.Interaction, command: str, ticker: str = None, value: str = None):
    await interaction.response.defer(thinking=True)
    
    if command == "expiration":
        if not ticker or not value:
            await interaction.followup.send("Please provide both ticker and expiration date.")
            return
        tck_prc, result = data_expiration(ticker, value)
        pre_notes=f"Ticker: {ticker}"
        pre_notes+=f"\nExpirate Date: {value}"
        pre_notes+=f"\nCurrent Price: {tck_prc}"
        formatted_table = f"```\n{result}\n```"
        await interaction.followup.send(f"{pre_notes}\n{formatted_table}")  # The result is already formatted for Discord
    elif command == "strike":
        if not ticker or not value:
            await interaction.followup.send("Please provide both ticker and strike price.")
            return
        try:
            strike = float(value)
        except ValueError:
            await interaction.followup.send("Invalid strike price. Please provide a number.")
            return
        tck_prc, result = data_strike(ticker, strike)
        pre_notes = f"Ticker: {ticker}"
        pre_notes += f"\nStrike Price: ${strike:.2f}"
        pre_notes += f"\nCurrent Price: {tck_prc}"
        formatted_table = f"```\n{result}\n```"
        await interaction.followup.send(f"{pre_notes}\n{formatted_table}")
    else:
        result = "Invalid command. Use 'expiration' or 'strike'."

client.run(DISCORD_TOKEN)