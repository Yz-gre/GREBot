import discord
from discord import app_commands
from discord.ext import commands
from transaction_data import TransactionData
from datetime import datetime, timedelta
from trade_commands import *
from daily_risk import *
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
import os
import logging
import csv
from gre_commands import (
    revenue_command, 
    investments_command, 
    notional_command, 
    bp_command, 
    positions_command, 
    account_summary_command, 
    ron_command
)
from data_commands import data_expiration, data_strike, data_call_vs_roll
from dotenv import load_dotenv, set_key
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
MZFilePath = os.getenv('MZFilePath')
NTFilePath = os.getenv('NTFilePath')
WaryPath = os.getenv('Wariness')

intents = discord.Intents.default()
intents.message_content = True
current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)

class UnregisteredUserError(Exception):
    pass

# User ID to TransactionData instance mapping
USER_DATA_MAPPING = {
    '719322412138627560': TransactionData(
        MZFilePath,
        float(os.getenv('719322412138627560_LOC_LIMIT', '0')),
        float(os.getenv('719322412138627560_LOC_USAGE', '0'))
    ),
    '903135191365734400': TransactionData(
        NTFilePath,
        float(os.getenv('903135191365734400_LOC_LIMIT', '0')),
        float(os.getenv('903135191365734400_LOC_USAGE', '0'))
    )
}

USER_FILEPATH_MAPPING = {
    '719322412138627560': MZFilePath,
    '903135191365734400': NTFilePath
}

# Process CSV for each TransactionData instance
for data_instance in USER_DATA_MAPPING.values():
    data_instance.process_csv()

class MyClient(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

client = MyClient()
client.mz_data = None  # This will be set for each user when needed

def get_user_data(user_id):
    if str(user_id) not in USER_DATA_MAPPING:
        raise UnregisteredUserError("User not registered!")
    return USER_DATA_MAPPING[str(user_id)]

def get_user_filepath(user_id):
    if str(user_id) not in USER_FILEPATH_MAPPING:
        raise UnregisteredUserError("User not registered!")
    return USER_FILEPATH_MAPPING[str(user_id)]

def write_to_csv(file_path, data):
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data)

def update_env_values(user_id, loc_limit, loc_usage):
    # Load current .env file
    load_dotenv()
    
    # Read the current .env file
    with open(".env", "r") as file:
        lines = file.readlines()
    
    # Update the values
    updated_lines = []
    for line in lines:
        if line.startswith(f"{user_id}_LOC_LIMIT="):
            updated_lines.append(f"{user_id}_LOC_LIMIT={loc_limit}\n")
        elif line.startswith(f"{user_id}_LOC_USAGE="):
            updated_lines.append(f"{user_id}_LOC_USAGE={loc_usage}\n")
        else:
            updated_lines.append(line)
    
    # Write the updated content back to the .env file
    with open(".env", "w") as file:
        file.writelines(updated_lines)
    
    # Reload the environment variables
    load_dotenv()
    
    # Update the TransactionData instance
    if str(user_id) in USER_DATA_MAPPING:
        user_data = USER_DATA_MAPPING[str(user_id)]
        user_data.LOCLimit = float(loc_limit)
        user_data.LOCUsage = float(loc_usage)
    
    return f"Updated LOC Limit to {loc_limit} and LOC Usage to {loc_usage} for user {user_id}"

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

# GRE Functions
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
    app_commands.Choice(name="ron", value="ron"),
    app_commands.Choice(name="refresh", value="refresh")
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
            /gre refresh - Reflect new trades
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
        elif command == 'refresh':
            # User ID to TransactionData instance mapping            
            global USER_DATA_MAPPING
            load_dotenv(override=True)
            USER_DATA_MAPPING = None
            USER_DATA_MAPPING = {
                '719322412138627560': TransactionData(
                    MZFilePath,
                    float(os.getenv('719322412138627560_LOC_LIMIT', '0')),
                    float(os.getenv('719322412138627560_LOC_USAGE', '0'))
                ),
                '903135191365734400': TransactionData(
                    NTFilePath,
                    float(os.getenv('903135191365734400_LOC_LIMIT', '0')),
                    float(os.getenv('903135191365734400_LOC_USAGE', '0'))
                )
            }
            # Process CSV for each TransactionData instance
            for data_instance in USER_DATA_MAPPING.values():
                data_instance.process_csv()
            result = "Data refreshed successfully."
        else:
            result = f"Unknown command: {command}. Use `/gre help` for a list of available commands."
        
        current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
        await interaction.followup.send(f"{command.capitalize()} as of {current_date.strftime('%Y-%m-%d')}:\n{result}")

    except Exception as e:
        # If an error occurs after deferring, send it as a followup
        await interaction.followup.send(f"An error occurred while processing the command: {str(e)}")
        logging.error(f"Error in gre command: {str(e)}", exc_info=True)

# Syncing Commands
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

# Displaying User ID
@client.tree.command(name="userid", description="Display your user ID")
async def userid(interaction: discord.Interaction):
    await interaction.response.send_message(f"Your user ID is: {interaction.user.id}")

# Data Commands
@client.tree.command(name="data", description="Execute data commands")
@app_commands.describe(command="Choose a command to execute")
@app_commands.choices(command=[
    app_commands.Choice(name="expiration", value="expiration"),
    app_commands.Choice(name="strike", value="strike"),
    app_commands.Choice(name="call_vs_roll", value="call_vs_roll")
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
    elif command == "call_vs_roll":
        if not ticker or not value:
            await interaction.followup.send("Please provide both ticker and strike price.")
            return
        try:
            strike = float(value)
        except ValueError:
            await interaction.followup.send("Invalid strike price. Please provide a number.")
            return
        tck_prc, result = data_call_vs_roll(ticker, strike)
        pre_notes = f"Ticker: {ticker}"
        pre_notes += f"\nStrike Price: ${strike:.2f}"
        pre_notes += f"\nCurrent Price: {tck_prc}"
        formatted_table = f"```\n{result}\n```"
        await interaction.followup.send(f"{pre_notes}\n{formatted_table}")        
    else:
        result = "Invalid command. Use 'expiration' or 'strike'."

# Trade Commands
@client.tree.command(name="trade", description="Execute trade commands")
@app_commands.describe(
    command="Choose a trade type",
    acct="Account",
    ticker="Ticker symbol",
    shares="Number of shares",
    currency="Currency"
)
@app_commands.choices(command=[
    app_commands.Choice(name=cmd, value=cmd) for cmd in [
        "Add_Trade", "Close_Position", "Roll_Position", "Cov_Call", "Send_CSV",
        "Assigned", "Cash_InOut", "Upd_LOC", "Delete_Last", "Last_Trade", "Int_Pay"
    ]
])
async def transaction(
    interaction: discord.Interaction, 
    command: str,
    acct: Optional[str] = None,
    ticker: Optional[str] = None,
    shares: Optional[int] = None,
    currency: Optional[str] = None
):
    try:
        # Don't defer the response here        

        try:
            mz_data = get_user_data(interaction.user.id)
            client.mz_data = mz_data
        except UnregisteredUserError:
            await interaction.response.send_message("User not registered!")
            return
        
        if command == "Add_Trade":
            if not all([acct, ticker, shares, currency]):
                await interaction.response.send_message("Please provide all required parameters for Add_Trade.")
                return
            await process_add_trade(interaction, mz_data, acct, ticker, shares, currency)
        elif command == "Close_Position":
            await process_close_position(interaction, mz_data)
        elif command == "Last_Trade":
            handler = LastTradeHandler(mz_data)
            await handler.handle(interaction)
        elif command == "Delete_Last":
            handler = DeleteLastTradeHandler(mz_data)
            await handler.handle(interaction)
        elif command == "Cov_Call":
            await process_cov_call(interaction, mz_data)
        elif command == "Assigned":
            await process_assigned(interaction, mz_data)
        elif command == "Cash_InOut":
            handler = CashInOutHandler(mz_data)
            await handler.handle(interaction)
        elif command == "Upd_LOC":
            # Get current values
            current_limit = float(os.getenv(f"{interaction.user.id}_LOC_LIMIT", "0"))
            current_usage = float(os.getenv(f"{interaction.user.id}_LOC_USAGE", "0"))
            
            # Call the handle_loc_update method from trade_commands
            new_limit, new_usage = await handle_loc_update(interaction, current_limit, current_usage)
            
            if new_limit is not None and new_usage is not None:
                # Update the .env file and TransactionData instance
                result = update_env_values(interaction.user.id, new_limit, new_usage)
                await interaction.followup.send(result)
        elif command == "Roll_Position":
            handler = RollPositionHandler(mz_data)
            await handler.handle(interaction)
        elif command == "Int_Pay":
            handler = InterestPaymentHandler(mz_data)
            await handler.handle(interaction)
        elif command == "Send_CSV":
            await interaction.response.defer(thinking=True)
            try:
                # Get the file path for the specific user
                file_path = get_user_filepath(interaction.user.id)
                if not os.path.exists(file_path):
                    await interaction.followup.send("Error: Your CSV file was not found.")
                    return
                file_name = os.path.basename(file_path)
                # Send the file
                await interaction.followup.send(file=discord.File(file_path, filename=file_name))
            except UnregisteredUserError:
                await interaction.followup.send("Error: You are not registered to use this command.")
            except Exception as e:
                await interaction.followup.send(f"An error occurred while sending the file: {str(e)}")
                logging.error(f"Error in send_csv command: {str(e)}", exc_info=True)
        else:
            await interaction.response.send_message(f"Unsupported trade type: {command}")
    
    except Exception as e:
        logging.error(f"Error in handle_trade: {str(e)}")
        if not interaction.response.is_done():
            await interaction.response.send_message(f"An error occurred: {str(e)}")
        else:
            await interaction.followup.send(f"An error occurred: {str(e)}")


scheduler = BackgroundScheduler(timezone=timezone('US/Eastern'))
csv_path = WaryPath  # Update this path

scheduler.add_job(lambda: write_daily_risk_to_csv(csv_path, USER_DATA_MAPPING), 
                  CronTrigger(day_of_week='mon-fri', hour=14, minute=0))
scheduler.add_job(lambda: write_daily_risk_to_csv(csv_path, USER_DATA_MAPPING), 
                  CronTrigger(day_of_week='mon-fri', hour=17, minute=0))
scheduler.add_job(lambda: write_daily_risk_to_csv(csv_path, USER_DATA_MAPPING), 
                  CronTrigger(day_of_week='mon-fri', hour=20, minute=15))

scheduler.start()

client.run(DISCORD_TOKEN)