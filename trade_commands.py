import discord
import csv
from discord import app_commands
from transaction_data import TransactionData
from typing import Dict, List
from datetime import datetime

def parse_number(value):
    """Parse a string number that may include commas and dollar signs."""
    try:
        return float(value.replace(',', '').replace('$', '').strip())
    except ValueError:
        print(f"Warning: Could not parse '{value}' as a number. Using 0.")
        return 0

def format_number(value, is_currency=False):
    """
    Format numbers based on whether they are currency or not.
    
    :param value: The number to format
    :param is_currency: Boolean indicating if the number is currency
    :return: Formatted string
    """
    try:
        num = float(value)
        if is_currency:
            return f"{num:,.2f}"
        elif num.is_integer():
            return f"{int(num)}"
        else:
            return f"{num:.2f}"
    except ValueError:
        return value  # Return the original value if it's not a number

class TransactionModal(discord.ui.Modal):
    def __init__(self, position_to_close):
        super().__init__(title=f"Close Position: {position_to_close['Ticker']}")
        self.position_to_close = position_to_close

        # Get today's date in the required format
        today = datetime.now().strftime('%Y-%m-%d')

        self.add_item(discord.ui.TextInput(
            label="Transaction Date (YYYY-MM-DD)", 
            placeholder="e.g. 2023-07-15",
            default=today
        ))
        self.add_item(discord.ui.TextInput(
            label=f"Number of Shares (max: {abs(position_to_close['Shares'])})",
            placeholder=f"Enter up to {abs(position_to_close['Shares'])}"
        ))
        
        if position_to_close['Type'] == 'Stk':
            self.add_item(discord.ui.TextInput(label="Closing Gains", placeholder="e.g. 500.25"))
        else:
            self.add_item(discord.ui.TextInput(label="Closing Costs", placeholder="e.g. 50.25"))
        
        self.add_item(discord.ui.TextInput(label="Notes", style=discord.TextStyle.long, required=False))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            date = datetime.strptime(self.children[0].value, '%Y-%m-%d')
            shares = int(self.children[1].value)
            closing_value = float(self.children[2].value)
            notes = self.children[3].value

            if shares > abs(self.position_to_close['Shares']):
                raise ValueError(f"Cannot close more shares than outstanding ({abs(self.position_to_close['Shares'])})")

            if self.position_to_close['Type'] == 'Stk':
                transactions = create_stock_closing_transactions(self.position_to_close, date, shares, closing_value, notes)
            else:
                transactions = [create_option_closing_transaction(self.position_to_close, date, shares, closing_value, notes)]

            transactions_display = "\n\n".join([format_transaction_display(t) for t in transactions])
            await interaction.response.send_message(
                f"New transaction(s) to be added:\n```\n{transactions_display}\n```\nDo you want to add these transactions?",
                view=ConfirmView(transactions)
            )
        except ValueError as e:
            await interaction.response.send_message(f"Error in input: {str(e)}", ephemeral=True)

class ConfirmView(discord.ui.View):
    def __init__(self, transactions):
        super().__init__()
        self.transactions = transactions

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for transaction in self.transactions:
            append_transaction(interaction.client.mz_data.csv_path, transaction)
            interaction.client.mz_data.process_transaction(transaction)
        await interaction.response.send_message("Transaction(s) added successfully.")
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Transaction(s) cancelled.")
        self.stop()

async def handle_trade(interaction: discord.Interaction, mz_data: TransactionData, trade_type: str):
    if trade_type == "Close_Position":
        await handle_close_position(interaction, mz_data)
    # Add other trade types here as elif statements
    else:
        await interaction.followup.send(f"Unsupported trade type: {trade_type}")

async def handle_close_position(interaction: discord.Interaction, mz_data: TransactionData):
    outstanding_positions = get_outstanding_positions(mz_data)
    positions_display = format_positions_display(outstanding_positions)
    
    select = discord.ui.Select(
        placeholder="Choose a position to close",
        options=[discord.SelectOption(label=f"{i+1}. {pos['Ticker']} ({pos['Type']}) @ {pos['Strike/Price']} on {pos['Expiry']}", value=str(i)) 
                 for i, pos in enumerate(outstanding_positions)]
    )

    async def select_callback(select_interaction: discord.Interaction):
        position_id = int(select_interaction.data["values"][0])
        position_to_close = outstanding_positions[position_id]
        
        modal = TransactionModal(position_to_close)
        await select_interaction.response.send_modal(modal)

    select.callback = select_callback
    view = discord.ui.View()
    view.add_item(select)
    await interaction.followup.send(f"Current outstanding positions:\n{positions_display}\n\nPlease select a position to close:", view=view)

def get_outstanding_positions(mz_data: TransactionData) -> List[Dict[str, str]]:
    outstanding_positions = []
    
    # Get outstanding options
    for key, value in mz_data.daily_balances['opt_positions'].items():
        if value[max(value.keys())] != 0:  # If the latest balance is not 0
            outstanding_positions.append({
                'Acct': key[0],
                'Ticker': key[3],
                'Currency': key[1],
                'Margin %': key[2],
                'Type': key[4],
                'Shares': value[max(value.keys())],
                'Strike/Price': key[5],
                'Expiry': key[6]
            })
    
    # Get outstanding stocks
    print("Debugging stock positions:")
    for key, value in mz_data.daily_balances['stk_shares'].items():
        if value[max(value.keys())] != 0:  # If the latest balance is not 0
            print(f"Found non-zero stock position: {key}")
            
            # The notional key is the same as the stock shares key
            notional_key = key
            print(f"Using notional key: {notional_key}")
            
            if notional_key in mz_data.daily_balances['stk_notional']:
                notional_value = mz_data.daily_balances['stk_notional'][notional_key]
                print(f"Notional value found: {notional_value}")
                latest_notional = notional_value[max(notional_value.keys())]
                latest_notional = parse_number(str(latest_notional))
            else:
                print(f"Notional key not found in stk_notional.")
                latest_notional = 0

            print(f"Latest notional: {latest_notional}")

            outstanding_positions.append({
                'Acct': key[0],
                'Ticker': key[3],
                'Currency': key[1],
                'Margin %': key[2],
                'Type': 'Stk',
                'Shares': value[max(value.keys())],
                'Strike/Price': '0',
                'Expiry': '9999-12-31',
                'Net Gains': format_number(latest_notional, is_currency=True)
            })
    
    return outstanding_positions

def format_positions_display(positions: List[Dict[str, str]]) -> str:
    display = ""
    for i, position in enumerate(positions, start=1):
        if position['Type'] == 'Stk':
            display += f"{i}. {position['Ticker']} (Stock) x {abs(position['Shares'])}\n"
        else:
            display += f"{i}. {position['Ticker']} ({position['Type']}) @ {position['Strike/Price']} on {position['Expiry']} x {abs(position['Shares'])}\n"
    return display

def create_option_closing_transaction(position, date, shares, closing_costs, notes):
    return {
        'Acct': position['Acct'],
        'Ticker': position['Ticker'],
        'Currency': position['Currency'],
        'Margin %': position['Margin %'],
        'Date': date.strftime('%Y-%m-%d'),
        'Trans Type': position['Type'],
        'Shares': format_number(-shares),  # Negative to close the position
        'Strike/Price': format_number(position['Strike/Price']),
        'Expiry': position['Expiry'],
        'Net Gains': format_number(-closing_costs, is_currency=True),  # Negative because it's a cost
        'Notes': notes
    }

def create_stock_closing_transactions(position, date, shares, closing_gains, notes):
    # Calculate the proportional Net Gains based on the number of shares being closed
    total_shares = abs(float(position['Shares']))
    total_notional = abs(parse_number(position['Net Gains']))
    proportion = shares / total_shares
    proportional_notional = total_notional * proportion

    closing_transaction = {
        'Acct': position['Acct'],
        'Ticker': position['Ticker'],
        'Currency': position['Currency'],
        'Margin %': position['Margin %'],
        'Date': date.strftime('%Y-%m-%d'),
        'Trans Type': 'Stk',
        'Shares': format_number(-shares),  # Negative to close the position
        'Strike/Price': '0',
        'Expiry': '9999-12-31',
        'Net Gains': format_number(proportional_notional, is_currency=True),
        'Notes': notes
    }
    
    capital_gains_transaction = {
        'Acct': position['Acct'],
        'Ticker': position['Ticker'],
        'Currency': position['Currency'],
        'Margin %': position['Margin %'],
        'Date': date.strftime('%Y-%m-%d'),
        'Trans Type': 'Cap Gains',
        'Shares': '0',
        'Strike/Price': '0',
        'Expiry': '9999-12-31',
        'Net Gains': format_number(float(closing_gains) - proportional_notional, is_currency=True),
        'Notes': ''
    }
    
    return [closing_transaction, capital_gains_transaction]

def format_transaction_display(transaction: Dict[str, str]) -> str:
    header_mapping = {
        "Acct": "Acct", "Ticker": "Tckr", "Currency": "CCY",
        "Margin %": "Mrgn", "Date": "Date", "Trans Type": "Type",
        "Shares": "Shrs", "Strike/Price": "StPr", "Expiry": "Expr",
        "Net Gains": "NtGn", "Notes": "Note"
    }
    
    # Split the items into two columns
    items = list(transaction.items())
    mid = len(items) // 2
    left_column = items[:mid]
    right_column = items[mid:]
    
    # Find the maximum width for each column
    max_key_length_left = max(len(header_mapping[key]) for key, _ in left_column)
    max_key_length_right = max(len(header_mapping[key]) for key, _ in right_column)
    max_value_length_left = max(len(str(value)) for _, value in left_column)
    
    # Format the output
    lines = []
    for (key1, value1), (key2, value2) in zip(left_column, right_column):
        left = f"{header_mapping[key1]:<{max_key_length_left}} : {value1:<{max_value_length_left}}"
        right = f"{header_mapping[key2]:<{max_key_length_right}} : {value2}"
        lines.append(f"{left}  |  {right}")
    
    # Add any remaining items from the right column if odd number of items
    if len(right_column) > len(left_column):
        key, value = right_column[-1]
        lines.append(f"{'':<{max_key_length_left + max_value_length_left + 3}}  |  {header_mapping[key]:<{max_key_length_right}} : {value}")
    
    return "\n".join(lines)

def append_transaction(csv_path: str, transaction: Dict[str, str]):
    fieldnames = [
        "Acct", "Ticker", "Currency", "Margin %", "Date", "Trans Type",
        "Shares", "Strike/Price", "Expiry", "Net Gains", "Notes"
    ]

    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if csvfile.tell() == 0:
            writer.writeheader()
        
        writer.writerow(transaction)

    print(f"Transaction appended to {csv_path}")