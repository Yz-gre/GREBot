import discord
import csv
import asyncio
import logging
import configparser
import os
from discord import app_commands, ui
from discord.ui import Select, View, Modal, TextInput
from transaction_data import TransactionData
from typing import Dict, List, Any, Union
from datetime import datetime

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_number(value: str) -> float:
    try:
        return float(value.replace(',', '').replace('$', '').strip())
    except ValueError:
        print(f"Warning: Could not parse '{value}' as a number. Using 0.")
        return 0

def format_number(value: Union[str, int, float], is_currency: bool = False) -> str:
    try:
        # If value is a string, try to convert it to float
        if isinstance(value, str):
            value = float(value.replace(',', '').replace('$', '').strip())
        
        # Now value should be either int or float
        if is_currency:
            return f"${value:,.2f}"
        elif isinstance(value, int) or value.is_integer():
            return f"{int(value)}"
        else:
            return f"{value:.2f}"
    except ValueError:
        # If conversion fails, return the original string
        return str(value)
    
# Base TradeHandler class
class TradeHandler:
    def __init__(self, mz_data: TransactionData):
        self.mz_data = mz_data

    async def handle(self, interaction: discord.Interaction):
        options = await self.get_options()
        await self.show_selection(interaction, options)

    async def get_options(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement get_options method")

    async def show_selection(self, interaction: discord.Interaction, options: List[Dict[str, Any]]):
        select = discord.ui.Select(
            placeholder="Choose an option",
            options=[discord.SelectOption(label=option['label'], value=str(i)) for i, option in enumerate(options)]
        )

        async def select_callback(select_interaction: discord.Interaction):
            option_id = int(select_interaction.data["values"][0])
            selected_option = options[option_id]
            await self.show_form(select_interaction, selected_option)

        select.callback = select_callback
        view = discord.ui.View()
        view.add_item(select)
        await interaction.followup.send("Please select an option:", view=view)

    async def show_form(self, interaction: discord.Interaction, selected_option: Dict[str, Any]):
        raise NotImplementedError("Subclasses must implement show_form method")

    async def process_form(self, interaction: discord.Interaction, form_data: Dict[str, Any]):
        raise NotImplementedError("Subclasses must implement process_form method")

# ClosePositionHandler class and Modal
class ClosePositionHandler(TradeHandler):
    async def get_options(self) -> List[Dict[str, Any]]:
        positions = get_outstanding_positions(self.mz_data)
        return [{
            'label': f"{pos['Ticker']} ({pos['Type']}) @ {pos['Strike/Price']} on {pos['Expiry']} x {abs(pos['Shares'])}",
            'value': pos
        } for pos in positions]

    async def show_form(self, interaction: discord.Interaction, selected_option: Dict[str, Any]):
        modal = ClosePositionModal(selected_option['value'])
        await interaction.response.send_modal(modal)

    async def process_form(self, interaction: discord.Interaction, form_data: Dict[str, Any]):
        position = form_data['position']
        date = datetime.strptime(form_data['date'], '%Y-%m-%d')
        shares = int(form_data['shares'])
        closing_value = float(form_data['closing_value'])
        notes = form_data['notes']

        if shares > abs(position['Shares']):
            raise ValueError(f"Cannot close more shares than outstanding ({abs(position['Shares'])})")

        if position['Type'] == 'Stk':
            transactions = create_stock_closing_transactions(position, date, shares, closing_value, notes)
        else:
            transactions = [create_option_closing_transaction(position, date, shares, closing_value, notes)]

        return transactions

class ClosePositionModal(discord.ui.Modal):
    def __init__(self, position: Dict[str, Any]):
        super().__init__(title=f"Close Position: {position['Ticker']}")
        self.position = position

        self.add_item(discord.ui.TextInput(
            label="Transaction Date (YYYY-MM-DD)", 
            placeholder="e.g. 2023-07-15",
            default=datetime.now().strftime('%Y-%m-%d')
        ))
        self.add_item(discord.ui.TextInput(
            label=f"Number of Shares (max: {abs(position['Shares'])})",
            placeholder=f"Enter up to {abs(position['Shares'])}"
        ))
        
        if position['Type'] == 'Stk':
            self.add_item(discord.ui.TextInput(label="Closing Gains", placeholder="e.g. 500.25"))
        else:
            self.add_item(discord.ui.TextInput(label="Closing Costs", placeholder="e.g. 50.25"))
        
        self.add_item(discord.ui.TextInput(label="Notes", style=discord.TextStyle.long, required=False))

    async def on_submit(self, interaction: discord.Interaction):
        form_data = {
            'position': self.position,
            'date': self.children[0].value,
            'shares': self.children[1].value,
            'closing_value': self.children[2].value,
            'notes': self.children[3].value
        }
        handler = ClosePositionHandler(interaction.client.mz_data)
        try:
            transactions = await handler.process_form(interaction, form_data)
            transactions_display = "\n\n".join([format_transaction_display(t) for t in transactions])
            await interaction.response.send_message(
                f"New transaction(s) to be added:\n```\n{transactions_display}\n```\nDo you want to add these transactions?",
                view=ConfirmView(transactions)
            )
        except ValueError as e:
            await interaction.response.send_message(f"Error in input: {str(e)}", ephemeral=True)

# AddTradeHandler class and Modal
class AddTradeHandler(TradeHandler):
    def __init__(self, mz_data: TransactionData, acct: str, ticker: str, shares: int, currency: str):
        super().__init__(mz_data)
        self.acct = acct
        self.ticker = ticker
        self.shares = shares
        self.currency = currency

    async def handle(self, interaction: discord.Interaction):
        await self.show_trade_type_selection(interaction)

    async def show_trade_type_selection(self, interaction: discord.Interaction):
        select = Select(
            placeholder="Choose trade type",
            options=[
                discord.SelectOption(label="Put Option", value="put"),
                discord.SelectOption(label="Stock Purchase", value="stock")
            ]
        )

        async def select_callback(select_interaction: discord.Interaction):
            trade_type = select_interaction.data["values"][0]
            if trade_type == "put":
                await select_interaction.response.send_modal(PutOptionModal(self))
            else:
                await select_interaction.response.send_modal(StockPurchaseModal(self))

        select.callback = select_callback
        view = View()
        view.add_item(select)
        
        # Use followup instead of response
        await interaction.followup.send("Please select the type of trade:", view=view)

    def create_put_option_transaction(self, form_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'Acct': self.acct,
            'Ticker': self.ticker,
            'Currency': self.currency,
            'Margin %': f"{form_data['margin']:.1f}",  # Format as 0.3, not 30.0%
            'Date': form_data['date'],
            'Trans Type': 'Put',
            'Shares': format_number(self.shares),
            'Strike/Price': format_number(form_data['strike']),
            'Expiry': form_data['expiry'],
            'Net Gains': f"{form_data['net_gains']:,.2f}",  # Format as 4,000.00, not $4,000.00
            'Notes': ''
        }

    def create_stock_purchase_transaction(self, form_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'Acct': self.acct,
            'Ticker': self.ticker,
            'Currency': self.currency,
            'Margin %': f"{form_data['margin']:.1f}",  # Format as 0.3, not 30.0%
            'Date': form_data['date'],
            'Trans Type': 'Stk',
            'Shares': format_number(self.shares),
            'Strike/Price': '0',
            'Expiry': '9999-12-31',
            'Net Gains': f"{-form_data['net_purchase_cost']:,.2f}",  # Format as -4,000.00, not $-4,000.00
            'Notes': ''
        }

    async def confirm_transaction(self, interaction: discord.Interaction, transaction: Dict[str, Any]):
        transaction_display = format_transaction_display(transaction)
        await interaction.response.send_message(
            f"New transaction to be added:\n```\n{transaction_display}\n```\nDo you want to add this transaction?",
            view=ConfirmView([transaction])
        )

class PutOptionModal(Modal):
    def __init__(self, handler: AddTradeHandler):
        super().__init__(title="Add Put Option Trade")
        self.handler = handler

        self.add_item(TextInput(
            label="Transaction Date",
            default=datetime.now().strftime('%Y-%m-%d'),
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Margin",
            placeholder="Enter 0, 0.3, 0.5, or 1",
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Strike",
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Expiry Date",
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Net Gains",
            style=discord.TextStyle.short
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            form_data = {
                'date': self.children[0].value,
                'margin': float(self.children[1].value),  # Convert percentage to decimal
                'strike': float(self.children[2].value),
                'expiry': self.children[3].value,
                'net_gains': float(self.children[4].value.replace('$', ''))  # Remove $ and commas
            }
            transaction = self.handler.create_put_option_transaction(form_data)
            await self.handler.confirm_transaction(interaction, transaction)
        except ValueError as e:
            await interaction.response.send_message(f"Invalid input: {str(e)}", ephemeral=True)

class StockPurchaseModal(Modal):
    def __init__(self, handler: AddTradeHandler):
        super().__init__(title="Add Stock Purchase Trade")
        self.handler = handler

        self.add_item(TextInput(
            label="Transaction Date",
            default=datetime.now().strftime('%Y-%m-%d'),
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Margin",
            placeholder="Enter 0, 0.3, 0.5, or 1",
            style=discord.TextStyle.short
        ))
        self.add_item(TextInput(
            label="Net Purchase Cost",
            style=discord.TextStyle.short
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            form_data = {
                'date': self.children[0].value,
                'margin': float(self.children[1].value),  # Convert percentage to decimal
                'net_purchase_cost': float(self.children[2].value.replace('$', ''))  # Remove $ and commas
            }
            transaction = self.handler.create_stock_purchase_transaction(form_data)
            await self.handler.confirm_transaction(interaction, transaction)
        except ValueError as e:
            await interaction.response.send_message(f"Invalid input: {str(e)}", ephemeral=True)

# LastTradeHandler
class LastTradeHandler(TradeHandler):
    async def handle(self, interaction: discord.Interaction):
        last_trade = self.get_last_trade()
        if last_trade:
            transaction_display = format_transaction_display(last_trade)
            content = f"Last trade:\n```\n{transaction_display}\n```"
        else:
            content = "No trades found."
        
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content)
            else:
                await interaction.response.send_message(content)
        except discord.errors.NotFound:
            logging.error("Interaction not found. It may have timed out.")
        except Exception as e:
            logging.error(f"Error in LastTradeHandler: {str(e)}")

    def get_last_trade(self) -> Dict[str, Any]:
        try:
            with open(self.mz_data.csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                trades = list(reader)
                return trades[-1] if trades else None
        except Exception as e:
            logging.error(f"Error reading CSV file: {str(e)}")
            return None

# DeleteLastTradeHandler Class and Modal        
class DeleteLastTradeHandler(TradeHandler):
    async def handle(self, interaction: discord.Interaction):
        last_trade = self.get_last_trade()
        if last_trade:
            transaction_display = format_transaction_display(last_trade)
            content = f"Last trade to be deleted:\n```\n{transaction_display}\n```\nDo you want to delete this trade?"
            view = DeleteConfirmView(self.mz_data.csv_path, last_trade)
        else:
            content = "No trades found to delete."
            view = None
        
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content, view=view)
            else:
                await interaction.response.send_message(content, view=view)
        except discord.errors.NotFound:
            logging.error("Interaction not found. It may have timed out.")
        except Exception as e:
            logging.error(f"Error in DeleteLastTradeHandler: {str(e)}")

    def get_last_trade(self) -> Dict[str, Any]:
        try:
            with open(self.mz_data.csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                trades = list(reader)
                return trades[-1] if trades else None
        except Exception as e:
            logging.error(f"Error reading CSV file: {str(e)}")
            return None

class DeleteConfirmView(discord.ui.View):
    def __init__(self, csv_path: str, last_trade: Dict[str, Any]):
        super().__init__()
        self.csv_path = csv_path
        self.last_trade = last_trade

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.delete_last_trade()
            await interaction.response.send_message("Last trade deleted successfully.")
        except Exception as e:
            logging.error(f"Error in delete confirmation: {str(e)}")
            await interaction.response.send_message("An error occurred while deleting the trade.")
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Deletion cancelled.")
        self.stop()

    def delete_last_trade(self):
        try:
            with open(self.csv_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                trades = list(reader)
                trades.pop()  # Remove the last trade

            with open(self.csv_path, 'w', newline='') as csvfile:
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(trades)
        except Exception as e:
            logging.error(f"Error deleting last trade: {str(e)}")
            raise

# CoveredCallHandler Class and Modal
class CovCallHandler(TradeHandler):
    async def get_options(self) -> List[Dict[str, Any]]:
        positions = get_outstanding_positions(self.mz_data)
        return [{
            'label': f"{pos['Ticker']} @ {pos['Strike/Price']} x {abs(pos['Shares'])}",
            'value': pos
        } for pos in positions if pos['Type'] == 'Stk']

    async def show_form(self, interaction: discord.Interaction, selected_option: Dict[str, Any]):
        modal = CovCallModal(selected_option['value'])
        await interaction.response.send_modal(modal)

    async def process_form(self, interaction: discord.Interaction, form_data: Dict[str, Any]):
        position = form_data['position']
        transaction = {
            'Acct': position['Acct'],
            'Ticker': position['Ticker'],
            'Currency': position['Currency'],
            'Margin %': position['Margin %'],
            'Date': form_data['date'],
            'Trans Type': 'Int / Tax',
            'Shares': format_number(form_data['shares']),
            'Strike/Price': format_number(form_data['strike']),
            'Expiry': form_data['expiry'],
            'Net Gains': format_number(form_data['net_gains']),
            'Notes': 'Selling Covered Calls'
        }
        return [transaction]

class CovCallModal(discord.ui.Modal):
    def __init__(self, position: Dict[str, Any]):
        super().__init__(title=f"Covered Call: {position['Ticker']}")
        self.position = position

        self.add_item(discord.ui.TextInput(
            label="Transaction Date (YYYY-MM-DD)", 
            default=datetime.now().strftime('%Y-%m-%d')
        ))
        self.add_item(discord.ui.TextInput(
            label=f"Number of Shares (max: {abs(position['Shares'])})",
            placeholder=f"Enter up to {abs(position['Shares'])}"
        ))
        self.add_item(discord.ui.TextInput(
            label="Strike Price",
            placeholder="e.g. 50.00"
        ))
        self.add_item(discord.ui.TextInput(
            label="Expiration Date (YYYY-MM-DD)",
            placeholder="e.g. 2023-12-15"
        ))
        self.add_item(discord.ui.TextInput(
            label="Net Gains",
            placeholder="e.g. 500.25"
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            form_data = {
                'position': self.position,
                'date': self.children[0].value,
                'shares': int(self.children[1].value),
                'strike': float(self.children[2].value),
                'expiry': self.children[3].value,
                'net_gains': float(self.children[4].value.replace('$', '').replace(',', ''))
            }
            
            if form_data['shares'] > abs(self.position['Shares']):
                raise ValueError(f"Cannot sell more calls than shares owned ({abs(self.position['Shares'])})")
            
            handler = CovCallHandler(interaction.client.mz_data)
            transactions = await handler.process_form(interaction, form_data)
            transactions_display = "\n\n".join([format_transaction_display(t) for t in transactions])
            await interaction.response.send_message(
                f"New transaction(s) to be added:\n```\n{transactions_display}\n```\nDo you want to add these transactions?",
                view=ConfirmView(transactions)
            )
        except ValueError as e:
            await interaction.response.send_message(f"Error in input: {str(e)}", ephemeral=True)

# AssignedHandler Class and Modal
class AssignedHandler(TradeHandler):
    async def get_options(self) -> List[Dict[str, Any]]:
        positions = get_outstanding_positions(self.mz_data)
        return [{
            'label': f"{pos['Ticker']} ({pos['Type']}) @ {pos['Strike/Price']} on {pos['Expiry']} x {abs(pos['Shares'])}",
            'value': pos
        } for pos in positions if pos['Type'] in ['Put', 'Call']]

    async def show_form(self, interaction: discord.Interaction, selected_option: Dict[str, Any]):
        modal = AssignedModal(selected_option['value'])
        await interaction.response.send_modal(modal)

    async def process_form(self, interaction: discord.Interaction, form_data: Dict[str, Any]):
        position = form_data['position']
        
        # First row: Close the option position
        option_close_transaction = {
            'Acct': position['Acct'],
            'Ticker': position['Ticker'],
            'Currency': position['Currency'],
            'Margin %': position['Margin %'],
            'Date': form_data['date'],
            'Trans Type': position['Type'],
            'Shares': format_number(-form_data['shares']),
            'Strike/Price': position['Strike/Price'],
            'Expiry': position['Expiry'],
            'Net Gains': '0',
            'Notes': 'Option assigned'
        }
        
        # Second row: New stock position
        stock_transaction = {
            'Acct': position['Acct'],
            'Ticker': position['Ticker'],
            'Currency': position['Currency'],
            'Margin %': position['Margin %'],
            'Date': form_data['date'],
            'Trans Type': 'Stk',
            'Shares': format_number(form_data['shares']),
            'Strike/Price': '0',
            'Expiry': '9999-12-31',
            'Net Gains': format_number(-form_data['purchase_cost']),
            'Notes': 'Stock from assigned option'
        }
        
        return [option_close_transaction, stock_transaction]

class AssignedModal(discord.ui.Modal):
    def __init__(self, position: Dict[str, Any]):
        super().__init__(title=f"Assigned: {position['Ticker']}")
        self.position = position

        self.add_item(discord.ui.TextInput(
            label="Transaction Date (YYYY-MM-DD)", 
            default=datetime.now().strftime('%Y-%m-%d')
        ))
        self.add_item(discord.ui.TextInput(
            label=f"Number of Shares (max: {abs(position['Shares'])})",
            placeholder=f"Enter up to {abs(position['Shares'])}"
        ))
        self.add_item(discord.ui.TextInput(
            label="Purchase Cost",
            placeholder="e.g. 5000.00"
        ))

    async def on_submit(self, interaction: discord.Interaction):
        try:
            form_data = {
                'position': self.position,
                'date': self.children[0].value,
                'shares': int(self.children[1].value),
                'purchase_cost': float(self.children[2].value.replace('$', '').replace(',', ''))
            }
            
            if form_data['shares'] > abs(self.position['Shares']):
                raise ValueError(f"Cannot assign more shares than the option position ({abs(self.position['Shares'])})")
            
            handler = AssignedHandler(interaction.client.mz_data)
            transactions = await handler.process_form(interaction, form_data)
            transactions_display = "\n\n".join([format_transaction_display(t) for t in transactions])
            await interaction.response.send_message(
                f"New transaction(s) to be added:\n```\n{transactions_display}\n```\nDo you want to add these transactions?",
                view=ConfirmView(transactions)
            )
        except ValueError as e:
            await interaction.response.send_message(f"Error in input: {str(e)}", ephemeral=True)

# CashInOutHandler Class and Modal
class CashInOutHandler(TradeHandler):
    async def handle(self, interaction: discord.Interaction):
        await self.show_form(interaction)

    async def show_form(self, interaction: discord.Interaction):
        modal = CashInOutModal(self.mz_data)
        await interaction.response.send_modal(modal)

    async def process_form(self, interaction: discord.Interaction, form_data: Dict[str, Any]):
        # Validate account
        if form_data['account'] not in self.mz_data.get_accounts():
            raise ValueError(f"Invalid account: {form_data['account']}")

        # Validate currency
        if form_data['currency'] not in ['CAD', 'USD']:
            raise ValueError(f"Invalid currency: {form_data['currency']}")

        # Validate permanent investment
        is_permanent = form_data['is_permanent'].lower() in ['yes', 'y', 'true', '1']

        transaction = {
            'Acct': form_data['account'],
            'Ticker': 'GREINV',
            'Currency': form_data['currency'],
            'Margin %': '0',
            'Date': form_data['date'],
            'Trans Type': 'Cash',
            'Shares': '0',
            'Strike/Price': '0',
            'Expiry': '9999-12-31',
            'Net Gains': format_number(form_data['amount'], is_currency=True),
            'Notes': 'Invest - Pre Convert' if is_permanent else 'Margin Cover'
        }
        return [transaction]

class CashInOutModal(discord.ui.Modal):
    def __init__(self, mz_data: TransactionData):
        super().__init__(title="Cash In/Out Transaction")
        self.mz_data = mz_data

        self.add_item(discord.ui.TextInput(
            label="Transaction Date (YYYY-MM-DD)",
            default=datetime.now().strftime('%Y-%m-%d')
        ))

        default_account = self.get_last_account()
        self.add_item(discord.ui.TextInput(
            label="Account",
            placeholder="Enter account name",
            default=default_account or ""
        ))

        self.add_item(discord.ui.TextInput(
            label="Currency (CAD or USD)",
            default="CAD"
        ))

        self.add_item(discord.ui.TextInput(
            label="Permanent Investment? (Yes/No)",
            default="No"
        ))

        self.add_item(discord.ui.TextInput(
            label="Cash Amount (negative for withdrawal)",
            placeholder="e.g. 1000.00 or -500.00"
        ))

    def get_last_account(self):
        try:
            with open(self.mz_data.csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                last_row = None
                for row in reader:
                    last_row = row
                return last_row['Acct'] if last_row else None
        except Exception as e:
            logging.error(f"Error reading CSV file: {str(e)}")
            return None

    async def on_submit(self, interaction: discord.Interaction):
        try:
            form_data = {
                'date': self.children[0].value,
                'account': self.children[1].value,
                'currency': self.children[2].value.upper(),
                'is_permanent': self.children[3].value,
                'amount': float(self.children[4].value.replace('$', '').replace(',', ''))
            }
            
            handler = CashInOutHandler(interaction.client.mz_data)
            transactions = await handler.process_form(interaction, form_data)
            transactions_display = "\n\n".join([format_transaction_display(t) for t in transactions])
            await interaction.response.send_message(
                f"New transaction(s) to be added:\n```\n{transactions_display}\n```\nDo you want to add these transactions?",
                view=ConfirmView(transactions)
            )
        except ValueError as e:
            await interaction.response.send_message(f"Error in input: {str(e)}", ephemeral=True)

# ConfirmView class
class ConfirmView(discord.ui.View):
    def __init__(self, transactions: List[Dict[str, Any]]):
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

def get_outstanding_positions(mz_data: TransactionData) -> List[Dict[str, Any]]:
    outstanding_positions = []
    
    for key, value in mz_data.daily_balances['opt_positions'].items():
        if value[max(value.keys())] != 0:
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
    
    for key, value in mz_data.daily_balances['stk_shares'].items():
        if value[max(value.keys())] != 0:
            notional_key = key
            if notional_key in mz_data.daily_balances['stk_notional']:
                notional_value = mz_data.daily_balances['stk_notional'][notional_key]
                latest_notional = notional_value[max(notional_value.keys())]
                latest_notional = parse_number(str(latest_notional))
            else:
                latest_notional = 0

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

def create_option_closing_transaction(position: Dict[str, Any], date: datetime, shares: int, closing_costs: float, notes: str) -> Dict[str, Any]:
    return {
        'Acct': position['Acct'],
        'Ticker': position['Ticker'],
        'Currency': position['Currency'],
        'Margin %': position['Margin %'],
        'Date': date.strftime('%Y-%m-%d'),
        'Trans Type': position['Type'],
        'Shares': format_number(-shares),
        'Strike/Price': format_number(position['Strike/Price']),
        'Expiry': position['Expiry'],
        'Net Gains': format_number(-closing_costs, is_currency=True),
        'Notes': notes
    }

def create_stock_closing_transactions(position: Dict[str, Any], date: datetime, shares: int, closing_gains: float, notes: str) -> List[Dict[str, Any]]:
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
        'Shares': format_number(-shares),
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

def format_transaction_display(transactions: Union[Dict[str, Any], List[Dict[str, Any]]]) -> str:
    if isinstance(transactions, dict):
        transactions = [transactions]
    
    formatted_transactions = []
    
    for transaction in transactions:
        header_mapping = {
            "Acct": "Account", "Ticker": "Ticker", "Currency": "Currency",
            "Margin %": "Margin %", "Date": "Date", "Trans Type": "Transaction Type",
            "Shares": "Shares", "Strike/Price": "Strike/Price", "Expiry": "Expiry",
            "Net Gains": "Net Gains", "Notes": "Notes"
        }
        
        # Handle potential BOM in the first key
        first_key = next(iter(transaction.keys()))
        if first_key.startswith('\ufeff'):
            transaction[first_key[1:]] = transaction.pop(first_key)
        
        lines = []
        max_key_length = max(len(header_mapping.get(key, key)) for key in transaction.keys())
        
        for key, value in transaction.items():
            formatted_key = header_mapping.get(key, key)
            if key in ['Net Gains', 'Strike/Price']:
                try:
                    numeric_value = float(value.replace('$', '').replace(',', ''))
                    formatted_value = f"${numeric_value:,.2f}"
                except ValueError:
                    formatted_value = value
            else:
                formatted_value = value
            
            lines.append(f"{formatted_key:<{max_key_length}} : {formatted_value}")
        
        formatted_transactions.append("\n".join(lines))
    
    return "\n\n".join(formatted_transactions)

def append_transaction(csv_path: str, transaction: Dict[str, Any]):
    fieldnames = [
        "Acct", "Ticker", "Currency", "Margin %", "Date", "Trans Type",
        "Shares", "Strike/Price", "Expiry", "Net Gains", "Notes"
    ]

    with open(csv_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if csvfile.tell() == 0:
            writer.writeheader()
        
        writer.writerow(transaction)

    print(f"Transaction appended to {csv_path}")

async def process_add_trade(interaction: discord.Interaction, mz_data: TransactionData, acct: str, ticker: str, shares: int, currency: str):
    handler = AddTradeHandler(mz_data, acct, ticker, shares, currency)
    await interaction.response.defer(thinking=True)
    await handler.handle(interaction)

async def process_close_position(interaction: discord.Interaction, mz_data: TransactionData):
    handler = ClosePositionHandler(mz_data)
    await interaction.response.defer(thinking=True)
    await handler.handle(interaction)

async def process_cov_call(interaction: discord.Interaction, mz_data: TransactionData):
    handler = CovCallHandler(mz_data)
    await interaction.response.defer(thinking=True)
    await handler.handle(interaction)

async def process_assigned(interaction: discord.Interaction, mz_data: TransactionData):
    handler = AssignedHandler(mz_data)
    await interaction.response.defer(thinking=True)
    await handler.handle(interaction)