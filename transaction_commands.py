import csv
import os
from datetime import datetime
from tabulate import tabulate

def append_transaction(csv_path, transaction_data):
    """
    Append a new transaction to the CSV file.
    
    :param csv_path: Path to the CSV file
    :param transaction_data: Dictionary containing transaction details
    """
    headers = ["Acct", "Ticker", "Currency", "Margin %", "Date", "Trans Type", 
               "Shares", "Strike/Price", "Expiry", "Net Gains", "Notes"]
    
    # Check if file exists, if not create it with headers
    file_exists = os.path.isfile(csv_path)
    
    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(transaction_data)

def display_last_trades(csv_path, num_trades=3):
    """
    Display the last 3 trades in a table format suitable for Discord.
    
    :param csv_path: Path to the CSV file
    :param num_trades: Number of trades to display (default 3)
    :return: Formatted string with table and notes
    """
    if not os.path.exists(csv_path):
        return "CSV file not found."

    with open(csv_path, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        if not fieldnames:
            return "CSV file is empty or has no headers."
        
        trades = list(reader)[-num_trades:]

    if not trades:
        return "No trades found."

    # Prepare data for table
    display_fields = [field for field in fieldnames if field != 'Notes']
    
    # Create table
    table = "```\n"
    table += f"{'Variable':<15}" + "".join(f"Trade {i+1:<15}" for i in range(len(trades))) + "\n"
    table += "-" * (15 * (len(trades) + 1)) + "\n"
    
    for field in display_fields:
        table += f"{field:<15}" + "".join(f"{trade.get(field, 'N/A'):<15}" for trade in trades) + "\n"
    
    table += "```"
    
    # Add notes
    notes = "\nNotes:\n" + "\n".join(f"Trade {i+1}: {trade.get('Notes', 'N/A')}" for i, trade in enumerate(trades))
    
    return table + notes