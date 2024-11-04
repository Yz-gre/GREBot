from tabulate import tabulate
from datetime import datetime, timedelta
import logging
from typing import Dict, Tuple, List
from transaction_data import TransactionData
from collections import defaultdict
import math
from yahooquery_tester import get_stock_last_price, get_option_values, get_fx_rate

# Configuration
FX_RATE, FX_ERROR = get_fx_rate('USDCAD')

# Helper functions
def format_number(num):
    return f"{num/1000:.1f}"

def safe_float(value):
    try:
        return float(value) * 1000
    except ValueError:
        return 0.0

def format_value(value, is_float=True, decimal_places=1, width=8):
    if isinstance(value, (int, float)):
        if is_float:
            return f"{value:.{decimal_places}f}".rjust(width)
        return f"{int(value)}".rjust(width)
    return str(value).ljust(width)

def adjust_cad_to_usd(cad_value):
    return cad_value / FX_RATE if FX_RATE else cad_value

def revenue_command(transaction_data: TransactionData) -> str:
    print("Starting revenue_command")
    current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
    print(f"Current date: {current_date}")
    
    periods = {
        'MTD': (current_date.replace(day=1) - timedelta(days=1), current_date),
        'YTD': (current_date.replace(month=1, day=1) - timedelta(days=1), current_date),
        'L3M': (current_date - timedelta(days=90), current_date),
        'L6M': (current_date - timedelta(days=180), current_date),
        'L12M': (current_date - timedelta(days=365), current_date),
        'Lifetime': (transaction_data.first_transaction_date, current_date)
    }
    print(f"Periods: {periods}")

    accounts = transaction_data.get_accounts()
    accounts.sort()
    print(f"Accounts: {accounts}")

    headers = ['Period', 'Total'] + accounts
    print(f"Headers: {headers}")
    
    rows = []
    for period_name, (start_date, end_date) in periods.items():
        print(f"\nProcessing period: {period_name}, Start: {start_date}, End: {end_date}")
        row = [period_name]
        total_revenue_usd = 0
        for account in accounts:
            print(f"  Processing account: {account}")
            _, start_breakdown = transaction_data.get_spot_balance(start_date, 'revenue', account=account)
            _, end_breakdown = transaction_data.get_spot_balance(end_date, 'revenue', account=account)
            print(f"    Start breakdown: {start_breakdown}")
            print(f"    End breakdown: {end_breakdown}")
            
            revenue_usd = 0
            for key in set(end_breakdown) | set(start_breakdown):
                currency = key[1]
                revenue = end_breakdown.get(key, 0) - start_breakdown.get(key, 0)
                print(f"      Key: {key}, Currency: {currency}, Revenue: {revenue}")
                if currency == 'CAD':
                    revenue = adjust_cad_to_usd(revenue)
                    print(f"      Adjusted CAD revenue: {revenue}")
                revenue_usd += revenue
            print(f"    Total revenue for {account}: {revenue_usd}")

            total_revenue_usd += revenue_usd
            row.append(format_number(revenue_usd))
        print(f"  Total revenue for period: {total_revenue_usd}")
        row.insert(1, format_number(total_revenue_usd))
        rows.append(row)
    
    print(f"Final rows: {rows}")

    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("Final table:")
    print(table)
    return table

def investments_command(transaction_data: TransactionData) -> str:
    print("Starting investments_command")
    current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
    print(f"Current date: {current_date}")
    
    accounts = transaction_data.get_accounts()
    currencies = ['CAD', 'USD']
    
    accounts.sort()
    print(f"Accounts: {accounts}")
    print(f"Currencies: {currencies}")

    headers = ['CCY/Type', 'Total'] + accounts
    print(f"Headers: {headers}")

    rows = []
    usd_equivalent_totals = {'Regular': 0, 'Temp': 0}

    for currency in currencies:
        print(f"\nProcessing currency: {currency}")
        rows.append([f"{currency}", "", ""])
        currency_totals = [0] * (len(accounts) + 1)
        for investment_type in ['Regular', 'Temp']:
            print(f"  Processing investment type: {investment_type}")
            row = [f"  {investment_type}"]
            for i, account in enumerate(['Total'] + accounts):
                print(f"    Processing account: {account}")
                try:
                    if account == 'Total':
                        balance = sum(transaction_data.get_spot_balance(
                            current_date, 'investments', account=acc, 
                            currency=currency, investment_type=investment_type.lower()
                        )[0] for acc in accounts)
                    else:
                        balance, _ = transaction_data.get_spot_balance(
                            current_date, 'investments', account=account, 
                            currency=currency, investment_type=investment_type.lower()
                        )
                    print(f"      Balance: {balance}")
                except Exception as e:
                    print(f"Error getting balance for {account}, {currency}, {investment_type}: {str(e)}")
                    balance = 0
                row.append(format_number(balance))
                currency_totals[i] += balance
                
                if account == 'Total':
                    usd_value = balance if currency == 'USD' else adjust_cad_to_usd(balance)
                    usd_equivalent_totals[investment_type] += usd_value
                    print(f"      USD equivalent: {usd_value}")
            rows.append(row)
        
        total_row = [f"  Total"] + [format_number(total) for total in currency_totals]
        rows.append(total_row)
        rows.append([])
        print(f"  Currency totals: {currency_totals}")

    if rows and not any(rows[-1]):
        rows.pop()
    
    print(f"USD equivalent totals: {usd_equivalent_totals}")
    print(f"Final rows: {rows}")
    
    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("Final table:")
    print(table)
    return table

def notional_command(transaction_data: TransactionData) -> str:
    print("Starting notional_command")
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Current date: {current_date}")
    
    accounts = transaction_data.get_accounts()
    currencies = ['USD', 'CAD']
    print(f"Accounts: {accounts}")
    print(f"Currencies: {currencies}")
    
    headers = ['', 'Total'] + accounts
    print(f"Headers: {headers}")
    rows = []

    def get_notional(balance_type, account=None, currency=None):
        print(f"Getting notional for {balance_type}, {account}, {currency}")
        try:
            balance, breakdown = transaction_data.get_spot_balance(
                current_date, balance_type, account=account, currency=currency
            )
            print(f"  Notional balance: {balance}")
            print("  Breakdown of positions:")
            for key, value in breakdown.items():
                if balance_type == 'stk_notional':
                    ticker = key[3]
                    print(f"    {ticker}: {value}")
                elif balance_type == 'opt_notional':
                    ticker, option_type, strike, expiry = key[3:7]
                    print(f"    {ticker} {option_type} {strike} {expiry}: {value}")
            return balance, breakdown
        except Exception as e:
            logging.error(f"Error getting notional for {balance_type}, {account}, {currency}: {str(e)}")
            return 0, {}

    total_usd_equivalent = 0
    for currency in currencies:
        print(f"\nProcessing currency: {currency}")
        rows.append([currency] + [''] * len(headers[1:]))
        currency_total = 0
        for asset_type in ['Stocks', 'Options']:
            print(f"  Processing asset type: {asset_type}")
            row = [f'  {asset_type}']
            for account in ['Total'] + accounts:
                print(f"    Processing account: {account}")
                if account == 'Total':
                    balance = 0
                    all_positions = {}
                    for acc in accounts:
                        acc_balance, acc_breakdown = get_notional(
                            'stk_notional' if asset_type == 'Stocks' else 'opt_notional',
                            acc, currency)
                        balance += acc_balance
                        for key, value in acc_breakdown.items():
                            all_positions[key] = all_positions.get(key, 0) + value
                    print("    Total positions across all accounts:")
                    for key, value in all_positions.items():
                        if asset_type == 'Stocks':
                            ticker = key[3]
                            print(f"      {ticker}: {value}")
                        else:
                            ticker, option_type, strike, expiry = key[3:7]
                            print(f"      {ticker} {option_type} {strike} {expiry}: {value}")
                else:
                    balance, _ = get_notional(
                        'stk_notional' if asset_type == 'Stocks' else 'opt_notional',
                        account, currency)
                print(f"      Balance: {balance}")
                row.append(format_number(balance))
                if account == 'Total':
                    currency_total += balance
            rows.append(row)
        
        total_row = ['  Total']
        for i in range(1, len(headers)):
            total = sum(safe_float(row[i]) for row in rows[-2:] if len(row) > i)
            total_row.append(format_number(total))
        rows.append(total_row)
        rows.append([''] * len(headers))
        print(f"  Currency total: {currency_total}")

        total_usd_equivalent += currency_total if currency == 'USD' else adjust_cad_to_usd(currency_total)
        print(f"  Total USD equivalent: {total_usd_equivalent}")

    if rows and not any(rows[-1]):
        rows.pop()

    rows.append([''] * len(headers))
    total_row = ['Total(US)']
    total_row.append(format_number(total_usd_equivalent))
    total_row.extend([''] * (len(headers) - 2))
    rows.append(total_row)
    rows.append([''] * len(headers))

    print(f"Final rows: {rows}")

    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("Final table:")
    print(table)
    return table

def bp_command(transaction_data: TransactionData) -> str:
    print("Starting bp_command")
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Current date: {current_date}")
    
    accounts = transaction_data.get_accounts()
    currencies = ['USD', 'CAD']
    print(f"Accounts: {accounts}")
    print(f"Currencies: {currencies}")
    
    headers = ['', 'Total'] + accounts
    print(f"Headers: {headers}")
    rows = []

    def get_balance(balance_type, account=None, currency=None):
        print(f"Getting balance for {balance_type}, account: {account}, currency: {currency}")
        try:
            balance, breakdown = transaction_data.get_spot_balance(
                current_date, balance_type, account=account, currency=currency
            )
            print(f"  Balance: {balance}")
            print("  Breakdown:")
            for key, value in breakdown.items():
                print(f"    {key}: {value}")
            return balance, breakdown
        except Exception as e:
            logging.error(f"Error getting balance for {balance_type}, {account}, {currency}: {str(e)}")
            return 0, {}

    total_usd_equivalent = 0
    for currency in currencies:
        print(f"\nProcessing currency: {currency}")
        rows.append([currency] + [''] * len(headers[1:]))
        currency_total = 0
        for balance_type in ['Cash', 'Stock', 'LOC']:
            print(f"  Processing balance type: {balance_type}")
            row = [f'  {balance_type}']
            for account in ['Total'] + accounts:
                print(f"    Processing account: {account}")
                if balance_type == 'LOC':
                    if account == 'Total' and currency == 'CAD':
                        loc_limit, loc_usage = transaction_data.get_loc_info()
                        balance = loc_limit - loc_usage
                        breakdown = {'LOC': balance}
                    else:
                        balance, breakdown = 0, {}
                elif account == 'Total':
                    balance = 0
                    breakdown = {}
                    for acc in accounts:
                        acc_balance, acc_breakdown = get_balance(
                            'cash_balances' if balance_type == 'Cash' else 'stk_notional',
                            acc, currency)
                        balance += acc_balance
                        for key, value in acc_breakdown.items():
                            breakdown[key] = breakdown.get(key, 0) + value
                else:
                    balance, breakdown = get_balance(
                        'cash_balances' if balance_type == 'Cash' else 'stk_notional',
                        account, currency)
                
                print(f"      Balance: {balance}")
                print("      Breakdown:")
                for key, value in breakdown.items():
                    print(f"        {key}: {value}")
                
                row.append(format_number(balance))
                if account == 'Total':
                    currency_total += balance
            rows.append(row)
        
        total_row = ['  Total']
        for i in range(1, len(headers)):
            total = sum(safe_float(row[i]) for row in rows[-3:] if len(row) > i)
            total_row.append(format_number(total))
        rows.append(total_row)
        rows.append([''] * len(headers))
        print(f"  Currency total: {currency_total}")

        total_usd_equivalent += currency_total if currency == 'USD' else adjust_cad_to_usd(currency_total)
        print(f"  Total USD equivalent so far: {total_usd_equivalent}")

    if rows and not any(rows[-1]):
        rows.pop()

    rows.append([''] * len(headers))
    total_row = ['Total(US)']
    total_row.append(format_number(total_usd_equivalent))
    total_row.extend([''] * (len(headers) - 2))
    rows.append(total_row)
    rows.append([''] * len(headers))
    
    print(f"Final rows: {rows}")

    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("Final table:")
    print(table)
    return table

def positions_command(transaction_data: TransactionData) -> str:
    print("Starting positions_command")
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Current date: {current_date}")
    
    print("Getting stock shares breakdown")
    _, stock_breakdown = transaction_data.get_spot_balance(current_date, 'stk_shares')
    print(f"Stock shares breakdown: {stock_breakdown}")
    
    print("Getting stock notional breakdown")
    _, stock_notional = transaction_data.get_spot_balance(current_date, 'stk_notional')
    print(f"Stock notional breakdown: {stock_notional}")
    
    print("Getting option positions breakdown")
    _, option_breakdown = transaction_data.get_spot_balance(current_date, 'opt_positions')
    print(f"Option positions breakdown: {option_breakdown}")
    
    all_tickers = set()
    all_options = []
    
    stock_positions = []
    option_positions = defaultdict(list)
    total_notional = 0

    print("\nProcessing stock positions")
    for key, shares in stock_breakdown.items():
        print(f"Processing stock: {key}")
        if shares != 0:
            account, currency, margin, ticker = key
            notional = stock_notional.get(key, 0)
            notional_usd = adjust_cad_to_usd(notional) if currency == 'CAD' else notional
            all_tickers.add(ticker)
            position = {
                'Description': f"{ticker} x {abs(shares/100):.0f}",
                'Notional': notional_usd / 1000,
                'Shares': shares,
                'Ticker': ticker,
                'Margin': float(margin.strip('%')) / 100,
                'Currency': currency
            }
            stock_positions.append(position)
            total_notional += notional_usd
            print(f"Added stock position: {position}")
    print(f"Total stock notional: {total_notional}")

    print("\nProcessing option positions")
    for key, shares in option_breakdown.items():
        print(f"Processing option: {key}")
        if shares != 0:
            account, currency, margin, ticker, option_type, strike, expiry = key
            strike = float(strike)
            expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
            notional = abs(shares * strike)
            notional_usd = adjust_cad_to_usd(notional) if currency == 'CAD' else notional
            strike_str = f"{strike:.0f}" if strike.is_integer() else f"{strike:.1f}"
            all_tickers.add(ticker)
            option_info = {
                'ticker': ticker,
                'expiration': expiry,
                'option_type': option_type,
                'strike': strike
            }
            all_options.append(option_info)
            position = {
                'Description': f"{ticker} @ {strike_str} x {abs(shares/100):.0f}",
                'Notional': notional_usd / 1000,
                'Expiry': expiry_date,
                'Shares': shares,
                'Ticker': ticker,
                'Strike': strike,
                'Option_Type': option_type,
                'Currency': currency,
                'Margin': float(margin.strip('%')) / 100  # Add margin to the position dictionary
            }
            option_positions[expiry_date.strftime('%Y-%m')].append(position)
            total_notional += notional_usd
            print(f"Added option position: {position}")
            print(f"Added option info: {option_info}")
    print(f"Total notional (stocks + options): {total_notional}")

    print("\nGetting stock prices")
    stock_prices = get_stock_last_price(list(all_tickers))
    stock_prices_dict = dict(zip(stock_prices['ticker'], stock_prices['last_price']))
    print(f"Stock prices: {stock_prices_dict}")
    
    print("\nGetting option values")
    option_prices = get_option_values(all_options)
    option_prices_dict = {(row['ticker'], row['expiration'], row['option_type'], row['strike']): row['value'] 
                          for _, row in option_prices.iterrows()}
    print(f"Option prices: {option_prices_dict}")
    
    print("\nGetting FX rate")
    FX_RATE, fx_error = get_fx_rate('USDCAD')
    print(f"FX rate: {FX_RATE}, Error: {fx_error}")
    
    def calculate_stock_bp(pos):
        print(f"\nCalculating BP for stock: {pos['Ticker']}")
        current_price = stock_prices_dict.get(pos['Ticker'], 0)
        print(f"Current price: {current_price}")
        stock_used_bp = pos['Notional'] * 1000 - (pos['Shares'] * current_price) + (pos['Shares'] * current_price * pos['Margin'] * 100)
        print(f"Stock used BP (before FX adjustment): {stock_used_bp}")
        adjusted_bp = stock_used_bp if pos['Currency'] == 'USD' else stock_used_bp / FX_RATE
        print(f"Adjusted BP: {adjusted_bp}")
        return adjusted_bp
    
    def calculate_option_bp(pos):
        print(f"\nCalculating BP for option: {pos['Ticker']} @ {pos['Strike']}")
        current_price = stock_prices_dict.get(pos['Ticker'], 0)
        print(f"Current stock price: {current_price}")
        option_price = option_prices_dict.get((pos['Ticker'], pos['Expiry'].strftime('%Y-%m-%d'), pos['Option_Type'], pos['Strike']), 0)
        print(f"Option price (before adjustment): {option_price}")
        
        option_price = 0 if option_price is None or not isinstance(option_price, (int, float)) or math.isnan(option_price) else option_price
        print(f"Adjusted option price: {option_price}")
        
        stock_component = pos['Margin'] * 100 * current_price  # Use the position's margin value
        option_component = option_price
        strike_diff_component = min(0, pos['Strike'] - current_price)
        
        print(f"Stock component: {stock_component}")
        print(f"Option component: {option_component}")
        print(f"Strike diff component: {strike_diff_component}")
        
        per_share_bp = stock_component + option_component + strike_diff_component
        print(f"Per share BP: {per_share_bp}")
        option_used_bp = abs(pos['Shares']) * per_share_bp
        print(f"Option used BP (before FX adjustment): {option_used_bp}")
        adjusted_bp = option_used_bp if pos['Currency'] == 'USD' else option_used_bp / FX_RATE
        print(f"Adjusted BP: {adjusted_bp}")
        return adjusted_bp
    
    print("\nGenerating output")
    output = ""
    output += f"{'':17}{'Not.':>6}{'%':>6}{'BP':>6}\n"
    output += "-" * 35 + "\n"
    
    if stock_positions:
        print("\nProcessing stock positions for output")
        for pos in sorted(stock_positions, key=lambda x: x['Description']):
            bp = calculate_stock_bp(pos)
            percentage = (pos['Notional'] * 1000 / total_notional) * 100
            line = f"{pos['Description']:<17}{pos['Notional']:>6.1f}{percentage:>6.1f}{bp/1000:>6.1f}\n"
            output += line
            print(f"Added line to output: {line.strip()}")
    
    if option_positions:
        print("\nProcessing option positions for output")
        output += "-" * 35 + "\n"
        for month in sorted(option_positions.keys()):
            print(f"Processing month: {month}")
            output += f"{month}:\n"
            for pos in sorted(option_positions[month], key=lambda x: (x['Expiry'], x['Description'])):
                bp = calculate_option_bp(pos)
                percentage = (pos['Notional'] * 1000 / total_notional) * 100
                new_description = '-' + pos['Description']
                line = f"{new_description:<17}{pos['Notional']:>6.1f}{percentage:>6.1f}{bp/1000:>6.1f}\n"
                output += line
                print(f"Added line to output: {line.strip()}")
    
    output = f"```\n{output}\n```"
    print("\nFinal output:")
    print(output)
    return output

def account_summary_command(transaction_data: TransactionData) -> str:
    print("Starting account_summary_command")
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Current date: {current_date}")
    
    accounts = transaction_data.get_accounts()
    headers = ['Metric', 'Total'] + accounts
    rows = []
    print(f"Accounts: {accounts}")
    print(f"Headers: {headers}")

    def get_balance(balance_type, account=None, currency=None, date=None):
        if date is None:
            date = current_date
        print(f"Getting balance for {balance_type}, account: {account}, currency: {currency}, date: {date}")
        balance, _ = transaction_data.get_spot_balance(
            date, balance_type, account=account, currency=currency
        )
        adjusted_balance = balance if currency == 'USD' else adjust_cad_to_usd(balance)
        print(f"  Balance: {balance}, Adjusted balance: {adjusted_balance}")
        return adjusted_balance

    print("\nCalculating available BP")
    avail_bp = {}
    avail_cashstk = {}
    for acc in accounts:
        cash_balance = sum(get_balance('cash_balances', acc, curr) for curr in ['USD', 'CAD'])
        stock_notional = sum(get_balance('stk_notional', acc, curr) for curr in ['USD', 'CAD'])
        avail_bp[acc] = cash_balance + stock_notional
        avail_cashstk[acc] = cash_balance + stock_notional
        print(f"Available BP for {acc}: {avail_bp[acc]}")
    
    total_cash = sum(avail_bp.values())
    avail_cashstk['Total'] = sum(avail_cashstk.values())
    loc_limit, loc_usage = transaction_data.get_loc_info()
    loc_available = adjust_cad_to_usd(loc_limit - loc_usage)
    avail_bp['Total'] = total_cash + loc_available
    print(f"Total available BP: {avail_bp['Total']}")

    print("\nGathering tickers and options")
    all_tickers = set()
    all_options = []

    for account in accounts:
        print(f"Processing account: {account}")
        _, stock_breakdown = transaction_data.get_spot_balance(current_date, 'stk_shares', account=account)
        all_tickers.update(key[3] for key in stock_breakdown.keys() if stock_breakdown[key] != 0)
        
        _, option_breakdown = transaction_data.get_spot_balance(current_date, 'opt_positions', account=account)
        for key, shares in option_breakdown.items():
            if shares != 0:
                _, _, margin, ticker, option_type, strike, expiry = key
                all_tickers.add(ticker)
                all_options.append({
                    'ticker': ticker,
                    'expiration': expiry,
                    'option_type': option_type,
                    'strike': float(strike),
                    'margin': float(margin.strip('%')) / 100  # Add margin to option info
                })
    print(f"All tickers: {all_tickers}")
    print(f"All options: {all_options}")

    print("\nGetting stock prices")
    stock_prices = get_stock_last_price(list(all_tickers))
    stock_prices_dict = dict(zip(stock_prices['ticker'], stock_prices['last_price']))
    print(f"Stock prices: {stock_prices_dict}")

    print("\nGetting option values")
    option_prices = get_option_values(all_options)
    option_prices_dict = {(row['ticker'], row['expiration'], row['option_type'], row['strike']): row['value'] 
                          for _, row in option_prices.iterrows()}
    print(f"Option prices: {option_prices_dict}")

    print("\nCalculating used BP")
    used_bp = {acc: 0 for acc in accounts}
    for account in accounts:
        print(f"Processing account: {account}")
        _, stock_shares = transaction_data.get_spot_balance(current_date, 'stk_shares', account=account)
        _, stock_notional = transaction_data.get_spot_balance(current_date, 'stk_notional', account=account)
        
        for key in stock_shares.keys():
            shares = stock_shares.get(key, 0)
            notional = stock_notional.get(key, 0)
            if shares != 0:
                _, currency, margin, ticker = key
                current_price = stock_prices_dict.get(ticker, 0)
                margin_percentage = float(margin.strip('%')) / 100                    
                stock_used_bp = notional - (shares * current_price) + (shares * current_price * margin_percentage * 100)                    
                used_bp[account] += stock_used_bp
                print(f"  Stock BP for {ticker}: {stock_used_bp}")
        
        _, option_breakdown = transaction_data.get_spot_balance(current_date, 'opt_positions', account=account)
        for key, shares in option_breakdown.items():
            if shares != 0:
                _, currency, margin, ticker, option_type, strike, expiry = key
                current_price = stock_prices_dict.get(ticker, 0)
                option_price = option_prices_dict.get((ticker, expiry, option_type, float(strike)), 0)
                
                option_price = 0 if option_price is None or not isinstance(option_price, (int, float)) or math.isnan(option_price) else option_price
                
                margin_percentage = float(margin.strip('%')) / 100
                stock_component = margin_percentage * 100 * current_price  # Use dynamic margin
                option_component = option_price
                strike_diff_component = min(0, float(strike) - current_price)
                
                per_share_bp = stock_component + option_component + strike_diff_component                    
                option_used_bp = abs(shares) * per_share_bp                    
                used_bp[account] += option_used_bp
                print(f"  Option BP for {ticker} {option_type} {strike}: {option_used_bp}")

    used_bp['Total'] = sum(used_bp.values())
    print(f"Total used BP: {used_bp['Total']}")

    print("\nCalculating notional values")
    notional = {acc: sum(get_balance(bt, account=acc, currency=curr) 
                         for bt in ['stk_notional', 'opt_notional'] 
                         for curr in ['USD', 'CAD']) 
                for acc in accounts}
    notional['Total'] = sum(notional.values())
    print(f"Notional values: {notional}")

    print("\nCalculating risk percentages")
    risk_percent = {acc: (used_bp[acc] / notional[acc] * 100 if notional[acc] else 0) for acc in accounts + ['Total']}
    print(f"Risk percentages: {risk_percent}")

    print("\nCalculating cover percentages")
    cover_percent = {acc: (avail_bp[acc] / notional[acc] * 100 if notional[acc] else 0) for acc in accounts + ['Total']}
    print(f"Cover percentages: {cover_percent}")

    print("\nCalculating MTD and LM revenue")
    mtd_start = current_date.replace(day=1) - timedelta(days=1)
    lm_start = mtd_start.replace(day=1) - timedelta(days=1)
    lm_end = mtd_start

    mtd_revenue = {acc: sum(get_balance('revenue', account=acc, currency=curr) for curr in ['USD', 'CAD']) 
                   - sum(get_balance('revenue', account=acc, currency=curr, date=mtd_start) for curr in ['USD', 'CAD'])
                   for acc in accounts}
    mtd_revenue['Total'] = sum(mtd_revenue.values())
    print(f"MTD revenue: {mtd_revenue}")

    lm_revenue = {acc: sum(get_balance('revenue', account=acc, currency=curr, date=lm_end) for curr in ['USD', 'CAD']) 
                  - sum(get_balance('revenue', account=acc, currency=curr, date=lm_start) for curr in ['USD', 'CAD'])
                  for acc in accounts}
    lm_revenue['Total'] = sum(lm_revenue.values())
    print(f"LM revenue: {lm_revenue}")

    metrics = [
        ('-Cash+Stk', avail_cashstk),
        ('Total BP', avail_bp),
        ('Used BP', used_bp),
        ('Notional', notional),
        ('Risk %', risk_percent),
        ('Cover %', cover_percent),
        ('MTD Rev.', mtd_revenue),
        ('LM Rev.', lm_revenue)
    ]

    print("\nGenerating table rows")
    for metric, values in metrics:
        row = [metric]
        for acc in ['Total'] + accounts:
            value = values[acc]
            if metric == 'Cover %' and acc != 'Total':
                formatted_value = ''
            else:
                formatted_value = f"{value:.1f}%" if metric in ['Risk %', 'Cover %'] else format_number(value)
            row.append(formatted_value)
        rows.append(row)
        print(f"Added row: {row}")

    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("\nFinal table:")
    print(table)
    return table

def ron_command(transaction_data: TransactionData) -> str:
    print("Starting ron_command")
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"Current date: {current_date}")
    
    headers = ['', 'Rev.', 'Not.', 'RON']
    rows = []
    total_revenue = 0
    total_notional = 0
    print(f"Headers: {headers}")

    for i in range(6):
        print(f"\nProcessing period {i+1} of 6")
        if i == 0:
            end_date = current_date
            start_date = current_date.replace(day=1)
            period = 'MTD'
        else:
            end_date = start_date - timedelta(days=1)
            start_date = end_date.replace(day=1)
            period = end_date.strftime('%m/%y')
        
        print(f"Period: {period}, Start date: {start_date}, End date: {end_date}")
        
        revenue = 0
        for currency in ['USD', 'CAD']:
            print(f"  Processing currency: {currency}")
            _, start_breakdown = transaction_data.get_spot_balance(start_date - timedelta(days=1), 'revenue', currency=currency)
            _, end_breakdown = transaction_data.get_spot_balance(end_date, 'revenue', currency=currency)
            
            print(f"    Start breakdown: {start_breakdown}")
            print(f"    End breakdown: {end_breakdown}")
            
            for key in set(end_breakdown) | set(start_breakdown):
                rev = end_breakdown.get(key, 0) - start_breakdown.get(key, 0)
                adjusted_rev = rev if currency == 'USD' else adjust_cad_to_usd(rev)
                revenue += adjusted_rev
                print(f"    Key: {key}, Revenue: {rev}, Adjusted revenue: {adjusted_rev}")

        print(f"  Total revenue for period: {revenue}")

        avg_notional = 0
        for currency in ['USD', 'CAD']:
            print(f"  Processing currency for notional: {currency}")
            for balance_type in ['stk_notional', 'opt_notional']:
                notional, breakdown = transaction_data.get_average_balance(start_date, end_date, balance_type, currency=currency)
                adjusted_notional = notional if currency == 'USD' else adjust_cad_to_usd(notional)
                avg_notional += adjusted_notional
                print(f"    Balance type: {balance_type}, Notional: {notional}, Adjusted notional: {adjusted_notional}")
                print(f"    Breakdown: {breakdown}")

        print(f"  Average notional for period: {avg_notional}")

        ron = (revenue / avg_notional * 100) if avg_notional != 0 else 0
        print(f"  RON for period: {ron:.2f}%")

        rows.append([
            period,
            format_number(revenue),
            format_number(avg_notional),
            f"{ron:.2f}%"
        ])
        print(f"  Added row: {rows[-1]}")

        total_revenue += revenue
        total_notional += avg_notional

    print(f"\nTotal revenue across all periods: {total_revenue}")
    print(f"Total notional across all periods: {total_notional}")

    avg_notional = total_notional / 6
    avg_ron = (total_revenue / total_notional * 100) if total_notional != 0 else 0

    print(f"Average notional: {avg_notional}")
    print(f"Average RON: {avg_ron:.2f}%")

    rows.append(['-' * 5, '-' * 7, '-' * 7, '-' * 7])
    rows.append([
        'Avg',
        format_number(total_revenue),
        format_number(avg_notional),
        f"{avg_ron:.2f}%"
    ])
    print(f"Added average row: {rows[-1]}")

    table = tabulate(rows, headers=headers, numalign="right", stralign="right", colalign=("left",) + ("right",) * (len(headers) - 1), disable_numparse=True)
    table = f"```\n{table}\n```"
    print("\nFinal table:")
    print(table)
    return table