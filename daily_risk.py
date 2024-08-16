import csv
from datetime import datetime
import time
from transaction_data import TransactionData
from yahooquery_tester import get_stock_last_price, get_fx_rate, get_option_values
from typing import Dict, Any, List

def adjust_cad_to_usd(value: float, currency: str, fx_rate: float) -> float:
    return value / fx_rate if currency == 'CAD' else value

def format_strike_for_symbol(strike):
    strike_int = int(float(strike) * 1000)
    return f"{strike_int:08d}"

def get_option_code(ticker: str, expiration: str, option_type: str, strike: float) -> str:
    expiry = datetime.strptime(expiration, '%Y-%m-%d')
    expiry_str = expiry.strftime('%y%m%d')
    strike_str = format_strike_for_symbol(strike)
    return f"{ticker}{expiry_str}{option_type[0].upper()}{strike_str}"

def get_live_prices(stock_tickers: List[str], option_data: List[Dict[str, Any]], max_retries: int = 3) -> Dict[str, float]:
    all_prices = {}

    # Fetch stock prices
    for attempt in range(max_retries):
        try:
            stock_prices = get_stock_last_price(stock_tickers)
            all_prices.update(dict(zip(stock_prices['ticker'], stock_prices['last_price'])))
            print(f"Successfully fetched prices for {len(stock_tickers)} stocks")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for stocks: {str(e)}")
            if attempt == max_retries - 1:
                print(f"Failed to fetch stock prices after {max_retries} attempts")
            time.sleep(2 ** attempt)  # Exponential backoff

    # Fetch option prices
    for attempt in range(max_retries):
        try:
            option_prices = get_option_values(option_data)
            for _, row in option_prices.iterrows():
                key = f"{row['ticker']}_{row['expiration']}_{row['option_type']}_{row['strike']}"
                all_prices[key] = row['value']
            print(f"Successfully fetched prices for {len(option_data)} options")
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for options: {str(e)}")
            if attempt == max_retries - 1:
                print(f"Failed to fetch option prices after {max_retries} attempts")
            time.sleep(2 ** attempt)  # Exponential backoff

    return all_prices

def calculate_daily_risk(user_data_mapping: Dict[str, TransactionData]) -> Dict[str, Any]:
    results = {}
    fx_rate, _ = get_fx_rate('USDCAD')
    print(f"Current USD/CAD exchange rate: {fx_rate}")

    for user_id, transaction_data in user_data_mapping.items():
        print(f"\nCalculating daily risk for user: {user_id}")
        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        accounts = transaction_data.get_accounts()
        
        # Calculate temporary cash
        temp_cash = sum(
            adjust_cad_to_usd(
                transaction_data.get_spot_balance(current_date, 'investments', account=acc, currency=curr, investment_type='temp')[0],
                curr,
                fx_rate
            )
            for acc in accounts
            for curr in ['USD', 'CAD']
        )
        print(f"Outstanding temporary cash: ${temp_cash:.2f}")

        # Calculate total cash
        total_cash = sum(
            adjust_cad_to_usd(
                transaction_data.get_spot_balance(current_date, 'cash_balances', account=acc, currency=curr)[0],
                curr,
                fx_rate
            )
            for acc in accounts
            for curr in ['USD', 'CAD']
        )
        print(f"Outstanding total cash: ${total_cash:.2f}")

        # Calculate stock notional
        stock_notional = sum(
            adjust_cad_to_usd(
                transaction_data.get_spot_balance(current_date, 'stk_notional', account=acc, currency=curr)[0],
                curr,
                fx_rate
            )
            for acc in accounts
            for curr in ['USD', 'CAD']
        )
        print(f"Outstanding stock notional: ${stock_notional:.2f}")

        # Calculate LOC information
        loc_limit, loc_usage = transaction_data.get_loc_info()
        loc_limit_usd = adjust_cad_to_usd(loc_limit, 'CAD', fx_rate)
        loc_usage_usd = adjust_cad_to_usd(loc_usage, 'CAD', fx_rate)
        loc_available = loc_limit_usd - loc_usage_usd
        print(f"Outstanding LOC limit: ${loc_limit_usd:.2f}")
        print(f"Outstanding LOC usage: ${loc_usage_usd:.2f}")
        print(f"Outstanding LOC available: ${loc_available:.2f}")

        # Calculate total available buying power
        total_bp = total_cash + stock_notional + loc_available
        print(f"Total available buying power: ${total_bp:.2f}")

        # Calculate option notional
        option_notional = sum(
            adjust_cad_to_usd(
                transaction_data.get_spot_balance(current_date, 'opt_notional', account=acc, currency=curr)[0],
                curr,
                fx_rate
            )
            for acc in accounts
            for curr in ['USD', 'CAD']
        )
        print(f"Outstanding option notional: ${option_notional:.2f}")

        # Calculate total outstanding notional
        total_notional = stock_notional + option_notional
        print(f"Total outstanding notional: ${total_notional:.2f}")

        # Prepare data for live price fetching
        stock_tickers = set()
        option_data = []
        
        for acc in accounts:
            for curr in ['USD', 'CAD']:
                _, stock_breakdown = transaction_data.get_spot_balance(current_date, 'stk_notional', account=acc, currency=curr)
                _, option_breakdown = transaction_data.get_spot_balance(current_date, 'opt_positions', account=acc, currency=curr)

                for key, notional in stock_breakdown.items():
                    if notional != 0:
                        _, _, _, ticker = key
                        stock_tickers.add(ticker)

                for key, shares in option_breakdown.items():
                    if shares != 0:
                        _, _, _, ticker, option_type, strike, expiry = key
                        stock_tickers.add(ticker)  # Add underlying stock ticker
                        option_data.append({
                            'ticker': ticker,
                            'expiration': expiry,
                            'option_type': option_type,
                            'strike': float(strike)
                        })

        print(f"Fetching live prices for {len(stock_tickers)} stocks and {len(option_data)} options")
        # Fetch live prices
        live_prices = get_live_prices(list(stock_tickers), option_data)

        # Calculate Risk %
        used_bp = 0
        for acc in accounts:
            print(f"\nCalculating risk for account: {acc}")
            for curr in ['USD', 'CAD']:
                _, stock_shares = transaction_data.get_spot_balance(current_date, 'stk_shares', account=acc, currency=curr)
                _, stock_breakdown = transaction_data.get_spot_balance(current_date, 'stk_notional', account=acc, currency=curr)
                _, option_breakdown = transaction_data.get_spot_balance(current_date, 'opt_positions', account=acc, currency=curr)

                for key, notional in stock_breakdown.items():
                    if notional != 0:
                        _, _, margin, ticker = key
                        shares = stock_shares.get(key, 0)
                        current_price = live_prices.get(ticker, 0)
                        if current_price == 0:
                            print(f"Warning: Missing price for stock {ticker}")
                            continue
                        margin_percentage = float(margin.strip('%')) / 100
                        stock_used_bp = notional - (shares * current_price) + (shares * current_price * margin_percentage * 100)
                        adjusted_bp = adjust_cad_to_usd(stock_used_bp, curr, fx_rate)
                        used_bp += adjusted_bp
                        print(f"Stock: {ticker}, Shares: {shares}, Price: ${current_price:.2f}, Margin: {margin_percentage:.2%}")
                        print(f"  Notional: ${notional:.2f}")
                        print(f"  Used BP: ${stock_used_bp:.2f} {curr} (${adjusted_bp:.2f} USD)")

                for key, shares in option_breakdown.items():
                    if shares != 0:
                        _, _, margin, ticker, option_type, strike, expiry = key
                        option_code = f"{ticker}_{expiry}_{option_type}_{strike}"
                        option_price = live_prices.get(option_code, 0)
                        stock_price = live_prices.get(ticker, 0)
                        if option_price == 0 or stock_price == 0:
                            print(f"Warning: Missing price for option {option_code} or its underlying stock {ticker}")
                            continue
                        margin_percentage = float(margin.strip('%')) / 100
                        strike = float(strike)
                        stock_component = margin_percentage * 100 * stock_price
                        option_component = option_price
                        strike_diff_component = min(0, strike - stock_price)
                        option_used_bp = abs(shares) * (stock_component + option_component + strike_diff_component)
                        adjusted_bp = adjust_cad_to_usd(option_used_bp, curr, fx_rate)
                        used_bp += adjusted_bp
                        print(f"Option: {ticker} {option_type} {strike} {expiry}, Contracts: {abs(shares)}")
                        print(f"  Stock Price: ${stock_price:.2f}, Option Price: ${option_price:.2f}, Margin: {margin_percentage:.2%}")
                        print(f"  Stock Component: ${stock_component:.2f}")
                        print(f"  Option Component: ${option_component:.2f}")
                        print(f"  Strike Diff Component: ${strike_diff_component:.2f}")
                        print(f"  Used BP: ${option_used_bp:.2f} {curr} (${adjusted_bp:.2f} USD)")

        risk_percent = (used_bp / total_notional * 100) if total_notional else 0
        print(f"\nTotal Used BP: ${used_bp:.2f} USD")
        print(f"Total Notional: ${total_notional:.2f} USD")
        print(f"Risk %: {risk_percent:.2f}%")

        # Calculate Cover %
        cover_percent = (total_bp / total_notional * 100) if total_notional else 0
        print(f"Cover %: {cover_percent:.2f}%")

        results[user_id] = {
            'temp_cash': temp_cash,
            'total_cash': total_cash,
            'stock_notional': stock_notional,  # This is now a single float value
            'loc_limit': loc_limit_usd,
            'loc_usage': loc_usage_usd,
            'loc_available': loc_available,
            'total_bp': total_bp,
            'option_notional': option_notional,
            'total_notional': total_notional,
            'risk_percent': risk_percent,
            'cover_percent': cover_percent
        }

    return results

def get_market_data():
    fx_rate, _ = get_fx_rate('USDCAD')
    tickers = ['^VIX', '^GSPC', '^IXIC', '^DJI', 'GC=F', 'CL=F']
    market_data = get_stock_last_price(tickers)
    market_dict = {
        'USDCAD': fx_rate,
        'VIX': market_data.loc[market_data['ticker'] == '^VIX', 'last_price'].iloc[0],
        'S&P500': market_data.loc[market_data['ticker'] == '^GSPC', 'last_price'].iloc[0],
        'NASDAQ': market_data.loc[market_data['ticker'] == '^IXIC', 'last_price'].iloc[0],
        'DOW': market_data.loc[market_data['ticker'] == '^DJI', 'last_price'].iloc[0],
        'Gold': market_data.loc[market_data['ticker'] == 'GC=F', 'last_price'].iloc[0],
        'Oil': market_data.loc[market_data['ticker'] == 'CL=F', 'last_price'].iloc[0]
    }
    return market_dict

def write_daily_risk_to_csv(file_path: str, user_data_mapping: Dict[str, TransactionData]):
    print("Starting daily risk calculation and CSV writing process")
    risk_data = calculate_daily_risk(user_data_mapping)
    market_data = get_market_data()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print("Writing data to CSV file")
    with open(file_path, 'a', newline='') as csvfile:
        fieldnames = ['timestamp', 'user_id', 'temp_cash', 'total_cash', 'stock_notional',
                      'loc_limit', 'loc_usage', 'loc_available', 'total_bp', 'option_notional',
                      'total_notional', 'risk_percent', 'cover_percent',
                      'USDCAD', 'VIX', 'S&P500', 'NASDAQ', 'DOW', 'Gold', 'Oil']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header if file is empty
        if csvfile.tell() == 0:
            writer.writeheader()
            print("CSV header written")

        for user_id, data in risk_data.items():
            row = {
                'timestamp': current_time,
                'user_id': user_id,
            }
            
            # Safely format numeric values for risk data
            numeric_fields = ['temp_cash', 'total_cash', 'stock_notional', 'loc_limit', 'loc_usage', 
                              'loc_available', 'total_bp', 'option_notional', 'total_notional', 
                              'risk_percent', 'cover_percent']
            for field in numeric_fields:
                try:
                    row[field] = f"{float(data.get(field, 0)):.2f}"
                except (ValueError, TypeError):
                    print(f"Warning: Invalid value for {field}: {data.get(field)}")
                    row[field] = "N/A"

            # Safely format market data
            market_fields = {'USDCAD': 4, 'VIX': 2, 'S&P500': 2, 'NASDAQ': 2, 'DOW': 2, 'Gold': 2, 'Oil': 2}
            for field, decimal_places in market_fields.items():
                try:
                    value = market_data.get(field, '')
                    row[field] = f"{float(value):.{decimal_places}f}"
                except (ValueError, TypeError):
                    print(f"Warning: Invalid value for {field}: {value}")
                    row[field] = "N/A"

            writer.writerow(row)
            print(f"Data written for user: {user_id}")

    print("CSV writing process completed")