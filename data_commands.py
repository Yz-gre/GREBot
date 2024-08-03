import pandas as pd
from datetime import datetime, timedelta
from yahooquery import Ticker
from yahooquery_tester import format_strike_for_symbol
from typing import Tuple
import re

def data_expiration(ticker: str, expiration_date: str) -> Tuple[str, str]:
    try:
        # Initialize Ticker object
        stock = Ticker(ticker)
        
        # Get current stock price
        current_price = stock.price[ticker]['regularMarketPrice']
        current_price_str = f"${current_price:.2f}"
        
        # Get option chain
        options = stock.option_chain
        
        if not isinstance(options, pd.DataFrame):
            return current_price_str, f"Unexpected options data type for {ticker}"
        
        # Filter options for the given expiration date and put options
        expiry_str = datetime.strptime(expiration_date, '%Y-%m-%d').strftime('%y%m%d')
        options = options[options['contractSymbol'].str.contains(f"{ticker}{expiry_str}P")]
        
        if options.empty:
            return current_price_str, f"No put options data available for {ticker} on {expiration_date}"
        
        # Find the ATM strike
        atm_strike = options['strike'].iloc[(options['strike'] - current_price).abs().argsort()[0]]
        
        # Get 5 strikes above and below ATM
        strikes = sorted(set(options['strike']))
        atm_index = strikes.index(atm_strike)
        selected_strikes = strikes[max(0, atm_index - 5):atm_index + 6]
        
        # Create contract symbols for selected strikes
        contract_symbols = [
            f"{ticker}{expiry_str}P{format_strike_for_symbol(strike)}"
            for strike in selected_strikes
        ]
        
        # Filter options for selected contract symbols
        filtered_options = options[options['contractSymbol'].isin(contract_symbols)]
        
        # Prepare results
        results = []
        for _, option in filtered_options.iterrows():
            price = (option['bid'] + option['ask']) / 2 if option['bid'] > 0 and option['ask'] > 0 else option['lastPrice']
            returns = price / option['strike']
            results.append({
                'Strike': option['strike'],
                'Price': price,
                'Return': returns
            })
        
        # Sort results by strike price
        results.sort(key=lambda x: x['Strike'])
        
        # Format output table
        table = f"{'Strike':^8} | {'Price':^8} | {'Return':^8}\n"
        table += f"{'-'*8}-+-{'-'*8}-+-{'-'*8}\n"
        for result in results:
            table += f"{result['Strike']:8.2f} | {result['Price']:8.2f} | {result['Return']:7.2%} \n"
        
        return current_price_str, table
    
    except Exception as e:
        return current_price_str, f"An error occurred: {str(e)}"

def data_strike(ticker: str, strike: float) -> Tuple[str, str]:
    try:
        # Initialize Ticker object
        stock = Ticker(ticker)
        
        # Get current stock price
        current_price = stock.price[ticker]['regularMarketPrice']
        current_price_str = f"${current_price:.2f}"
        
        # Get option chain
        options = stock.option_chain
        
        if not isinstance(options, pd.DataFrame):
            print("Error: options is not a DataFrame")
            return current_price_str, f"Unexpected options data type for {ticker}"
        
        # Get the next 6 monthly expiration dates (3rd Friday of each month)
        today = datetime.now()
        expiration_dates = []
        current_month = today.replace(day=1)
        while len(expiration_dates) < 6:
            third_friday = current_month + timedelta(days=(4 - current_month.weekday() + 7) % 7 + 14)
            if third_friday > today:
                expiration_dates.append(third_friday)
            current_month = current_month.replace(day=1) + timedelta(days=32)
            current_month = current_month.replace(day=1)
        
        # Create contract symbols for the strike and expiration dates
        contract_symbols = [
            f"{ticker}{date.strftime('%y%m%d')}P{format_strike_for_symbol(strike)}"
            for date in expiration_dates
        ]
        
        # Filter options for the created contract symbols
        filtered_options = options[options['contractSymbol'].isin(contract_symbols)]
        
        if filtered_options.empty:
            print("No options data available after filtering")
            return current_price_str, f"No put options data available for {ticker} with strike {strike} for the next 6 monthly expirations"
        
        # Prepare results
        results = []
        for _, option in filtered_options.iterrows():
            # Extract date from contract symbol using regex
            match = re.search(r'(\d{6})P', option['contractSymbol'])
            if match:
                expiry_date = datetime.strptime(match.group(1), '%y%m%d')
                price = (option['bid'] + option['ask']) / 2 if option['bid'] > 0 and option['ask'] > 0 else option['lastPrice']
                returns = price / strike
                results.append({
                    'Expiry': expiry_date,
                    'Price': price,
                    'Return': returns
                })                
            else:
                print(f"Could not extract date from contract symbol: {option['contractSymbol']}")
        
        # Sort results by expiration date
        results.sort(key=lambda x: x['Expiry'])        
        
        # Format output table
        table = f"{'Expiry':^10} | {'Price':^8} | {'Return':^8}\n"
        table += f"{'-'*10}-+-{'-'*8}-+-{'-'*8}\n"
        for result in results:
            table += f"{result['Expiry'].strftime('%Y-%m-%d'):10} | {result['Price']:8.2f} | {result['Return']:7.2%} \n"
                
        return current_price_str, table
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return current_price_str, f"An error occurred: {str(e)}"   