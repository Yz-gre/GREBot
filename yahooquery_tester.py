from yahooquery import Ticker
from datetime import datetime
import pandas as pd

def format_strike_for_symbol(strike):
    """
    Format the strike price for the contract symbol.
    """
    strike_int = int(strike * 1000)  # Convert to integer, handling decimals
    return f"{strike_int:08d}"  # Pad with zeros to 8 digits

def get_stock_last_price(tickers):
    """
    Get the last price (current share price) for one or more stock tickers.
    
    :param tickers: str or list of str, ticker symbol(s) to fetch prices for
    :return: DataFrame with ticker symbols and their last prices
    """
    if isinstance(tickers, str):
        tickers = [tickers]  # Convert single ticker to list
    
    results = []
    
    for ticker_symbol in tickers:
        try:
            ticker = Ticker(ticker_symbol)
            price_data = ticker.price[ticker_symbol]
            
            if 'regularMarketPrice' in price_data:
                last_price = price_data['regularMarketPrice']
                results.append({'ticker': ticker_symbol, 'last_price': last_price, 'error': None})
            else:
                results.append({'ticker': ticker_symbol, 'last_price': None, 'error': "Regular market price not available"})
        
        except Exception as e:
            results.append({'ticker': ticker_symbol, 'last_price': None, 'error': str(e)})
    
    return pd.DataFrame(results)

def get_option_values(options):
    """
    Get option values for multiple options.
    
    :param options: list of dicts, each containing:
                    {'ticker': str, 'expiration': str, 'option_type': str, 'strike': float}
    :return: DataFrame with option details and calculated values
    """
    results = []
    
    for option in options:
        ticker_symbol = option['ticker']
        expiration = datetime.strptime(option['expiration'], '%Y-%m-%d')
        option_type = option['option_type'].upper()
        strike = option['strike']
        
        # Construct the contract symbol
        expiration_str = expiration.strftime('%y%m%d')
        strike_str = format_strike_for_symbol(strike)
        contract_symbol = f"{ticker_symbol}{expiration_str}{option_type[0]}{strike_str}"
        
        ticker = Ticker(ticker_symbol)
        option_data = ticker.option_chain
        
        if option_data is None or option_data.empty:
            results.append({**option, 'contract_symbol': contract_symbol, 'value': None, 'error': "No option data available"})
            continue
        
        try:
            if 'contractSymbol' in option_data.index.names:
                contract_data = option_data.xs(contract_symbol, level='contractSymbol')
            elif 'contractSymbol' in option_data.columns:
                contract_data = option_data[option_data['contractSymbol'] == contract_symbol]
            else:
                results.append({**option, 'contract_symbol': contract_symbol, 'value': None, 'error': "Contract symbol not found in data structure"})
                continue

            if contract_data.empty:
                results.append({**option, 'contract_symbol': contract_symbol, 'value': None, 'error': "No data found for contract symbol"})
                continue

            bid = contract_data['bid'].values[0]
            ask = contract_data['ask'].values[0]
            last_price = contract_data['lastPrice'].values[0]

            if bid > 0 and ask > 0:
                value = (bid + ask) / 2
            else:
                value = last_price

            results.append({**option, 'contract_symbol': contract_symbol, 'value': value, 'error': None})

        except KeyError:
            results.append({**option, 'contract_symbol': contract_symbol, 'value': None, 'error': "Contract symbol not found in option chain"})
        except Exception as e:
            results.append({**option, 'contract_symbol': contract_symbol, 'value': None, 'error': str(e)})
    
    return pd.DataFrame(results)

def get_fx_rate(currency_pair):
    """
    Get the current exchange rate for a currency pair.
    
    :param currency_pair: str, e.g., "USDCAD" for USD to CAD
    :return: tuple (exchange_rate, error_message)
    """
    try:
        ticker = Ticker(f"{currency_pair}=X")
        quote_data = ticker.price[f"{currency_pair}=X"]
        
        if 'regularMarketPrice' not in quote_data:
            return None, "Unable to retrieve exchange rate"
        
        exchange_rate = quote_data['regularMarketPrice']
        return exchange_rate, None
    
    except Exception as e:
        return None, f"An error occurred: {str(e)}"

