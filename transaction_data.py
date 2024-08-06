import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from typing import List, Dict, Tuple, Union, Optional, Any
from functools import lru_cache
import requests
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TransactionData:
    def __init__(self, csv_path=None, LOCLimit=0, LOCUsage=0):
        self.transactions: List[Dict[str, str]] = []
        self.daily_balances: Dict[str, Dict[Tuple[str, ...], Dict[datetime, float]]] = {
            'investments': defaultdict(lambda: defaultdict(float)),
            'revenue': defaultdict(lambda: defaultdict(float)),
            'opt_positions': defaultdict(lambda: defaultdict(float)),
            'opt_notional': defaultdict(lambda: defaultdict(float)),
            'stk_shares': defaultdict(lambda: defaultdict(float)),
            'stk_notional': defaultdict(lambda: defaultdict(float)),
            'cash_balances': defaultdict(lambda: defaultdict(float))            
        }
        self.first_transaction_date: Union[datetime, None] = None
        self.last_update: Union[datetime, None] = None
        self.csv_path = csv_path
        self.LOCLimit = LOCLimit
        self.LOCUsage = LOCUsage

    def process_csv(self):
        if not os.path.exists(self.csv_path):
            print(f"CSV file not found: {self.csv_path}")
            return

        with open(self.csv_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            print("CSV headers:", reader.fieldnames)
            
            for row in reader:
                print("Processing row:", row)
                self.process_transaction(row)
        
        self.last_update = datetime.now()

        print("\nAccounts:", self.get_accounts())
        print("Currencies:", self.get_currencies())
        print("Tickers:", self.get_tickers())

    def process_transaction(self, transaction: Dict[str, str]) -> None:
        try:
            if '\ufeffAcct' in transaction:
                transaction['Acct'] = transaction.pop('\ufeffAcct')
            
            if 'Acct' not in transaction:
                print(f"Missing 'Acct' key in transaction: {transaction}")
                return

            self.transactions.append(transaction)
            
            date = datetime.strptime(transaction['Date'], '%Y-%m-%d')
            if self.first_transaction_date is None or date < self.first_transaction_date:
                self.first_transaction_date = date
            
            account = transaction['Acct']
            currency = transaction['Currency']
            margin = transaction['Margin %']
            trans_type = transaction['Trans Type']
            amount = self.parse_amount(transaction['Net Gains'])
            ticker = transaction['Ticker']

            notes = transaction.get('Notes', '').lower()

            if trans_type == 'Cash':
                if 'invest' in notes and 'pre convert' in notes:
                    self.update_daily_balance('investments', (account, currency, 'regular'), date, amount)
                elif any(item in notes for item in ['margin cover', 'short term juicing']):
                    self.update_daily_balance('investments', (account, currency, 'temp'), date, amount)
                elif 'personal withdraw' in notes:
                    self.update_daily_balance('investments', (account, currency, 'regular'), date, amount)

            self.update_daily_balance('cash_balances', (account, currency), date, amount)

            if trans_type in ['Put', 'Call', 'Cap Gains', 'Div', 'Int / Tax']:
                self.update_daily_balance('revenue', (account, currency, ticker, trans_type), date, amount)

            if trans_type in ['Put', 'Call']:
                shares = self.parse_amount(transaction['Shares'])
                strike_price = self.parse_amount(transaction.get('Strike/Price', '0'))
                expiry = datetime.strptime(transaction['Expiry'], '%Y-%m-%d')
                self.update_daily_balance('opt_positions', (account, currency, margin, ticker, trans_type, str(strike_price), expiry.strftime('%Y-%m-%d')), date, shares)
                self.update_daily_balance('opt_notional', (account, currency, margin, ticker, trans_type, str(strike_price), expiry.strftime('%Y-%m-%d')), date, shares * strike_price)

            if trans_type == 'Stk':
                shares = self.parse_amount(transaction['Shares'])
                self.update_daily_balance('stk_shares', (account, currency, margin, ticker), date, shares)
                self.update_daily_balance('stk_notional', (account, currency, margin, ticker), date, -amount)

        except KeyError as e:
            print(f"KeyError in transaction: {e}")
            print(f"Transaction data: {transaction}")
        except ValueError as e:
            print(f"ValueError in transaction: {e}")
            print(f"Transaction data: {transaction}")

    def update_daily_balance(self, balance_type: str, key: Tuple, date: datetime, amount: float) -> None:
        daily_balance = self.daily_balances[balance_type][key]
        current_date = datetime.now()
        for day in (date + timedelta(n) for n in range((current_date - date).days + 1)):
            daily_balance[day] += amount

    @staticmethod
    def parse_amount(value: Union[str, int, float]) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        value = value.strip()
        if value in ['-', '', '$-', ' $-']:
            return 0.0
        try:
            value = value.replace('$', '').replace('€', '').replace('£', '').replace(',', '')
            if value.startswith('(') and value.endswith(')'):
                return -float(value[1:-1])
            return float(value)
        except ValueError:
            logging.warning(f"Unexpected value in amount field: {value}. Treating as 0.")
            return 0.0
    
    def get_spot_balance(self, date: datetime, balance_type: str, account: Optional[str] = None, 
                         currency: Optional[str] = None, tickers: Optional[Union[str, List[str]]] = None, 
                         margin: Optional[str] = None, investment_type: Optional[str] = None) -> Tuple[float, Dict[Tuple, float]]:
        if balance_type not in self.daily_balances:
            raise ValueError(f"Invalid balance type: {balance_type}")
    
        if isinstance(tickers, str):
            tickers = [tickers]
    
        total_balance = 0
        breakdown: Dict[Tuple, float] = {}
    
        for key, daily_balance in self.daily_balances[balance_type].items():
            if date in daily_balance:
                if self._match_filters(key, balance_type, account, currency, tickers, margin, investment_type):
                    balance = daily_balance[date]
                    total_balance += balance
    
                    if balance_type == 'investments':
                        breakdown_key = (key[0], key[1], key[2])
                    else:
                        breakdown_key = key
    
                    breakdown[breakdown_key] = breakdown.get(breakdown_key, 0) + balance
    
        return total_balance, breakdown

    def get_average_balance(self, start_date: datetime, end_date: datetime, balance_type: str, 
                            account: Optional[str] = None, currency: Optional[str] = None, 
                            tickers: Optional[Union[str, List[str]]] = None, 
                            margin: Optional[str] = None, investment_type: Optional[str] = None) -> Tuple[float, Dict[Tuple, float]]:
        if balance_type not in self.daily_balances:
            raise ValueError(f"Invalid balance type: {balance_type}")
    
        if isinstance(tickers, str):
            tickers = [tickers]
    
        total_sum = 0
        breakdown_sum: Dict[Tuple, float] = {}
        day_count = (end_date - start_date).days + 1
    
        current_date = start_date
        while current_date <= end_date:
            daily_total, daily_breakdown = self.get_spot_balance(
                current_date, balance_type, account, currency, tickers, margin, investment_type
            )
            
            total_sum += daily_total
            
            for key, value in daily_breakdown.items():
                breakdown_sum[key] = breakdown_sum.get(key, 0) + value
    
            current_date += timedelta(days=1)
    
        average_total = total_sum / day_count
        average_breakdown = {key: value / day_count for key, value in breakdown_sum.items()}
    
        return average_total, average_breakdown

    def _match_filters(self, key: Tuple, balance_type: str, account: Optional[str], 
                       currency: Optional[str], tickers: Optional[List[str]], 
                       margin: Optional[str], investment_type: Optional[str]) -> bool:
        if balance_type == 'investments':
            return (
                (account is None or key[0] == account) and
                (currency is None or key[1] == currency) and
                (investment_type is None or key[2] == investment_type)
            )
        elif balance_type in ['opt_positions', 'opt_notional', 'stk_shares', 'stk_notional']:
            return (
                (account is None or key[0] == account) and
                (currency is None or key[1] == currency) and
                (margin is None or key[2] == margin) and
                (tickers is None or key[3] in tickers)
            )
        elif balance_type == 'revenue':
            return (
                (account is None or key[0] == account) and
                (currency is None or key[1] == currency) and
                (tickers is None or key[2] in tickers)
            )
        elif balance_type == 'cash_balances':
            return (
                (account is None or key[0] == account) and
                (currency is None or key[1] == currency)
            )
        else:
            return False
    
    @lru_cache(maxsize=None)
    def get_currencies(self) -> List[str]:
        return list(set(key[1] for key in self.daily_balances['cash_balances'].keys()))
    
    @lru_cache(maxsize=None)
    def get_accounts(self) -> List[str]:
        return list(set(key[0] for key in self.daily_balances['cash_balances'].keys()))
    
    @lru_cache(maxsize=None)
    def get_tickers(self) -> List[str]:
        tickers = set()
        for balance_type in ['revenue', 'opt_positions', 'stk_shares', 'stk_notional']:            
            for key in self.daily_balances[balance_type].keys():                
                try:
                    if balance_type == 'revenue':
                        if len(key) > 2:
                            tickers.add(key[2])
                    else:
                        if len(key) > 3:
                            tickers.add(key[3])
                except IndexError:
                    print(f"  IndexError for key: {key}")        
        return list(tickers)
    
    @staticmethod
    def _categorize_revenue(trans_type):
        if trans_type in ['Put', 'Call']:
            return 'Options'
        elif trans_type == 'Cap Gains':
            return 'Cap Gains'
        else:
            return 'Other'
    
    def format_currency(self, value: float, use_thousands: bool = False) -> str:
        if use_thousands:
            return f"${value:,.2f}"
        else:
            return f"${value / 1000:.1f}k"

    def get_loc_info(self) -> Tuple[float, float]:
        return self.LOCLimit, self.LOCUsage

    def clear_cache(self):
        self.get_currencies.cache_clear()
        self.get_accounts.cache_clear()
        self.get_tickers.cache_clear()

    def update_cache(self):
        self.clear_cache()
        # Force recalculation of cached methods
        self.get_currencies()
        self.get_accounts()
        self.get_tickers()