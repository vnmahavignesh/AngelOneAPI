from datetime import datetime
import os
from typing import Any, Dict, List, Optional
from SmartApi import SmartConnect
import pandas as pd


class LiveMarketDataManager:
    """Handles Live Market Data fetching operations"""
    
    def __init__(self, smart_connect: SmartConnect):
        self.smart_connect = smart_connect

    def get_live_market_data(self, mode, exchange_tokens: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """Fetch live market data for given exchange tokens
        
        Args:
            mode: Market data mode (FULL, QUOTE, etc.)
            exchange_tokens: Dictionary with exchange names as keys and list of tokens as values
                            Example: {"NSE": ["99926000"], "NFO": ["40803"]}
        
        Returns:
            Dictionary containing the live market data response
        """
        try:
            if exchange_tokens is None:
                exchange_tokens = {}
            
            res = self.smart_connect.getMarketData(mode=mode, exchangeTokens=exchange_tokens)
            
            if not res or 'data' not in res:
                return {'status': False, 'data': {'fetched': [], 'unfetched': []}}
            
            return res
            
        except Exception as e:
            print(f"Error fetching live market data: {str(e)}")
            return {'status': False, 'data': {'fetched': [], 'unfetched': []}}

    def get_live_market_data_as_dataframe(self, mode, 
                                         exchange_tokens: Dict[str, List[str]] = None) -> pd.DataFrame:
        """Fetch live market data and return as DataFrame
        
        Args:
            mode: Market data mode (FULL, QUOTE, etc.)
            exchange_tokens: Dictionary with exchange names as keys and list of tokens as values
        
        Returns:
            pd.DataFrame with live market data for all fetched instruments
        """
        try:
            response = self.get_live_market_data(mode, exchange_tokens)
            
            if not response or 'data' not in response:
                return pd.DataFrame()
            
            fetched_data = response['data'].get('fetched', [])
            
            if not fetched_data:
                print("No live market data fetched")
                return pd.DataFrame()
            
            # Convert to DataFrame
            live_df = pd.DataFrame(fetched_data)
            
            print(f"\nLive market data fetched successfully with {len(live_df)} records!")
            return live_df
            
        except Exception as e:
            print(f"Error converting live market data to DataFrame: {str(e)}")
            return pd.DataFrame()

    def get_instrument_data(self, exchange: str, token: str) -> Optional[Dict[str, Any]]:
        """Fetch live market data for a single instrument
        
        Args:
            exchange: Exchange name (NSE, NFO, etc.)
            token: Symbol token of the instrument
        
        Returns:
            Dictionary containing instrument data or None if not found
        """
        try:
            exchange_tokens = {exchange: [token]}
            response = self.get_live_market_data("FULL", exchange_tokens)
            
            if not response or 'data' not in response:
                return None
            
            fetched_data = response['data'].get('fetched', [])
            
            if fetched_data:
                return fetched_data[0]
            
            return None
            
        except Exception as e:
            print(f"Error fetching instrument data: {str(e)}")
            return None

    def get_multiple_instruments_data(self, exchange_tokens: Dict[str, List[str]]) -> pd.DataFrame:
        """Fetch live market data for multiple instruments and return as DataFrame
        
        Args:
            exchange_tokens: Dictionary with exchange names as keys and list of tokens as values
        
        Returns:
            pd.DataFrame with live market data for all requested instruments
        """
        return self.get_live_market_data_as_dataframe("FULL", exchange_tokens)

    def extract_key_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and return only the key market data fields
        
        Args:
            df: DataFrame containing live market data
        
        Returns:
            DataFrame with key fields only
        """
        if df.empty:
            return pd.DataFrame()
        
        # Define key fields to extract
        key_fields = [
            'exchange', 'tradingSymbol', 'symbolToken', 'ltp', 'open', 
            'high', 'low', 'close', 'netChange', 'percentChange', 
            'tradeVolume', 'opnInterest', 'exchFeedTime'
        ]
        
        # Filter only available columns
        available_fields = [field for field in key_fields if field in df.columns]
        
        return df[available_fields]

    def print_instrument_data(self, df: pd.DataFrame) -> None:
        """Print formatted instrument data
        
        Args:
            df: DataFrame containing live market data
        """
        if df.empty:
            print("No data to display")
            return
        
        for _, instrument in df.iterrows():
            print(f"\n{'='*50}")
            print(f"Exchange: {instrument.get('exchange', 'N/A')}")
            print(f"Symbol: {instrument.get('tradingSymbol', 'N/A')}")
            print(f"Token: {instrument.get('symbolToken', 'N/A')}")
            print(f"{'='*50}")
            print(f"Open: {instrument.get('open', 'N/A')}")
            print(f"High: {instrument.get('high', 'N/A')}")
            print(f"Low: {instrument.get('low', 'N/A')}")
            print(f"Close: {instrument.get('close', 'N/A')}")
            print(f"LTP: {instrument.get('ltp', 'N/A')}")
            print(f"Net Change: {instrument.get('netChange', 'N/A')}")
            print(f"Percent Change: {instrument.get('percentChange', 'N/A')}%")
            print(f"Volume: {instrument.get('tradeVolume', 'N/A')}")
            print(f"Open Interest: {instrument.get('opnInterest', 'N/A')}")
            print(f"52 Week High: {instrument.get('52WeekHigh', 'N/A')}")
            print(f"52 Week Low: {instrument.get('52WeekLow', 'N/A')}")
            print(f"Feed Time: {instrument.get('exchFeedTime', 'N/A')}")

    def save_live_market_data_to_csv(self, exchange_tokens: Dict[str, List[str]], 
                                     output_dir: str = '.', 
                                     prefix: str = 'live_market_data') -> str:
        """Fetch live market data and save to CSV file
        
        Args:
            exchange_tokens: Dictionary with exchange names as keys and list of tokens as values
            output_dir: Directory to save the CSV file
            prefix: Prefix for the CSV filename
        
        Returns:
            Path to the saved CSV file
        """
        try:
            # Fetch data as DataFrame
            df = self.get_live_market_data_as_dataframe("FULL", exchange_tokens)
            
            if df.empty:
                print("No data to save")
                return ""
            
            # Remove the 'depth' column if it exists
            if 'depth' in df.columns:
                df = df.drop(columns=['depth'])
                print("Removed 'depth' column from the data")
            
            # Generate timestamp for filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f"{prefix}_{timestamp}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            
            # Save to CSV
            df.to_csv(csv_path, index=False)
            print(f"Live market data saved to: {csv_path}")
            
            return csv_path
            
        except Exception as e:
            print(f"Error saving live market data to CSV: {str(e)}")
            return ""