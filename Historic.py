from datetime import datetime
import os
from typing import Any, Dict
from SmartApi import SmartConnect
import pandas as pd


class HistoricalDataManager:
    """Handles market data fetching operations"""

    def __init__(self, smart_connect: SmartConnect):
        self.smart_connect = smart_connect

    def get_historical_data(self, params: Dict[str, Any]) -> pd.DataFrame:
        """Fetch historical candle data and return as DataFrame

        Args:
            params: Dictionary containing historical data parameters:
                - exchange: Exchange name (NSE, NFO, etc.)
                - symboltoken: Token of the instrument
                - interval: Time interval (ONE_MINUTE, FIVE_MINUTE, etc.)
                - fromdate: Start datetime (YYYY-MM-DD HH:MM)
                - todate: End datetime (YYYY-MM-DD HH:MM)

        Returns:
            pd.DataFrame with columns: ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        """
        try:
            res = self.smart_connect.getCandleData(params)

            if not res or 'data' not in res:
                return pd.DataFrame()

            hist_df = pd.DataFrame(
                res['data'],
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

            # Convert timestamp to datetime (uncomment if needed)
            hist_df['timestamp'] = pd.to_datetime(
                hist_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M')
            

            return hist_df

        except Exception as e:
            print(f"Error fetching historical data: {str(e)}")
            return pd.DataFrame()
        
    def save_historical_data_to_csv(self, token_values, day_open, fromdate, todate, exchange, interval, output_dir):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') # Generate a timestamp for the filename
        csv_filename = f"Nifty_{day_open}_{timestamp}.csv"         # Create a unique filename using the day open price and timestamp
        csv_path = os.path.join(output_dir, csv_filename)    # Combine the output directory and filename to get the full path for the CSV file
        first_write = True       # Flag to indicate if it's the first write to the CSV file (to include header only for the first write)

        for strike, records in token_values.items():
            for record in records:
                symbol = record.get('symbol')
                token = str(record.get('token'))
                params = {
                    'exchange': exchange,
                    'symboltoken': token,
                    'interval': interval,
                    'fromdate': fromdate,
                    'todate': todate,
                }
                hist_df = self.get_historical_data(params) # Fetch historical data for the current symbol/token using the get_historical_data method
                if hist_df.empty:
                    continue

                hist_df['strikeprice'] = int(strike / 100) # Add a new column for strike price by dividing the strike value by 100 (since it was multiplied by 100 in the master list)
                hist_df['symbol'] = symbol # Add a new column for symbol using the symbol from the current record
                hist_df['token'] = token # Add a new column for token using the token from the current record
                hist_df = hist_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'strikeprice', 'symbol', 'token']] # Reorder columns to include the new columns for strike price, symbol, and token at the end

                hist_df.to_csv(csv_path, mode='a', index=False, header=first_write) # Append to CSV, write header only for the first write
                first_write = False  # Set the flag to False after the first write to ensure header is only written once

        return csv_path
