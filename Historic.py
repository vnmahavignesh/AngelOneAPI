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
