from datetime import datetime
import os
from typing import Any, Dict, List
from SmartApi import SmartConnect
import pandas as pd


class LiveMarketDataManager:
    """Handles Live Market Data fetching operations"""
    
    def __init__(self, smart_connect: SmartConnect):
        self.smart_connect = smart_connect

    def get_live_market_data(self, mode, exchange_tokens: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """Fetch live market data for given exchange tokens"""
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
        """Fetch live market data and return as DataFrame"""
        try:
            response = self.get_live_market_data(mode, exchange_tokens)
            
            if not response or 'data' not in response:
                return pd.DataFrame()
            
            fetched_data = response['data'].get('fetched', [])
            
            if not fetched_data:
                print("No live market data fetched")
                return pd.DataFrame()
            
            live_df = pd.DataFrame(fetched_data)
            print(f"\nLive market data fetched successfully with {len(live_df)} records!")
            return live_df
            
        except Exception as e:
            print(f"Error converting live market data to DataFrame: {str(e)}")
            return pd.DataFrame()