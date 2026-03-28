from typing import Any, Dict
from SmartApi import SmartConnect
import pandas as pd


class OptionGreeksManager:
    """Handles fetching and processing of option Greeks data"""

    def __init__(self, smart_connect: SmartConnect):
        self.smart_connect = smart_connect

    def get_option_greeks(self, params: Dict[str, Any]) -> pd.DataFrame:
        """Fetch option Greeks data and return as DataFrame sorted by tradeVolume in descending order"""
        try:
            response = self.smart_connect.optionGreek(params)

            if not response or 'data' not in response:
                return pd.DataFrame()

            response_df = pd.DataFrame(response['data'])

            # Conversion dictionary: column -> (type, is_percentage)
            column_conversions = {
                'strikePrice': ('int', False),
                'delta': ('float', False),
                'gamma': ('float', False),
                'theta': ('float', False),
                'vega': ('float', False),
                'impliedVolatility': ('float', False),
                'tradeVolume': ('int', False),
            }

            for column, (conv_type, *args) in column_conversions.items():
                if column in response_df.columns:
                    try:
                        if conv_type == 'int':
                            if column == 'strikePrice':
                                response_df[column] = (pd.to_numeric(response_df[column], errors='coerce')
                                                       .fillna(0)
                                                       .astype(float)
                                                       .mul(100)
                                                       .astype(int))
                            else:
                                response_df[column] = pd.to_numeric(
                                    response_df[column], errors='coerce').fillna(0).astype(int)
                        elif conv_type == 'float':
                            response_df[column] = pd.to_numeric(
                                response_df[column], errors='coerce').fillna(0.0).astype(float)
                    except Exception as e:
                        print(f"Error converting column {column}: {str(e)}")
                        continue

            return response_df

        except Exception as e:
            print(f"Error processing option Greeks: {str(e)}")
            return pd.DataFrame()