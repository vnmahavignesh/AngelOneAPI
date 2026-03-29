import requests
import pandas as pd

MASTER_URL = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'


class Masterlist:

    def __init__(self):
        d = requests.get(MASTER_URL).json()
        self.token_df = pd.DataFrame.from_dict(d)
        self.token_df['expiry'] = pd.to_datetime(
            self.token_df['expiry'], format='mixed').apply(lambda x: x.date())
        self.token_df = self.token_df.astype({'strike': 'float64'})

    def get_token_df(self):
        return self.token_df

    def get_nifty_strike_map(self, expiry_date, strike_values, name):
        """Return symbol/token records for NIFTY strikes at a given expiry.

        Args:
            expiry_date (str|datetime.date): Expiry date, e.g. '30-03-2026'.
            strike_values (iterable): Strike prices to filter, e.g. [23000, 23050].

        Returns:
            dict: {strike: [{'symbol': ..., 'token': ...}, ...], ...}
        """
        if isinstance(expiry_date, str):
            expiry_date = pd.to_datetime(expiry_date, dayfirst=True).date()

        df = self.token_df.copy()
        # Filter by instrument name (e.g., NIFTY)
        df = df[df['name'].astype(str).str.strip().str.upper() == name]
        # Filter by expiry date ,it is coming from env variable in string format, so we need to convert it to datetime.date for comparison
        df = df[df['expiry'] == expiry_date]

        # Convert strike values to float, multiply by 100 (since master list has strike multiplied by 100), and sort them
        strike_values = sorted({float(v) * 100 for v in strike_values})
        df = df[df['strike'].isin(strike_values)]

        result = {}
        for strike, group in df.groupby('strike'):
            result[int(strike)] = group[['symbol', 'token']].to_dict('records')

        return result
