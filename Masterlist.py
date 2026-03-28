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