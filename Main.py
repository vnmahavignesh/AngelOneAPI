import os
import time
from datetime import datetime
from Login import Login
from Masterlist import Masterlist
from Historic import HistoricalDataManager
import pandas as pd
from OptionGreeks import OptionGreeksManager

session_data = None
token_df = None
day_open = int(os.getenv("DAY_OPEN"))
strike_range = int(os.getenv("STRIKE_LEVELS"))
values = [day_open + i * 50 for i in range(-strike_range, strike_range + 1)]
print("Values:", values)


def save_historical_data_to_csv(historical_manager, token_values, day_open, fromdate, todate, exchange='NFO', interval='ONE_MINUTE', output_dir='.'):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{day_open}_{timestamp}.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    first_write = True

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
            hist_df = historical_manager.get_historical_data(params)
            if hist_df.empty:
                continue

            hist_df['strikeprice'] = int(strike / 100)
            hist_df['symbol'] = symbol
            hist_df['token'] = token
            hist_df = hist_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'strikeprice', 'symbol', 'token']]

            hist_df.to_csv(csv_path, mode='a', index=False, header=first_write)
            first_write = False

    return csv_path

if __name__ == "__main__":
    # Record the start time
    start_time = time.time()

    """------------------------------------------------- Masterlist -------------------------------------------------------"""
    master = Masterlist()  # Create an instance of the Masterlist class to fetch the master list data
    # Get the token DataFrame from the master list instance and print it
    token_df = master.get_token_df()
    # print(token_df)
    print(f"\nMasterlist loaded in {time.time() - start_time:.2f} seconds")
    # Print a success message with the number of records fetched
    print(f"\nMasterlist successfully fetched with {len(token_df)} records!")
    token_values = master.get_nifty_strike_map(os.getenv("EXPIRY_DATE"), values)
    print("\nToken values for NIFTY strikes at expiry "+os.getenv("EXPIRY_DATE")+":")
    for strike, records in token_values.items():
        print(f"Strike: {strike}")
        for record in records:
            print(f"  Symbol: {record['symbol']}, Token: {record['token']}")

    """------------------------------------------------ End of Masterlist --------------------------------------------------"""

    """-------------------------------------------------- Login to the API -----------------------------------------------"""

    # Create an instance of the LoginManager class and login to the API
    login_manager = Login()
    # Call the login method and store the session data and print it
    session_data = login_manager.login()
    print("\nSession Data: \n", session_data)

    # Check if login was successful and print the smart connect object, otherwise print a failure message
    # Note: The smart connect object is only printed if the login is successful, otherwise it will not be available
    # This is because the smart connect object is only created and returned in the session data if the login is successful. If the login fails, the session data will not contain a valid smart connect object, and attempting to print it would result in an error. Therefore, we check for a successful login before trying to access and print the smart connect object.
    if session_data['status'] == 'success':
        print("\nLogin successful!")
        # Print the time taken to login
        print(f"\nLogin successful! in {time.time() - start_time:.2f} seconds")

        # Get the smart connect object from the session data
        smart_connect = session_data['connection']
        print("\nSmart Connect Object:\n", smart_connect)
    else:
        print("\nLogin failed!")

    """------------------------------------------------- End of Login --------------------------------------------------"""

    """------------------------------------------------- Historical Data Fetching --------------------------------------------------"""
    # Check if login was successful before attempting to fetch historical data, otherwise print a failure message
    # Check if the login was successful before attempting to fetch historical data
    if session_data['status'] == 'success':
        # Create an instance of the HistoricalDataManager class using the smart connect object from the session data
        data_manager = HistoricalDataManager(smart_connect)
        if data_manager:
            output_csv = save_historical_data_to_csv(
                data_manager,
                token_values,
                day_open,
                fromdate=os.getenv("FROM_DATE") + " 09:15",
                todate=os.getenv("TO_DATE") + " 15:30",
                exchange="NFO",
                interval="ONE_MINUTE",
                output_dir='.'
            )
            print(f"\nHistorical candle data saved to: {output_csv}")
        else:
            print("\nFailed to fetch historic data")
    else:
        print("\nHistoric Data is not available")

    # Print the time taken to fetch historical data
    print(f"\nHistorical data fetched in {time.time() - start_time:.2f} seconds\n")

    """------------------------------------------------- End of Historical Data Fetching --------------------------------------------------"""

    # """------------------------------------------------- Option Greeks Fetching --------------------------------------------------"""
    # # Check if login was successful before attempting to fetch option Greeks data, otherwise print a failure message
    # if session_data['status'] == 'success':
    #     # Create an instance of the OptionGreeksManager class using the smart connect object from the session data
    #     option_greeks_manager = OptionGreeksManager(smart_connect)
    #     if option_greeks_manager:
    #         option_greeks_params = {
    #             "name": "NIFTY",
    #             "expirydate": "30MAR2026"
    #         }
    #         global_option_greeks = option_greeks_manager.get_option_greeks(
    #             # Call the get_option_greeks method with the specified parameters and store the result in a variable
    #             option_greeks_params)

    #         # Sort by tradeVolume in descending order (combining CE and PE)
    #         if not global_option_greeks.empty and 'tradeVolume' in global_option_greeks.columns:
    #             global_option_greeks = global_option_greeks.sort_values(
    #                 'tradeVolume', ascending=False)

    #         print("\nOption Greeks Data:")
    #         # print(global_option_greeks)
    #     else:
    #         print("\nOption Greeks manager not available")
    # else:
    #     print("\nOption Greeks data is not available")

    # # Print the time taken to fetch option Greeks data
    # print(f"\nOption Greeks data fetched in {time.time() - start_time:.2f} seconds\n")

    # """------------------------------------------------- End of Option Greeks Fetching --------------------------------------------------"""
