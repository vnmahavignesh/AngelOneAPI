import os
import time
from LiveMarketData import LiveMarketDataManager
from Login import Login
from Masterlist import Masterlist
from Historic import HistoricalDataManager
import pandas as pd

"""---------------------------------------------Global Variables Start-------------------------------------------------------"""

# Global variables to store session data, token DataFrame, day open price, and strike range
session_data = None
token_df = None
day_open = int(os.getenv("DAY_OPEN"))
strike_range = int(os.getenv("STRIKE_LEVELS"))

# Calculate the strike values based on the day open and strike range
# Generate strike values around the day open price, with a step of 50, based on the specified strike range (e.g., if strike_range is 15, it will generate 31 strike values from day_open - 750 to day_open + 750)
values = [day_open + i * (int(os.getenv("STRIKE_STEP")))
          for i in range(-strike_range, strike_range + 1)]
print("Values:", values)  # Print the calculated strike values for verification

"""---------------------------------------------Global Variables End-------------------------------------------------------"""

"""---------------------------------------------Main Execution Start-------------------------------------------------------"""
if __name__ == "__main__":

    # Record the start time
    start_time = time.time()

    """------------------------------------------------- Masterlist -------------------------------------------------------"""
    master = Masterlist()  # Create an instance of the Masterlist class to fetch the master list data
    # Get the token DataFrame from the master list instance and print it
    token_df = master.get_token_df()
    # print(token_df) # Print the token DataFrame to verify that it has been loaded correctly
    # Print the time taken to load the master list data
    print(f"\nMasterlist loaded in {time.time() - start_time:.2f} seconds")
    # Print a success message with the number of records fetched
    print(f"\nMasterlist successfully fetched with {len(token_df)} records!")

    # Get the token values for the specified expiry date and strike levels, and print them
    token_values = master.get_nifty_strike_map(
        os.getenv("EXPIRY_DATE"), values, os.getenv("INSTRUMENT_NAME"))
    # print the token values for the specified expiry date and strike levels
    print("\nToken values for NIFTY strikes at expiry " +
          os.getenv("EXPIRY_DATE")+":")

    # print the total number of records fetched for the token values
    total_records = sum(len(records) for records in token_values.values())
    # Print the total number of records fetched for the specified expiry date and strike levels
    print(f"Total records: {total_records}")

    # for strike, records in token_values.items(): # Print the strike price and corresponding symbol/token records for each strike level
    #     print(f"Strike: {strike}")
    #     for record in records:
    #         print(f"  Symbol: {record['symbol']}, Token: {record['token']}")

    """------------------------------------------------ End of Masterlist -------------------------------------------------"""

    """-------------------------------------------------- Login to the API ------------------------------------------------"""
    login_manager = Login()  # Create an instance of the LoginManager class and login to the API
    # Call the login method and store the session data and print it
    session_data = login_manager.login()
    # print("\nSession Data: \n", session_data)

    # Check if login was successful and print the smart connect object, otherwise print a failure message
    # Note: The smart connect object is only printed if the login is successful, otherwise it will not be available
    # This is because the smart connect object is only created and returned in the session data if the login is successful. If the login fails, the session data will not contain a valid smart connect object, and attempting to print it would result in an error. Therefore, we check for a successful login before trying to access and print the smart connect object.
    if session_data['status'] == 'success':
        # Print a success message if the login was successful
        print("\nLogin successful!")
        # Print the time taken to login
        print(f"\nLogin successful! in {time.time() - start_time:.2f} seconds")

        # Get the smart connect object from the session data
        smart_connect = session_data['connection']
        # Print the smart connect object to verify that it has been created successfully
        print("\nSmart Connect Object:\n", smart_connect)
    else:
        # Print a failure message if the login was not successful
        print("\nLogin failed!")

    """------------------------------------------------- End of Login -------------------------------------------------------"""

    """------------------------------------------------- Live Market Data Fetching ------------------------------------------"""
    if session_data['status'] == 'success':
        # Create an instance of LiveMarketDataManager
        live_data_manager = LiveMarketDataManager(smart_connect)
        
        # Method 1: Get data for specific tokens
        exchange_tokens = {"NFO": ["40803"]}  # You can add more tokens here
        
        # Fetch and display data
        live_df = live_data_manager.get_live_market_data_as_dataframe("FULL", exchange_tokens)
        
        if not live_df.empty:
            # Print formatted instrument data
            live_data_manager.print_instrument_data(live_df)
            
            # Extract and display only key fields
            key_fields_df = live_data_manager.extract_key_fields(live_df)
            print("\n\nKey Fields Data:")
            print(key_fields_df)
            
            # Option 1: Get NFO data only
            nfo_data = live_df[live_df['exchange'] == 'NFO']
            if not nfo_data.empty:
                print("\n\nNFO Data:")
                print(nfo_data[['exchange', 'tradingSymbol', 'open', 'high', 'low', 'close', 'ltp', 'percentChange']])
            
            # Option 2: Get data for a single instrument
            instrument_data = live_data_manager.get_instrument_data("NFO", "40803")
            if instrument_data:
                print("\n\nSingle Instrument Data:")
                print(f"Symbol: {instrument_data.get('tradingSymbol')}")
                print(f"LTP: {instrument_data.get('ltp')}")
                print(f"Open Interest: {instrument_data.get('opnInterest')}")
            
            # Option 3: Save to CSV
            csv_path = live_data_manager.save_live_market_data_to_csv(
                exchange_tokens, 
                output_dir='.', 
                prefix='nifty_live_data'
            )
            if csv_path:
                print(f"\nLive data saved to: {csv_path}")
        else:
            print("\nNo live market data fetched")
    else:
        print("\nLive Market Data is not available")

    print(f"\nLive market data fetched in {time.time() - start_time:.2f} seconds\n")

    """------------------------------------------------- End of Live Market Data Fetching -----------------------------------"""
