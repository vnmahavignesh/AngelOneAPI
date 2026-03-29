import os
import time
from Login import Login
from Masterlist import Masterlist
from Historic import HistoricalDataManager
import pandas as pd
from OptionGreeks import OptionGreeksManager

"""---------------------------------------------Global Variables Start-------------------------------------------------------"""

# Global variables to store session data, token DataFrame, day open price, and strike range
session_data = None
token_df = None
day_open = int(os.getenv("DAY_OPEN"))
strike_range = int(os.getenv("STRIKE_LEVELS"))

# Calculate the strike values based on the day open and strike range
values = [day_open + i * (int(os.getenv("STRIKE_STEP"))) for i in range(-strike_range, strike_range + 1)] # Generate strike values around the day open price, with a step of 50, based on the specified strike range (e.g., if strike_range is 15, it will generate 31 strike values from day_open - 750 to day_open + 750)
print("Values:", values) # Print the calculated strike values for verification

"""---------------------------------------------Global Variables End-------------------------------------------------------"""

"""---------------------------------------------Main Execution Start-------------------------------------------------------"""
if __name__ == "__main__":

    # Record the start time
    start_time = time.time()

    """------------------------------------------------- Masterlist -------------------------------------------------------"""
    master = Masterlist()  # Create an instance of the Masterlist class to fetch the master list data   
    token_df = master.get_token_df()  # Get the token DataFrame from the master list instance and print it
    # print(token_df) # Print the token DataFrame to verify that it has been loaded correctly
    print(f"\nMasterlist loaded in {time.time() - start_time:.2f} seconds") # Print the time taken to load the master list data    
    print(f"\nMasterlist successfully fetched with {len(token_df)} records!") # Print a success message with the number of records fetched

    token_values = master.get_nifty_strike_map(os.getenv("EXPIRY_DATE"), values, os.getenv("INSTRUMENT_NAME")) # Get the token values for the specified expiry date and strike levels, and print them
    print("\nToken values for NIFTY strikes at expiry "+os.getenv("EXPIRY_DATE")+":") #print the token values for the specified expiry date and strike levels
    
    total_records = sum(len(records) for records in token_values.values()) #print the total number of records fetched for the token values
    print(f"Total records: {total_records}") # Print the total number of records fetched for the specified expiry date and strike levels

    # for strike, records in token_values.items(): # Print the strike price and corresponding symbol/token records for each strike level
    #     print(f"Strike: {strike}")
    #     for record in records:
    #         print(f"  Symbol: {record['symbol']}, Token: {record['token']}")

    """------------------------------------------------ End of Masterlist -------------------------------------------------"""

    """-------------------------------------------------- Login to the API ------------------------------------------------"""    
    login_manager = Login() # Create an instance of the LoginManager class and login to the API    
    session_data = login_manager.login() # Call the login method and store the session data and print it
    # print("\nSession Data: \n", session_data)

    # Check if login was successful and print the smart connect object, otherwise print a failure message
    # Note: The smart connect object is only printed if the login is successful, otherwise it will not be available
    # This is because the smart connect object is only created and returned in the session data if the login is successful. If the login fails, the session data will not contain a valid smart connect object, and attempting to print it would result in an error. Therefore, we check for a successful login before trying to access and print the smart connect object.
    if session_data['status'] == 'success':
        print("\nLogin successful!")                                            # Print a success message if the login was successful
        print(f"\nLogin successful! in {time.time() - start_time:.2f} seconds") # Print the time taken to login
        
        smart_connect = session_data['connection'] # Get the smart connect object from the session data
        print("\nSmart Connect Object:\n", smart_connect) # Print the smart connect object to verify that it has been created successfully
    else:
        print("\nLogin failed!") # Print a failure message if the login was not successful

    """------------------------------------------------- End of Login -------------------------------------------------------"""

    """------------------------------------------------- Historical Data Fetching -------------------------------------------"""
    # Check if login was successful before attempting to fetch historical data, otherwise print a failure message    
    if session_data['status'] == 'success':
        # Create an instance of the HistoricalDataManager class using the smart connect object from the session data
        data_manager = HistoricalDataManager(smart_connect)
        if data_manager:
            # Call the save_historical_data_to_csv method with the specified parameters and store the output CSV path in a variable
            output_csv = data_manager.save_historical_data_to_csv(
                token_values,
                day_open,
                fromdate=os.getenv("FROM_DATE"),
                todate=os.getenv("TO_DATE"),
                exchange=os.getenv("EXCHANGE"),
                interval=os.getenv("INTERVAL"),
                output_dir='.'
            ) 
            print(f"\nHistorical candle data saved to: {output_csv}") # Print the output CSV path where the historical data has been saved
        else:
            print("\nFailed to fetch historic data") # Print a failure message if the historical data manager instance could not be created
    else:
        print("\nHistoric Data is not available") # Print a failure message if the login was not successful and historical data cannot be fetched

    print(f"\nHistorical data fetched in {time.time() - start_time:.2f} seconds\n") # Print the time taken to fetch historical data

    """------------------------------------------------- End of Historical Data Fetching --------------------------------------------------"""

    """------------------------------------------------- Option Greeks Fetching -----------------------------------------------------------"""
    # Check if login was successful before attempting to fetch option Greeks data, otherwise print a failure message
    if session_data['status'] == 'success':
        # Create an instance of the OptionGreeksManager class using the smart connect object from the session data
        option_greeks_manager = OptionGreeksManager(smart_connect)
        if option_greeks_manager:
            option_greeks_params = {
                "name": "NIFTY",
                "expirydate": "30MAR2026"
            }
            global_option_greeks = option_greeks_manager.get_option_greeks(
                # Call the get_option_greeks method with the specified parameters and store the result in a variable
                option_greeks_params)

            # Sort by tradeVolume in descending order (combining CE and PE)
            if not global_option_greeks.empty and 'tradeVolume' in global_option_greeks.columns:
                global_option_greeks = global_option_greeks.sort_values(
                    'tradeVolume', ascending=False)

            print("\nOption Greeks Data:")
            # print(global_option_greeks)
        else:
            print("\nOption Greeks manager not available")
    else:
        print("\nOption Greeks data is not available")

    # Print the time taken to fetch option Greeks data
    print(
        f"\nOption Greeks data fetched in {time.time() - start_time:.2f} seconds\n")

    """------------------------------------------------- End of Option Greeks Fetching --------------------------------------------------"""
