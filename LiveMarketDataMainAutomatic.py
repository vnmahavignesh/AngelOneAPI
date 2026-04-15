import os
import time
from datetime import datetime, timezone, timedelta
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


def wait_for_market_start():
    """Wait until market opens at 9:15 AM IST"""
    ist_offset = timedelta(hours=5, minutes=30)
    market_start_time = 9, 15  # 9:15 AM IST

    while True:
        now_ist = datetime.now(timezone.utc) + ist_offset
        current_hour = now_ist.hour
        current_minute = now_ist.minute

        # Check if current time is >= 9:15 AM
        if current_hour > market_start_time[0] or (current_hour == market_start_time[0] and current_minute >= market_start_time[1]):
            print(
                f"\nMarket is now open! Starting data fetch at {now_ist.strftime('%H:%M:%S')} IST")
            break
        else:
            # Calculate wait time until market opens
            market_open_time = now_ist.replace(
                hour=market_start_time[0], minute=market_start_time[1], second=0, microsecond=0)
            wait_seconds = (market_open_time - now_ist).total_seconds()
            wait_minutes = int(wait_seconds // 60)
            wait_seconds_remain = int(wait_seconds % 60)

            print(
                f"Market not open yet. Current time: {now_ist.strftime('%H:%M:%S')} IST")
            print(
                f"Waiting {wait_minutes} minutes and {wait_seconds_remain} seconds until 9:15 AM IST...")
            time.sleep(60)  # Check every minute


def is_market_open():
    """Check if market is currently open (9:15 AM to 3:30 PM IST)"""
    ist_offset = timedelta(hours=5, minutes=30)
    now_ist = datetime.now(timezone.utc) + ist_offset
    current_time = now_ist.time()

    market_start = datetime.strptime("09:15", "%H:%M").time()
    market_end = datetime.strptime("15:30", "%H:%M").time()

    return market_start <= current_time <= market_end


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
          os.getenv("EXPIRY_DATE") + ":")

    # print the total number of records fetched for the token values
    total_records = sum(len(records) for records in token_values.values())
    # Print the total number of records fetched for the specified expiry date and strike levels
    print(f"Total records: {token_values}")

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

        # Extract tokens from token_values
        exchange_tokens_list = []
        for strike, records in token_values.items():
            for record in records:
                exchange_tokens_list.append(str(record['token']))
        exchange_tokens = {"NFO": exchange_tokens_list}

        # Define market hours in IST (UTC+5:30)
        ist_offset = timedelta(hours=5, minutes=30)

        # Wait for market to open at 9:15 AM IST
        print("\n" + "="*60)
        print("Waiting for market to open at 9:15 AM IST...")
        print("="*60)
        wait_for_market_start()

        # Define end time: 3:30 PM IST (UTC+5:30)
        end_time = datetime.now(timezone.utc) + ist_offset
        end_time = end_time.replace(
            hour=15, minute=30, second=0, microsecond=0)

        # CSV filename: Nifty_day_open_yyyymmdd.csv
        current_date = datetime.now().strftime('%Y%m%d')
        csv_filename = f"Nifty_{day_open}_{current_date}.csv"
        csv_path = os.path.join('.', csv_filename)

        # Define columns to remove
        columns_to_remove = ['exchange', 'symbolToken', 'lastTradeQty', 'netChange', 'percentChange', 'avgPrice',
                             'lowerCircuit', 'upperCircuit', 'exchTradeTime', '52WeekLow', '52WeekHigh', 'depth', 'timestamp']

        # Loop every minute until 3:30 PM IST
        iteration = 0
        while datetime.now(timezone.utc) + ist_offset < end_time:
            # Verify market is still open (safety check)
            if not is_market_open():
                print("Market is closed. Stopping data fetch.")
                break

            # Fetch and display data
            live_df = live_data_manager.get_live_market_data_as_dataframe(
                "FULL", exchange_tokens)

            if not live_df.empty:
                # Add timestamp column (for reference but will be removed before saving)
                live_df['timestamp'] = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')

                # Remove specified columns (only if they exist in the dataframe)
                columns_to_drop = [
                    col for col in columns_to_remove if col in live_df.columns]
                if columns_to_drop:
                    live_df = live_df.drop(columns=columns_to_drop)
                    # print(f"\nRemoved columns: {columns_to_drop}")

                # Check if file exists to determine if header should be written
                file_exists = os.path.exists(csv_path)

                # Append to CSV without the removed columns
                live_df.to_csv(csv_path, mode='a', index=False,
                               header=not file_exists)

                iteration += 1
                current_time_ist = (datetime.now(
                    timezone.utc) + ist_offset).strftime('%H:%M:%S')
                print(
                    f"[Iteration {iteration}] Data appended to {csv_path} at {current_time_ist} IST")
                # print(f"Columns saved ({len(live_df.columns)} columns): {list(live_df.columns)}")
                # print(f"Rows saved: {len(live_df)}")
            else:
                current_time_ist = (datetime.now(
                    timezone.utc) + ist_offset).strftime('%H:%M:%S')
                print(f"No live market data fetched at {current_time_ist} IST")

            # Sleep for 60 seconds
            time.sleep(60)

        print(f"\n{'='*60}")
        print(f"Loop ended. Final CSV saved at: {csv_path}")
        print(f"Total iterations completed: {iteration}")
        print(f"Total runtime: {time.time() - start_time:.2f} seconds")
        print(f"{'='*60}")
    else:
        print("\nLive Market Data is not available")

    print(f"\nCSV file will be saved as: {csv_filename}")
    print(f"Day open value used in filename: {day_open}")
    print(
        f"\nLive market data fetching completed in {time.time() - start_time:.2f} seconds\n")

    """------------------------------------------------- End of Live Market Data Fetching -----------------------------------"""
