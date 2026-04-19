import os
import sys
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


class Tee:
    """Helper class to write to multiple outputs (console and file)"""
    def __init__(self, *outputs):
        self.outputs = outputs
    
    def write(self, message):
        for output in self.outputs:
            output.write(message)
            output.flush()
    
    def flush(self):
        for output in self.outputs:
            output.flush()


def setup_github_actions_environment():
    """Handle GitHub Actions specific settings"""
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    
    if is_github_actions:
        print("="*60)
        print("Running in GitHub Actions environment")
        print("="*60)
        
        # Ensure we have write permissions
        workspace = os.getenv('GITHUB_WORKSPACE', '.')
        os.chdir(workspace)
        print(f"Working directory: {os.getcwd()}")
        
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Create log file for this run
        log_filename = f'logs/market_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        log_file = open(log_filename, 'a')
        
        # Redirect output to both console and file
        sys.stdout = Tee(sys.stdout, log_file)
        sys.stderr = Tee(sys.stderr, log_file)
        
        print(f"Log file created: {log_filename}")
        print(f"Current time (UTC): {datetime.now(timezone.utc)}")
        print(f"GitHub Run ID: {os.getenv('GITHUB_RUN_ID', 'N/A')}")
        
    return is_github_actions


# Call at the beginning of main execution
is_github_actions = setup_github_actions_environment()


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
    
    try:
        # Record the start time
        start_time = time.time()
        print(f"\n{'='*60}")
        print(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        """------------------------------------------------- Masterlist -------------------------------------------------------"""
        print("\n[1/5] Loading Masterlist...")
        master = Masterlist()  # Create an instance of the Masterlist class to fetch the master list data
        # Get the token DataFrame from the master list instance and print it
        token_df = master.get_token_df()
        # Print the time taken to load the master list data
        print(f"\nMasterlist loaded in {time.time() - start_time:.2f} seconds")
        # Print a success message with the number of records fetched
        print(f"Masterlist successfully fetched with {len(token_df)} records!")

        # Get the token values for the specified expiry date and strike levels, and print them
        token_values = master.get_nifty_strike_map(
            os.getenv("EXPIRY_DATE"), values, os.getenv("INSTRUMENT_NAME"))
        # print the token values for the specified expiry date and strike levels
        print("\nToken values for NIFTY strikes at expiry " +
              os.getenv("EXPIRY_DATE") + ":")

        # print the total number of records fetched for the token values
        total_records = sum(len(records) for records in token_values.values())
        print(f"Total strike levels: {len(token_values)}")
        print(f"Total token records: {total_records}")

        """------------------------------------------------ End of Masterlist -------------------------------------------------"""

        """-------------------------------------------------- Login to the API ------------------------------------------------"""
        print("\n[2/5] Logging into AngelOne API...")
        login_manager = Login()  # Create an instance of the LoginManager class and login to the API
        # Call the login method and store the session data and print it
        session_data = login_manager.login()

        # Check if login was successful and print the smart connect object, otherwise print a failure message
        if session_data['status'] == 'success':
            # Print a success message if the login was successful
            print("Login successful!")
            # Print the time taken to login
            print(f"Login completed in {time.time() - start_time:.2f} seconds")

            # Get the smart connect object from the session data
            smart_connect = session_data['connection']
            print("Smart Connect object created successfully")
        else:
            error_msg = session_data.get('message', 'Unknown error')
            error_code = session_data.get('errorcode', 'N/A')
            raise Exception(f"Login failed! Error: {error_msg}, Code: {error_code}")

        """------------------------------------------------- End of Login -------------------------------------------------------"""

        """------------------------------------------------- Live Market Data Fetching ------------------------------------------"""
        print("\n[3/5] Setting up live market data fetching...")
        
        # Create an instance of LiveMarketDataManager
        live_data_manager = LiveMarketDataManager(smart_connect)

        # Extract tokens from token_values
        exchange_tokens_list = []
        for strike, records in token_values.items():
            for record in records:
                exchange_tokens_list.append(str(record['token']))
        exchange_tokens = {"NFO": exchange_tokens_list}
        
        print(f"Total tokens to fetch: {len(exchange_tokens_list)}")

        # Define market hours in IST (UTC+5:30)
        ist_offset = timedelta(hours=5, minutes=30)

        # Wait for market to open at 9:15 AM IST
        print("\n[4/5] Waiting for market to open...")
        print("="*60)
        print("Waiting for market to open at 9:15 AM IST...")
        print("="*60)
        wait_for_market_start()

        # Define end time: 3:30 PM IST (UTC+5:30) changed to 3.15 PM
        end_time = datetime.now(timezone.utc) + ist_offset
        end_time = end_time.replace(
            hour=15, minute=15, second=0, microsecond=0)

        # CSV filename: Nifty_day_open_yyyymmdd.csv
        current_date = datetime.now().strftime('%Y%m%d')
        csv_filename = f"Nifty_{day_open}_{current_date}.csv"
        csv_path = os.path.join('.', csv_filename)

        # Define columns to remove
        columns_to_remove = ['exchange', 'symbolToken', 'lastTradeQty', 'netChange', 'percentChange', 'avgPrice',
                             'lowerCircuit', 'upperCircuit', 'exchTradeTime', '52WeekLow', '52WeekHigh', 'depth', 'timestamp']

        print(f"\n[5/5] Starting data collection...")
        print(f"CSV file will be saved as: {csv_filename}")
        print(f"Data collection will run until 3:30 PM IST")
        print(f"{'='*60}\n")

        # Loop every minute until 3:30 PM IST
        iteration = 0
        successful_fetches = 0
        failed_fetches = 0
        
        while datetime.now(timezone.utc) + ist_offset < end_time:
            # Verify market is still open (safety check)
            if not is_market_open():
                print("Market is closed. Stopping data fetch.")
                break

            try:
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

                    # Check if file exists to determine if header should be written
                    file_exists = os.path.exists(csv_path)

                    # Append to CSV without the removed columns
                    live_df.to_csv(csv_path, mode='a', index=False,
                                   header=not file_exists)

                    iteration += 1
                    successful_fetches += 1
                    current_time_ist = (datetime.now(
                        timezone.utc) + ist_offset).strftime('%H:%M:%S')
                    print(
                        f"[Iteration {iteration}] ✅ Data appended to {csv_filename} at {current_time_ist} IST (Rows: {len(live_df)})")
                else:
                    failed_fetches += 1
                    current_time_ist = (datetime.now(
                        timezone.utc) + ist_offset).strftime('%H:%M:%S')
                    print(f"[Iteration {iteration+1}] ⚠️ No live market data fetched at {current_time_ist} IST")

            except Exception as e:
                failed_fetches += 1
                print(f"[Iteration {iteration+1}] ❌ Error fetching data: {str(e)}")

            # Sleep for 60 seconds
            time.sleep(60)

        # Print final summary
        print(f"\n{'='*60}")
        print("DATA COLLECTION COMPLETED")
        print(f"{'='*60}")
        print(f"Final CSV saved at: {csv_path}")
        print(f"Total iterations completed: {iteration}")
        print(f"Successful fetches: {successful_fetches}")
        print(f"Failed fetches: {failed_fetches}")
        print(f"Total runtime: {time.time() - start_time:.2f} seconds")
        
        # Verify CSV file was created and has data
        if os.path.exists(csv_path):
            file_size = os.path.getsize(csv_path)
            df_check = pd.read_csv(csv_path)
            print(f"CSV file size: {file_size} bytes")
            print(f"Total rows in CSV: {len(df_check)}")
            print(f"Total columns in CSV: {len(df_check.columns)}")
        else:
            print("⚠️ WARNING: CSV file was not created!")
            
        print(f"{'='*60}")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"❌ FATAL ERROR: {str(e)}")
        print(f"{'='*60}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # GitHub Actions specific cleanup
        if is_github_actions:
            print("\n✅ GitHub Actions execution completed")
            
            # List created files for debugging
            print("\nFiles created in this run:")
            for file in os.listdir('.'):
                if file.startswith('Nifty_') and file.endswith('.csv'):
                    file_size = os.path.getsize(file)
                    print(f"  - {file} ({file_size} bytes)")
            
            print(f"\nLog files:")
            for file in os.listdir('logs'):
                if file.startswith('market_data_'):
                    print(f"  - logs/{file}")
            
            print(f"\n{'='*60}")
            print("GitHub Action completed successfully")
            print(f"{'='*60}")
        
        print(f"\nScript finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {time.time() - start_time:.2f} seconds\n")