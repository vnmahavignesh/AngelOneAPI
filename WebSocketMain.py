import os
import time
from AngelOneWebSocket import AngelOneWebSocket
from Login import Login
from Masterlist import Masterlist

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

        auth_token = session_data['data']['jwtToken'] # Get the authentication token from the session data and print it
        print("\nAuthentication Token:\n", auth_token) # Print the authentication token to verify that it has been retrieved successfully

        feed_token = session_data['data']['feedToken'] # Get the feed token from the session data and print it
        print("\nFeed Token:\n", feed_token) # Print the feed token to verify that it has been retrieved successfully
    else:
        print("\nLogin failed!") # Print a failure message if the login was not successful

    """------------------------------------------------- End of Login -------------------------------------------------------"""
    
    """----------------------------------------------Websocket Connection and Data Handling---------------------------------------------- """

    angelone_websocket = AngelOneWebSocket(auth_token, feed_token) # Create an instance of the AngelOneWebSocket class using the authentication token and feed token 
    angelone_websocket.start_connection() # Connect to the AngelOne WebSocket using the authentication token and feed token

    nifty_tokens = [{"exchangeType": 2, "tokens": ['54505', '54506']}]
    angelone_websocket.subscribe(nifty_tokens) # Subscribe to the specified NIFTY tokens to receive real-time data updates

    try:
      # Keep the main thread alive
        while True:                  
            time.sleep(10) # Keep the main thread alive

    except KeyboardInterrupt:
            print("\nShutting down...")
            angelone_websocket.stop_connection()           
               
    """----------------------------------------------End of Websocket Connection and Data Handling---------------------------------------------- """
    