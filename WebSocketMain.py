import os
import time
import datetime
from AngelOneWebSocket1m import AngelOneWebSocket
from Login import Login
from Masterlist import Masterlist

"""---------------------------------------------Global Variables Start-------------------------------------------------------"""

session_data = None
token_df = None
day_open = int(os.getenv("DAY_OPEN"))
strike_range = int(os.getenv("STRIKE_LEVELS"))

# Calculate strike values
values = [day_open + i * (int(os.getenv("STRIKE_STEP")))
          for i in range(-strike_range, strike_range + 1)]
print("Strike values:", values)

def wait_for_market_start():
    """Wait until market start time (9:15 AM)"""
    print("Waiting for market to start at 9:15 AM...")
    while True:
        now = datetime.datetime.now()
        current_time = now.time()
        market_start = datetime.time(9, 15, 0)
        
        if current_time >= market_start:
            print(f"Market opened at {now.strftime('%H:%M:%S')}")
            return True
        
        # Calculate seconds until market start
        next_start = datetime.datetime.combine(now.date(), market_start)
        wait_seconds = (next_start - now).total_seconds()
        
        if wait_seconds > 0:
            print(f"Market starts in {wait_seconds/60:.1f} minutes...")
            time.sleep(min(wait_seconds, 300))  # Sleep in 5-minute intervals

"""---------------------------------------------Main Execution-------------------------------------------------------"""
if __name__ == "__main__":

    start_time = time.time()
    
    # Wait for market to start
    wait_for_market_start()

    """------------------------------------------------- Masterlist -------------------------------------------------------"""
    master = Masterlist()
    token_df = master.get_token_df()
    print(f"\nMasterlist loaded in {time.time() - start_time:.2f} seconds")
    print(f"Total records: {len(token_df)}")

    # Get token values for strikes
    token_values = master.get_nifty_strike_map(
        os.getenv("EXPIRY_DATE"), values, os.getenv("INSTRUMENT_NAME"))
    
    # Extract tokens and create mappings
    all_tokens = []
    token_to_symbol = {}
    token_to_strike = {}
    
    for strike, records in token_values.items():
        for record in records:
            all_tokens.append(record['token'])
            token_to_symbol[record['token']] = record['symbol']
            token_to_strike[record['token']] = strike
    
    print(f"\nTotal tokens to subscribe: {len(all_tokens)}")
    print(f"Sample tokens: {all_tokens[:5]}")

    """-------------------------------------------------- Login -------------------------------------------------------"""
    login_manager = Login()
    session_data = login_manager.login()

    if session_data['status'] != 'success':
        print("Login failed!")
        exit(1)

    print(f"Login successful in {time.time() - start_time:.2f} seconds")
    
    auth_token = session_data['data']['jwtToken']
    feed_token = session_data['data']['feedToken']

    """----------------------------------------------Websocket Connection----------------------------------------------"""
    
    # Create WebSocket instance
    angelone_websocket = AngelOneWebSocket(
        auth_token, feed_token, token_to_symbol, token_to_strike, login_manager)
    
    # Start connection
    angelone_websocket.start_connection()
    time.sleep(3)  # Wait for connection to establish
    
    # Prepare and subscribe to tokens
    nifty_tokens = [{"exchangeType": 2, "tokens": all_tokens}]  # 2 = NFO
    success = angelone_websocket.subscribe(nifty_tokens, mode=3, correlation_id="nifty_stream")
    
    if success:
        print(f"\n✅ Successfully subscribed to {len(all_tokens)} NIFTY tokens")
        print("📊 WebSocket is streaming live data...")
        print(f"💾 Data will be saved to: nifty_websocket_data_YYYYMMDD.csv")
        print("⏰ Data saved every minute (at :00 seconds)")
        print("🛑 Press Ctrl+C to stop\n")
    else:
        print("Failed to subscribe. Exiting...")
        exit(1)

    try:
        # Keep running until market closes
        market_end_time = datetime.time(15, 31, 0)
        
        while True:
            current_time = datetime.datetime.now()
            
            # Check if market has closed (after 3:31 PM)
            if current_time.time() > market_end_time:
                print(f"\n🏁 Market closed at {current_time.strftime('%H:%M:%S')}. Stopping...")
                break
            
            # Print status every 5 minutes
            if int(time.time()) % 300 == 0:
                print(f"\n[Status] {current_time.strftime('%H:%M:%S')} - Connected: {angelone_websocket.is_connected}")
                if angelone_websocket.latest_data is not None:
                    latest = angelone_websocket.latest_data.iloc[0]
                    print(f"[Status] Latest data - {latest['symbol']} LTP: {latest['ltp']}")
                print(f"[Status] Minute data collected: {len(angelone_websocket.current_minute_data)} ticks this minute\n")
            
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n\n🛑 User interrupted. Shutting down...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        angelone_websocket.stop_connection()
        print(f"\n✅ WebSocket connection closed.")
        print(f"📁 Data saved in: nifty_websocket_data_*.csv")
        print(f"⏱️  Total runtime: {(time.time() - start_time) / 60:.2f} minutes")