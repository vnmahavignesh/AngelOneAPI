from datetime import datetime as dt, time as dtime, timedelta
import logging
import os
import queue
import threading
import time
import sys

from smartWebSocketV2 import SmartWebSocketV2
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Market session guard – reconnection is suppressed after 15:30
# ---------------------------------------------------------------------------
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


def _within_market_hours() -> bool:
    now = dt.now().time()
    return MARKET_OPEN <= now <= MARKET_CLOSE


def _is_market_time() -> bool:
    """Check if current time is within market hours including buffer"""
    now = dt.now().time()
    market_start_buffer = dtime(9, 0)
    market_end_buffer = dtime(15, 45)
    return market_start_buffer <= now <= market_end_buffer


# ---------------------------------------------------------------------------
# SWS factory – always created with a high retry budget
# ---------------------------------------------------------------------------
_SWS_MAX_RETRY = 9999
_SWS_RETRY_DELAY = 5


def _make_sws(auth_token, api_key, client_id, feed_token,
              on_open, on_data, on_error, on_close) -> SmartWebSocketV2:
    """Create a fresh SmartWebSocketV2 with a full retry budget and wired callbacks."""
    sws = SmartWebSocketV2(
        auth_token, api_key, client_id, feed_token,
        max_retry_attempt=_SWS_MAX_RETRY,
        retry_strategy=0,
        retry_delay=_SWS_RETRY_DELAY,
    )
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close
    return sws


class AngelOneWebSocket:
    """Handles WebSocket connections and data streaming with single CSV output."""

    def __init__(self, auth_token: str, feed_token: str,
                 token_to_symbol: dict, token_to_strike: dict,
                 login_manager):
        """
        Initialize WebSocket manager.

        Args:
            auth_token:       JWT token from Login API.
            feed_token:       Feed token from Login API.
            token_to_symbol:  {token_str: symbol_str}
            token_to_strike:  {token_str: strike_int}
            login_manager:    Login instance used to refresh tokens on error.
        """
        self.api_key = os.getenv("API_KEY")
        self.client_id = os.getenv("CLIENT_ID")
        self.auth_token = auth_token
        self.feed_token = feed_token
        self.token_to_symbol = token_to_symbol
        self.token_to_strike = token_to_strike
        self.login_manager = login_manager

        # Throttle token-refresh to at most once per 60 s
        self.last_error_refresh = 0
        self.last_token_refresh = time.time()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        
        # Track last data received time
        self.last_data_time = time.time()

        # CSV Management
        self.csv_filename = None  # Will be set when market starts
        self.csv_header_written = False
        self.last_minute_save = None
        
        # OPTION 1: Store all ticks (tick-by-tick data) - current behavior
        self.current_minute_ticks = []  # Store all ticks for current minute
        
        # OPTION 2: Store aggregated minute data (one record per minute per symbol)
        self.minute_aggregates = {}  # {token: {open, high, low, close, volume, ...}}
        
        # Choose your saving mode:
        self.save_mode = "AGGREGATED"  # Options: "TICK_BY_TICK" or "AGGREGATED"
        
        # Data storage for latest values
        self.latest_values = {}  # Store latest value for each token

        # Connection state
        self.connection_thread = None
        self.subscription_queue = queue.Queue()
        self.is_connected = False
        self.latest_data = None
        self.should_stop = False
        self.heartbeat_thread = None
        self.save_thread = None
        self.ping_thread = None
        self.health_thread = None
        
        # Subscription info
        self.current_token_list = None
        self.current_mode = 3
        self.correlation_id = "nifty_stream"

        # Create initial connection
        self.sws = _make_sws(
            self.auth_token, self.api_key, self.client_id, self.feed_token,
            self._on_open, self._on_data, self._on_error, self._on_close,
        )
        
        # Start all background threads
        self._start_heartbeat_monitor()
        self._start_minute_saver()
        self._start_connection_health_check()

    # ------------------------------------------------------------------
    # Minute Data Saver Thread
    # ------------------------------------------------------------------
    
    def _start_minute_saver(self):
        """Start a thread that saves data at the end of each minute"""
        def save_loop():
            while not self.should_stop:
                try:
                    # Calculate time until next minute boundary
                    now = dt.now()
                    # Next minute at :00 seconds
                    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                    sleep_seconds = (next_minute - now).total_seconds()
                    
                    # Sleep until the next minute boundary
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    
                    # Save data for the completed minute (if any)
                    if not self.should_stop:
                        if self.save_mode == "TICK_BY_TICK":
                            self._save_tick_data()
                        else:
                            self._save_aggregated_data()
                        
                        # Reset for next minute
                        self.current_minute_ticks = []
                        self.minute_aggregates = {}
                        self.last_minute_save = dt.now()
                        
                        print(f"[Saver] Minute data saved at {dt.now().strftime('%H:%M:%S')}")
                    
                except Exception as e:
                    print(f"[Saver] Error: {e}")
                    time.sleep(1)
        
        self.save_thread = threading.Thread(target=save_loop, daemon=True, name="MinuteSaver")
        self.save_thread.start()
        print(f"[WS] Minute data saver thread started (Mode: {self.save_mode})")

    def _save_tick_data(self):
        """Save all ticks from the minute (tick-by-tick data)"""
        try:
            if not self.current_minute_ticks:
                print("[SAVE] No tick data to save this minute")
                return
                
            # Create DataFrame from collected ticks
            minute_df = pd.DataFrame(self.current_minute_ticks)
            
            if minute_df.empty:
                print("[SAVE] DataFrame is empty")
                return

            # Set filename (single file for the day)
            if self.csv_filename is None:
                date_str = dt.now().strftime('%Y%m%d')
                self.csv_filename = f'nifty_tick_data_{date_str}.csv'
            
            # Check if file exists to determine header
            file_exists = os.path.isfile(self.csv_filename)
            
            # Append to CSV
            minute_df.to_csv(self.csv_filename, mode='a', 
                           header=not file_exists, index=False)
            
            if not self.csv_header_written:
                self.csv_header_written = True
                
            print(f"[SAVE] {dt.now().strftime('%H:%M:%S')} - Saved {len(minute_df)} ticks to {self.csv_filename}")
            
        except Exception as e:
            print(f"[SAVE] Error saving tick data: {e}")
            import traceback
            traceback.print_exc()

    def _save_aggregated_data(self):
        """Save one record per symbol per minute (OHLCV data)"""
        try:
            if not self.minute_aggregates:
                print("[SAVE] No aggregated data to save this minute")
                return
            
            # Convert aggregates to list of records
            records = []
            for token, agg in self.minute_aggregates.items():
                if agg.get('tick_count', 0) > 0:  # Only save if we received ticks
                    records.append({
                        'minute': self.last_minute_save.strftime('%Y-%m-%d %H:%M:00') if self.last_minute_save else dt.now().strftime('%Y-%m-%d %H:%M:00'),
                        'token': token,
                        'symbol': agg.get('symbol', ''),
                        'strike': agg.get('strike', 0),
                        'open': agg.get('open', 0),
                        'high': agg.get('high', 0),
                        'low': agg.get('low', 0),
                        'close': agg.get('close', 0),
                        'volume': agg.get('volume', 0),
                        'total_buy_qty': agg.get('total_buy_qty', 0),
                        'total_sell_qty': agg.get('total_sell_qty', 0),
                        'oi': agg.get('oi', 0),
                        'tick_count': agg.get('tick_count', 0),
                        'vwap': agg.get('vwap', 0)  # Volume Weighted Average Price
                    })
            
            if not records:
                print("[SAVE] No valid records to save")
                return
            
            # Create DataFrame
            minute_df = pd.DataFrame(records)
            
            # Set filename (single file for the day)
            if self.csv_filename is None:
                date_str = dt.now().strftime('%Y%m%d')
                self.csv_filename = f'nifty_1min_ohlc_{date_str}.csv'
            
            # Check if file exists to determine header
            file_exists = os.path.isfile(self.csv_filename)
            
            # Append to CSV
            minute_df.to_csv(self.csv_filename, mode='a', 
                           header=not file_exists, index=False)
            
            if not self.csv_header_written:
                self.csv_header_written = True
                
            print(f"[SAVE] {dt.now().strftime('%H:%M:%S')} - Saved {len(records)} aggregated records to {self.csv_filename}")
            
            # Print sample of saved data
            if records:
                sample = records[0]
                print(f"[SAVE] Sample: {sample['symbol']} - O:{sample['open']:.2f} H:{sample['high']:.2f} L:{sample['low']:.2f} C:{sample['close']:.2f} V:{sample['volume']}")
            
        except Exception as e:
            print(f"[SAVE] Error saving aggregated data: {e}")
            import traceback
            traceback.print_exc()

    def _update_minute_aggregate(self, data):
        """Update aggregated data for a symbol"""
        token = data.get('token')
        if not token:
            return
        
        # Initialize aggregate for this token if not exists
        if token not in self.minute_aggregates:
            self.minute_aggregates[token] = {
                'token': token,
                'symbol': data.get('symbol', ''),
                'strike': data.get('strike', 0),
                'open': data.get('ltp', 0),
                'high': data.get('ltp', 0),
                'low': data.get('ltp', 0),
                'close': data.get('ltp', 0),
                'volume': 0,
                'total_buy_qty': 0,
                'total_sell_qty': 0,
                'oi': 0,
                'tick_count': 0,
                'sum_price_volume': 0,  # For VWAP calculation
                'sum_volume': 0
            }
        
        agg = self.minute_aggregates[token]
        
        # Update OHLC
        current_ltp = data.get('ltp', 0)
        agg['close'] = current_ltp
        agg['high'] = max(agg['high'], current_ltp)
        agg['low'] = min(agg['low'], current_ltp) if agg['low'] > 0 else current_ltp
        
        # Update volume and quantities
        agg['volume'] += data.get('volume', 0)
        agg['total_buy_qty'] += data.get('total_buy_qty', 0)
        agg['total_sell_qty'] += data.get('total_sell_qty', 0)
        
        # Update OI (use latest)
        if data.get('oi', 0) > 0:
            agg['oi'] = data.get('oi', 0)
        
        # Update tick count
        agg['tick_count'] += 1
        
        # Update VWAP calculation (sum of price * volume)
        volume = data.get('volume', 0)
        if volume > 0:
            agg['sum_price_volume'] += current_ltp * volume
            agg['sum_volume'] += volume
            agg['vwap'] = agg['sum_price_volume'] / agg['sum_volume'] if agg['sum_volume'] > 0 else current_ltp

    # ------------------------------------------------------------------
    # Heartbeat Monitoring
    # ------------------------------------------------------------------
    
    def _start_heartbeat_monitor(self):
        """Start a thread to monitor heartbeat and connection health"""
        def monitor():
            while not self.should_stop:
                time.sleep(30)
                
                if self.is_connected and _within_market_hours():
                    # Check connection health
                    if hasattr(self.sws, 'last_pong_timestamp') and self.sws.last_pong_timestamp:
                        time_since_last_pong = time.time() - self.sws.last_pong_timestamp
                        if time_since_last_pong > 120:
                            print(f"[WS] No heartbeat for {time_since_last_pong:.0f}s - forcing reconnection")
                            self._force_reconnection()
                    
                    # Refresh tokens every 25 minutes
                    if time.time() - self.last_token_refresh > 1500:
                        print("[WS] Scheduled token refresh")
                        self._refresh_tokens_and_reconnect()
                        self.last_token_refresh = time.time()
        
        self.heartbeat_thread = threading.Thread(target=monitor, daemon=True, name="HeartbeatMonitor")
        self.heartbeat_thread.start()

    def _start_connection_health_check(self):
        """Start a thread to periodically check connection health and reconnect if needed"""
        def health_check_loop():
            last_heartbeat = time.time()
            
            while not self.should_stop:
                time.sleep(30)  # Check every 30 seconds
                
                if not _within_market_hours():
                    continue
                    
                current_time = time.time()
                
                # Check if we have recent data (within last 2 minutes)
                if hasattr(self, 'last_data_time'):
                    time_since_data = current_time - self.last_data_time
                    if time_since_data > 120 and self.is_connected:  # No data for 2 minutes
                        print(f"[HEALTH] No data received for {time_since_data:.0f}s - connection may be dead")
                        print("[HEALTH] Forcing reconnection due to no data")
                        self._force_reconnection()
                
                # Also check if connection state says connected but we suspect it's not
                if self.is_connected and self.reconnect_attempts > 0:
                    # Reset reconnect attempts if we've been stable for a while
                    if current_time - last_heartbeat > 300:  # 5 minutes stable
                        self.reconnect_attempts = 0
                        last_heartbeat = current_time
        
        self.health_thread = threading.Thread(target=health_check_loop, daemon=True, name="HealthCheck")
        self.health_thread.start()
        print("[WS] Connection health check thread started")

    def _force_reconnection(self):
        """Force a complete reconnection"""
        print("[WS] Forcing reconnection...")
        self.is_connected = False
        try:
            if hasattr(self.sws, 'wsapp') and self.sws.wsapp:
                self.sws.close_connection()
        except:
            pass
        
        time.sleep(2)
        self._refresh_tokens_and_reconnect()

    def _refresh_tokens_and_reconnect(self):
        """Refresh authentication tokens and reconnect"""
        print("[WS] Refreshing authentication tokens...")
        try:
            new_session = self.login_manager.login()
            if new_session['status'] == 'success':
                self.auth_token = new_session['data']['jwtToken']
                self.feed_token = new_session['data']['feedToken']
                
                # Stop current connection
                self.is_connected = False
                try:
                    if hasattr(self.sws, 'wsapp') and self.sws.wsapp:
                        self.sws.close_connection()
                except:
                    pass
                
                # Create new connection with fresh tokens
                self.sws = _make_sws(
                    self.auth_token, self.api_key, self.client_id, self.feed_token,
                    self._on_open, self._on_data, self._on_error, self._on_close,
                )
                
                # Restart connection
                self.start_connection()
                self.last_token_refresh = time.time()
                self.reconnect_attempts = 0
                print("[WS] Tokens refreshed and reconnection initiated")
            else:
                print(f"[WS] Token refresh failed: {new_session.get('message')}")
        except Exception as e:
            print(f"[WS] Error refreshing tokens: {e}")

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def start_connection(self):
        """Start the WebSocket connection in a daemon background thread."""
        if self.should_stop:
            print("[WS] Connection stopped - cannot restart")
            return
            
        if self.connection_thread and self.connection_thread.is_alive():
            print("[WS] Connection thread already running – skipping start.")
            return

        self.connection_thread = threading.Thread(
            target=self._connect_with_retry,
            daemon=True,
            name="WebSocketThread",
        )
        self.connection_thread.start()
        print("[WS] Connection thread started.")

    def _connect_with_retry(self):
        """Connect with retry logic and backoff"""
        retry_count = 0
        max_retries = 20  # Increased max retries
        
        while not self.should_stop and _is_market_time():
            try:
                print(f"[WS] Attempting to connect (attempt {retry_count + 1})...")
                self.sws.connect()
                # If connect succeeds without exception, break out of loop
                self.reconnect_attempts = 0  # Reset reconnect attempts on successful connect
                return
            except Exception as e:
                print(f"[WS] Connection failed (attempt {retry_count + 1}): {e}")
                retry_count += 1
                self.reconnect_attempts += 1
                
                # Calculate backoff delay (exponential backoff)
                backoff_delay = min(5 * (2 ** min(retry_count, 4)), 60)  # Max 60 seconds
                print(f"[WS] Waiting {backoff_delay} seconds before retry...")
                
                # Refresh tokens every few retries
                if retry_count % 5 == 0:
                    print("[WS] Multiple connection failures - refreshing tokens")
                    try:
                        new_session = self.login_manager.login()
                        if new_session['status'] == 'success':
                            self.auth_token = new_session['data']['jwtToken']
                            self.feed_token = new_session['data']['feedToken']
                            
                            self.sws = _make_sws(
                                self.auth_token, self.api_key,
                                self.client_id, self.feed_token,
                                self._on_open, self._on_data,
                                self._on_error, self._on_close,
                            )
                            print("[WS] Tokens refreshed")
                    except Exception as refresh_err:
                        print(f"[WS] Token refresh failed: {refresh_err}")
                
                time.sleep(backoff_delay)
        
        if self.should_stop:
            print("[WS] Connection stopped by user")
        elif not _is_market_time():
            print("[WS] Market time ended - stopping connection attempts")

    def stop_connection(self):
        """Cleanly stop the WebSocket connection."""
        self.should_stop = True
        self.is_connected = False
        
        # Save any remaining data before stopping
        if self.save_mode == "TICK_BY_TICK" and self.current_minute_ticks:
            print(f"[WS] Saving final {len(self.current_minute_ticks)} ticks before stopping...")
            self._save_tick_data()
        elif self.save_mode == "AGGREGATED" and self.minute_aggregates:
            print(f"[WS] Saving final aggregated data before stopping...")
            self._save_aggregated_data()
            
        try:
            if hasattr(self.sws, 'wsapp') and self.sws.wsapp:
                self.sws.close_connection()
        except Exception as e:
            print(f"[WS] Warning during stop_connection: {e}")
        print("[WS] Connection stopped.")

    # ------------------------------------------------------------------
    # Subscription helpers
    # ------------------------------------------------------------------

    def subscribe(self, token_list, mode=3, correlation_id="default_corr"):
        """Subscribe to tokens (thread-safe)."""
        self.current_token_list = token_list
        self.current_mode = mode
        self.correlation_id = correlation_id
        
        if self.is_connected:
            try:
                self.sws.subscribe(correlation_id, mode, token_list)
                print(f"[WS] Subscribed to {len(token_list)} exchange group(s).")
                return True
            except Exception as e:
                print(f"[WS] Subscription failed: {e}")
                return False
        else:
            self.subscription_queue.put((correlation_id, mode, token_list))
            print("[WS] Not yet connected – subscription queued.")
            return False

    def _process_queued_subscriptions(self):
        """Drain the subscription queue after the connection is open."""
        while not self.subscription_queue.empty():
            corr_id, mode, tokens = self.subscription_queue.get()
            self.subscribe(tokens, mode, corr_id)

    def resubscribe(self):
        """Resubscribe to previously subscribed tokens"""
        if self.current_token_list and self.is_connected:
            try:
                self.sws.subscribe(self.correlation_id, self.current_mode, self.current_token_list)
                print(f"[WS] Resubscribed to {len(self.current_token_list)} exchange group(s).")
            except Exception as e:
                print(f"[WS] Resubscription failed: {e}")

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def _on_open(self, wsapp):
        """Called when the WebSocket handshake succeeds."""
        self.is_connected = True
        self.reconnect_attempts = 0
        self.last_data_time = time.time()  # Reset data time tracker
        print(f"[WS] Connection opened at {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Start a ping thread to keep connection alive
        def send_ping():
            while self.is_connected and not self.should_stop:
                time.sleep(30)  # Send ping every 30 seconds
                if self.is_connected and hasattr(self.sws, 'wsapp') and self.sws.wsapp:
                    try:
                        # Send a ping frame (if supported) or just a simple message
                        self.sws.wsapp.ping()
                        print("[PING] Sent keep-alive ping")
                    except Exception as e:
                        print(f"[PING] Failed to send ping: {e}")
                        break
        
        self.ping_thread = threading.Thread(target=send_ping, daemon=True, name="PingSender")
        self.ping_thread.start()
        
        self._process_queued_subscriptions()

    def _on_data(self, wsapp, message):
        """Called for every incoming market-data tick."""
        try:
            # Track when we last received data
            self.last_data_time = time.time()
            
            # Parse the message
            field_mapping = {
                'token': 'token',
                'exchange_timestamp': 'exch_ts',
                'last_traded_price': 'ltp',
                'last_traded_quantity': 'ltq',
                'average_traded_price': 'avgtp',
                'volume_trade_for_the_day': 'volume',
                'total_buy_quantity': 'total_buy_qty',
                'total_sell_quantity': 'total_sell_qty',
                'open_price_of_the_day': 'open',
                'high_price_of_the_day': 'high',
                'low_price_of_the_day': 'low',
                'closed_price': 'close',
                'last_traded_timestamp': 'lt_timestamp',
                'open_interest': 'oi',
                'open_interest_change_percentage': 'oi_change_pct',
                'upper_circuit_limit': 'upper_circuit',
                'lower_circuit_limit': 'lower_circuit',
                '52_week_high_price': 'week_52_high',
                '52_week_low_price': 'week_52_low',
            }

            data = {new: message.get(old) for old, new in field_mapping.items()}
            
            # Add symbol and strike
            token_str = str(message.get('token'))
            data['symbol'] = self.token_to_symbol.get(token_str, '')
            data['strike'] = int(self.token_to_strike.get(token_str, 0) / 100)
            
            # Add fetch timestamp (when this data was received)
            data['fetch_timestamp'] = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            data['fetch_minute'] = dt.now().strftime('%Y-%m-%d %H:%M:00')
            
            # Normalize prices (divide by 100 as sent by server)
            price_fields = ['ltp', 'avgtp', 'open', 'high', 'low', 'close',
                           'upper_circuit', 'lower_circuit', 'week_52_high', 'week_52_low']
            for field in price_fields:
                if field in data and data[field] is not None:
                    data[field] = data[field] / 100
            
            # Format exchange timestamp
            if 'exch_ts' in data and data['exch_ts']:
                data['exch_ts'] = dt.fromtimestamp(data['exch_ts'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            # Store data based on save mode
            if self.save_mode == "TICK_BY_TICK":
                # Store all ticks
                self.current_minute_ticks.append(data)
            else:
                # Update aggregated data
                self._update_minute_aggregate(data)
            
            # Update latest values
            self.latest_values[token_str] = data
            self.latest_data = pd.DataFrame([data])
            
            # Print occasional update (every 10 seconds)
            if int(time.time()) % 10 == 0:
                if self.save_mode == "TICK_BY_TICK":
                    print(f"[TICK] {dt.now().strftime('%H:%M:%S')} - {data['symbol']} LTP: {data['ltp']} Vol: {data.get('volume', 0)}")
                else:
                    agg = self.minute_aggregates.get(token_str, {})
                    print(f"[TICK] {dt.now().strftime('%H:%M:%S')} - {data['symbol']} LTP: {data['ltp']} H:{agg.get('high', 0):.2f} L:{agg.get('low', 0):.2f} Vol:{agg.get('volume', 0)}")
            
        except Exception as e:
            print(f"[WS] ERROR in _on_data: {e}")
            import traceback
            traceback.print_exc()

    def _on_error(self, wsapp, error):
        """Handle WebSocket errors with auto-reconnection"""
        self.is_connected = False
        print(f"[WS] Error received: {error}")

        if self.should_stop:
            print("[WS] Stopping - ignoring error")
            return

        if not _within_market_hours():
            print("[WS] Outside market hours – reconnection suppressed.")
            return

        # Increment reconnect attempts
        self.reconnect_attempts += 1
        
        # Calculate backoff delay
        backoff_delay = min(5 * self.reconnect_attempts, 60)
        
        print(f"[WS] Error occurred - will reconnect in {backoff_delay}s (attempt {self.reconnect_attempts})")

        # Refresh tokens if it's been more than 5 minutes or multiple errors
        current_time = time.time()
        if current_time - self.last_error_refresh > 300 or self.reconnect_attempts > 3:
            print("[WS] Refreshing tokens due to connection error …")
            self.last_error_refresh = current_time
            
            def refresh_and_reconnect():
                try:
                    new_session = self.login_manager.login()
                    if new_session['status'] == 'success':
                        self.auth_token = new_session['data']['jwtToken']
                        self.feed_token = new_session['data']['feedToken']
                        
                        # Create new connection with fresh tokens
                        self.sws = _make_sws(
                            self.auth_token, self.api_key,
                            self.client_id, self.feed_token,
                            self._on_open, self._on_data,
                            self._on_error, self._on_close,
                        )
                        self.last_token_refresh = current_time
                        print("[WS] Tokens refreshed successfully")
                        
                        # Start connection
                        if not self.should_stop:
                            self.start_connection()
                    else:
                        print("[WS] Token refresh failed, will retry basic reconnect")
                        if not self.should_stop:
                            self.start_connection()
                except Exception as e:
                    print(f"[WS] Error during token refresh: {e}")
                    if not self.should_stop:
                        self.start_connection()
            
            threading.Timer(backoff_delay, refresh_and_reconnect).start()
        else:
            # Just reconnect without token refresh
            if not self.should_stop:
                threading.Timer(backoff_delay, self.start_connection).start()

    def _on_close(self, wsapp):
        """Called when the server closes the connection."""
        self.is_connected = False
        close_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[WS] Connection closed at {close_time}")

        # Don't reconnect if we're stopping or market is closed
        if self.should_stop:
            print("[WS] Intentional stop - no reconnect")
            return
            
        if not _within_market_hours():
            print("[WS] Outside market hours – reconnection suppressed.")
            return

        # Always try to reconnect during market hours
        print("[WS] Connection closed unexpectedly - attempting to reconnect...")
        
        # Clear connection state
        self.is_connected = False
        
        # Schedule reconnection with exponential backoff
        reconnect_delay = min(5 * (self.reconnect_attempts + 1), 30)  # Max 30 seconds
        
        print(f"[WS] Scheduling reconnect in {reconnect_delay} seconds (attempt {self.reconnect_attempts + 1})")
        
        def reconnect():
            if not self.should_stop and _within_market_hours():
                # Refresh tokens before reconnecting
                self._refresh_tokens_and_reconnect()
        
        threading.Timer(reconnect_delay, reconnect).start()