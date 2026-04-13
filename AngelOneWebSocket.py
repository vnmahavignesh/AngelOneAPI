from datetime import datetime as dt
import os
import queue
import threading
from venv import logger

from smartWebSocketV2 import SmartWebSocketV2
import pandas as pd


class AngelOneWebSocket:
    """Handles WebSocket connections and data streaming"""

    def __init__(self, auth_token: str, feed_token: str, token_to_symbol: dict):
        """
        Initialize WebSocket manager

        Args:
            smart_connect: Authenticated SmartConnect instance
            auth_token: JWT token from authentication
            feed_token: Feed token from authentication
            token_to_symbol: Dictionary mapping tokens to symbols
        """

        self.api_key = os.getenv("API_KEY")
        self.client_id = os.getenv("CLIENT_ID")
        self.auth_token = auth_token
        self.feed_token = feed_token
        self.token_to_symbol = token_to_symbol
        self.excel_updater = ExcelDataUpdater()  # Add this line
        self.current_token_list = None  # To store subscribed tokens

        # # Print the authentication token to verify that it has been retrieved successfully
        # print("\n Authentication Token--------:\n", self.auth_token)
        # # Print the feed token to verify that it has been retrieved successfully
        # print("\n Feed Token------------:\n", self.feed_token)

        # WebSocket setup
        self.sws = SmartWebSocketV2(
            self.auth_token, self.api_key, self.client_id, self.feed_token)
        self.connection_thread = None
        self.subscription_queue = queue.Queue()
        self.is_connected = False
        self.latest_data = None

        # Setup callbacks
        self.sws.on_open = self._on_open
        self.sws.on_data = self._on_data
        self.sws.on_error = self._on_error
        self.sws.on_close = self._on_close

    def start_connection(self):
        """Start the WebSocket connection in a background thread"""
        if self.connection_thread and self.connection_thread.is_alive():
            logger.warning("Connection already running")
            return

        self.connection_thread = threading.Thread(
            target=self.sws.connect,
            daemon=True,
            name="WebSocketThread"
        )
        self.connection_thread.start()
        logger.info("WebSocket connection started")

    def stop_connection(self):
        """Stop the WebSocket connection"""
        if hasattr(self.sws, 'wsapp') and self.sws.wsapp.sock:
            self.sws.close_connection()
        self.is_connected = False
        logger.info("WebSocket connection stopped")

    def subscribe(self, token_list, mode=3, correlation_id="default_corr"):
        """Subscribe to tokens (thread-safe)"""
        self.current_token_list = token_list  # Store the token list
        if self.is_connected:
            try:
                self.sws.subscribe(correlation_id, mode, token_list)
                logger.info(f"Subscribed to tokens: {token_list}")
            except Exception as e:
                logger.error(f"Subscription failed: {e}")
        else:
            # Queue the subscription for when connection is ready
            self.subscription_queue.put((correlation_id, mode, token_list))
            logger.info(f"Queued subscription for tokens: {token_list}")

    def _process_queued_subscriptions(self):
        """Process any subscriptions that were queued before connection"""
        while not self.subscription_queue.empty():
            corr_id, mode, tokens = self.subscription_queue.get()
            self.subscribe(tokens, mode, corr_id)

    def _on_open(self, wsapp):
        """Callback when WebSocket connection is established"""
        self.is_connected = True
        logger.info("WebSocket connection opened")
        self._process_queued_subscriptions()

    def _on_data(self, wsapp, message):
        """Callback when data is received"""
        try:
            #  # Clear the console
            # os.system('cls')  # Use 'cls' for Windows systems

            # Field mapping and data processing
            field_mapping = {
                'token': 'token',
                'sequence_number': 'seq_num',
                'exchange_timestamp': 'exch_ts',
                'last_traded_price': 'ltp',
                'subscription_mode_val': 'mode',
                'last_traded_quantity': 'ltq',
                'average_traded_price': 'avgtp',
                'volume_trade_for_the_day': 'volume',
                'total_buy_quantity': 'total_buy_qty',
                'total_sell_quantity': 'total_sell_qty',
                'open_price_of_the_day': 'open',
                'high_price_of_the_day': 'high',
                'low_price_of_the_day': 'low',
                'closed_price': 'PDclose',
                'last_traded_timestamp': 'LTTS',
                'open_interest': 'oi',
                'open_interest_change_percentage': 'oipct',
                'upper_circuit_limit': 'upper_circuit',
                'lower_circuit_limit': 'lower_circuit',
                '52_week_high_price': '52w_high',
                '52_week_low_price': '52w_low'
            }

            data = {new_name: message.get(
                old_name) for old_name, new_name in field_mapping.items()}
            df = pd.DataFrame([data])

            # Add symbol from token mapping
            df['symbol'] = self.token_to_symbol.get(
                str(message.get('token')), '')

            # Price normalization
            price_fields = ['ltp', 'avgtp', 'open', 'high', 'low', 'PDclose',
                            'upper_circuit', 'lower_circuit', '52w_high', '52w_low']
            for field in price_fields:
                if field in df.columns:
                    df[field] = df[field] / 100

            # Timestamp conversion
            if 'LTTS' in df.columns:
                df['LTTS'] = pd.to_datetime(
                    df['LTTS'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
            if 'exch_ts' in df.columns:
                df['exch_ts'] = dt.fromtimestamp(
                    df['exch_ts'].iloc[0] / 1000).isoformat()

            # Store latest data
            self.latest_data = df

            # Define columns to display
            display_columns = ['symbol','ltp', 'open', 'high', 'low', 'PDclose',
                               'oi', 'oipct', 'avgtp', 'volume','token', 'exch_ts', 'total_buy_qty', 'total_sell_qty', '52w_high', '52w_low']

            # Get current token
            current_token = str(message.get('token'))

            # Prepare data for display
            display_data = df[display_columns].copy()

            # Print to terminal
            print("\n" + "="*50)
            print(
                f"Live Data Update - Token: {current_token}, Symbol: {df['symbol'].iloc[0]}")
            print("="*50)
            print(display_data.to_string(index=False))
            print("-"*50 + "\n")

            # Update Excel
            self.excel_updater.update_excel(df, self.current_token_list)

        except Exception as e:
            logger.error(f"Error in _on_data: {str(e)}")
            print(f"ERROR: {str(e)}")

    def _on_error(self, wsapp, error):
        """Callback when error occurs"""
        self.is_connected = False
        logger.error(f"WebSocket error: {error}")
        threading.Timer(5.0, self.start_connection).start()

    def _on_close(self, wsapp):
        """Callback when connection is closed"""
        self.is_connected = False
        logger.info("WebSocket connection closed")
        if not self.sws.CLOSE_CONNECTION:
            threading.Timer(5.0, self.start_connection).start()

    def unsubscribe(self, token_list, mode=3, correlation_id="default_corr"):
        """Unsubscribe from tokens (thread-safe)"""
        if self.is_connected:
            try:
                self.sws.unsubscribe(correlation_id, mode, token_list)
                logger.info(f"Unsubscribed from tokens: {token_list}")
            except Exception as e:
                logger.error(f"Unsubscription failed: {e}")
        else:
            logger.warning("Cannot unsubscribe - not connected")


class ExcelDataUpdater:
    """Handles updating Excel with live market data"""

    def __init__(self, file_name="live_market_data.xlsx"):
        self.file_name = file_name
        self.all_data = pd.DataFrame()  # Store all market data
        self.display_columns = ['symbol','ltp', 'open', 'high', 'low', 'PDclose',
                                'oi', 'oipct', 'avgtp', 'volume','token', 'exch_ts', 'total_buy_qty', 'total_sell_qty', '52w_high', '52w_low']

    def update_excel(self, data: pd.DataFrame, token_list: list = None):
        """
        Update Excel with live market data, sorted by volume descending

        Args:
            data: DataFrame containing market data for a single token
            token_list: List of tokens being watched (for initialization)
        """
        try:
            import xlwings as xw

            # Get current token
            current_token = str(data['token'].iloc[0])

            # Update or append to all_data
            if self.all_data.empty:
                self.all_data = data.copy()
            else:
                # Check if token already exists
                if current_token in self.all_data['token'].astype(str).values:
                    # Update existing row
                    mask = self.all_data['token'].astype(str) == current_token
                    for col in data.columns:
                        if col in self.all_data.columns:
                            self.all_data.loc[mask, col] = data[col].values[0]
                else:
                    # Append new row
                    self.all_data = pd.concat(
                        [self.all_data, data], ignore_index=True)

            # Sort by volume in descending order
            if 'volume' in self.all_data.columns:
                self.all_data = self.all_data.sort_values(
                    by='volume', ascending=False).reset_index(drop=True)

            # Try to open existing workbook or create new
            try:
                wb = xw.Book(self.file_name)
            except FileNotFoundError:
                wb = xw.Book()
                wb.save(self.file_name)

            # Get or create sheet
            if "LiveData" not in [s.name for s in wb.sheets]:
                sheet = wb.sheets.add("LiveData")
            else:
                sheet = wb.sheets["LiveData"]
                sheet.clear()  # Clear existing data

            # Write headers
            sheet.range("A1").value = self.display_columns

            # Write sorted data
            if not self.all_data.empty:
                sheet.range(
                    "A2").value = self.all_data[self.display_columns].values.tolist()

            sheet.autofit()
            wb.save()

            print(
                f"Updated Excel - Sorted by volume descending. Current tokens: {len(self.all_data)}")

        except Exception as excel_error:
            print(f"Excel update failed: {str(excel_error)}")

    def update_option_greeks(self, option_greeks_data: pd.DataFrame):
        """
        Update Excel with option Greeks data under the 'optiongreek' sheet.

        Args:
            option_greeks_data: DataFrame containing option Greeks data
        """
        try:
            import xlwings as xw

            # Try to open existing workbook or create new
            try:
                wb = xw.Book(self.file_name)
            except FileNotFoundError:
                wb = xw.Book()
                wb.save(self.file_name)

            # Get or create sheet
            if "optiongreek" not in [s.name for s in wb.sheets]:
                sheet = wb.sheets.add("optiongreek")
            else:
                sheet = wb.sheets["optiongreek"]
                sheet.clear()  # Clear existing data to avoid duplication

            # Write headers in the first row
            headers = [
                "name", "expiry", "strikePrice", "optionType", "delta",
                "gamma", "theta", "vega", "impliedVolatility", "tradeVolume"
            ]
            sheet.range("A1").value = headers

            # Write data starting from row 2
            sheet.range("A2").value = option_greeks_data[headers].values
            sheet.autofit()
            wb.save()

            print("Updated Excel with option Greeks data in 'optiongreek' sheet")

        except Exception as excel_error:
            print(f"Excel update failed: {str(excel_error)}")
