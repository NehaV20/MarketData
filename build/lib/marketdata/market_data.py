import requests
import logging
import time

from .Authentication import AuthClient
from .config import API_BASE_URL, INSTRUMENT_URL
from .instrument import fetch_and_load_instruments, verify_instrument_id
from .websocket_stream_handler import MarketDataWebSocketClient


logging.basicConfig(level=logging.DEBUG)

class MarketDataClient:
    def __init__(self, app_key: str, user_id: str):
        self.app_key = app_key
        self.user_id = user_id
        self.api_base_url = API_BASE_URL
        self.api_hist_base_url = API_BASE_URL 
        self.auth_client = AuthClient(app_key, user_id)

        self._ensure_logged_in()
        self.ws_client = MarketDataWebSocketClient(self.access_token)
        self.ws_client.set_on_connect(self.on_connect)
        self.ws_client.set_on_close(self.on_close)
        self._on_tick = None

    INSTRUMENTS_CACHE = fetch_and_load_instruments(INSTRUMENT_URL)

    def _is_connected(self):
        return self.ws_client._is_connected()

    def on_connect(self):
        logging.info("WebSocket connected.")

    def on_close(self, close_status_code, close_msg):
        logging.warning(f"WebSocket closed: {close_status_code}, {close_msg}")

    def _resolve_ids(self, items):
        if isinstance(items, (str, int)):
            items = [items]

        resolved = []
        for x in items:

            if isinstance(x, int):
                resolved.append(verify_instrument_id(instrument_id=x))

            elif isinstance(x, str) and x.isdigit():
                resolved.append(verify_instrument_id(instrument_id=int(x)))
            else:
                if isinstance(x, str):
                    parts = x.split("|")
                    if len(parts) >= 1:
                        x = f"{parts[0]}|{parts[1]}"
                    else: 
                        raise ValueError(
                            f"Exchange segment missing. Use format EXCHANGE|NAME, e.g. NSECM|RELIANCE. Got: {x}"
                        )
                resolved.append(verify_instrument_id(symbol=x))
        return resolved

    @property
    def on_message(self):
        return self._on_tick

    @on_message.setter
    def on_message(self, callback):
        self._on_tick = callback
        self.ws_client.set_on_message(callback)

    def _ensure_logged_in(self):
        self.access_token = self.auth_client.get_access_token()

    def _send_request(self, endpoint, payload):
        url = f"{self.api_base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "*/*"
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Request failed: {response.status_code} - {response.text}")

    # LTP
    def get_ltp(self, instrument):
        instrument_ids = self._resolve_ids(instrument)
        payload = {"InstrumentIds": instrument_ids}
        return self._send_request("/marketfeed/ltp", payload)

    # Option Chain (symbol only, no ID conversion needed)
    def get_option_chain(self, symbol, expiry_date):
        payload = {"symbol": symbol, "expiryDate": expiry_date}
        return self._send_request("/marketfeed/optionChain", payload)

    # Quote
    def get_quote(self, instrument):
        instrument_ids = self._resolve_ids(instrument)
        payload = {"InstrumentIds": instrument_ids}
        return self._send_request("/marketfeed/quote", payload)

    # Historical Data
    def get_historical_data(self, instrument, from_date, to_date):
        payload = {
            "Instrument": instrument, 
            "from": from_date,
            "to": to_date
        }
        return self._send_request("/marketfeed/historicalData", payload)

    def connect_ws(self):
        if not self._is_connected():
            self.ws_client.start()
            while not self._is_connected():
                time.sleep(0.1)

    def stop_websocket(self):
        if self.ws_client:
            self.ws_client.stop()
            logging.info("WebSocket stopped.")

    def subscribe_market_data(self, instrument):
        instrument_ids = self._resolve_ids(instrument)
        if self._is_connected():
            self.ws_client.subscribe(instrument_ids)
            logging.info(f"Subscribed to: {instrument_ids}")
        else:
            logging.error("WebSocket is not connected. Cannot subscribe.")

    def unsubscribe_market_data(self, instrument):
        instrument_ids = self._resolve_ids(instrument)
        if self._is_connected():
            self.ws_client.unsubscribe(instrument_ids)
            logging.info(f"Unsubscribed from: {instrument_ids}")
        else:
            logging.error("WebSocket is not connected. Cannot unsubscribe.")
