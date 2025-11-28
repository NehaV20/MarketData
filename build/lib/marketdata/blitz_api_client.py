import json
import redis
import requests
import logging
import time
from marketdata.Authentication import AuthClient
from marketdata.config import API_BASE_URL, REDIS_URL
from marketdata.websocket_stream_handler import MarketDataWebSocketClient

logging.basicConfig(level=logging.DEBUG)

class BlitzAPIClient:
    def __init__(self, app_key: str, user_id: str):
        self.app_key = app_key
        self.user_id = user_id
        self.api_base_url = API_BASE_URL
        self.api_hist_base_url = API_BASE_URL
        self.token = None 
        self.auth_client = AuthClient(app_key, user_id)
        self.access_token = None

          # Ensure logged in
        self._ensure_logged_in()

        # Initialize WebSocket
        self.ws_client = MarketDataWebSocketClient(self.access_token)
        self.ws_client.set_on_connect(self.on_connect)
        self.ws_client.set_on_close(self.on_close)

        # Initialize Redis
        self.redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


    # ---------------------------------------------------------------------
    # Authentication and Connection Management
    # ---------------------------------------------------------------------
    def _ensure_logged_in(self):
        """Ensure the user is logged in and has a valid access token."""
        self.access_token = self.auth_client.get_access_token()

    def _is_connected(self):
        """Check if the WebSocket connection is active."""
        return self.ws_client.ws and self.ws_client.ws.sock and self.ws_client.ws.sock.connected

    def on_connect(self):
        """Callback when WebSocket connects."""
        self.ws_client.start()
        logging.info("WebSocket connected successfully.")

    def on_close(self, close_status_code, close_msg):
        """Callback when WebSocket closes."""
        logging.warning(f"WebSocket closed: {close_status_code}, {close_msg}")

    @property
    def on_message(self):
        return self._on_tick

    @on_message.setter
    def on_message(self, callback):
        self._on_tick = callback
        self.ws_client.set_on_message(callback)


    # ---------------------------------------------------------------------
    # Common HTTP Helper
    # ---------------------------------------------------------------------

    def _send_request(self, endpoint, payload=None, method="POST", params=None, retries=0):
        MAX_RETRIES = 2
        url = f"{self.api_base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "*/*"
        }

        try:
            if method == "POST":
                response = requests.post(url, json=payload, headers=headers)
            elif method == "PUT":
                response = requests.put(url, json=payload, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, params=params, headers=headers)
            elif method == "GET":
                response = requests.get(url, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if response.status_code == 401 and retries < MAX_RETRIES:
                logging.warning("Access token expired. Refreshing token and retrying...")
                self._ensure_logged_in()
                return self._send_request(endpoint, payload, method, params, retries + 1)
            try:
                response_json = response.json() if response.text else None
            except ValueError:
                response_json = None
            return {
                "status_code": response.status_code,
                "response_text": response.text,
                "response_json": response_json,
                "headers": dict(response.headers)
            }

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return {
                "status_code": None,
                "response_text": str(e),
                "response_json": None,
                "headers": {}
            }


    # ---------------------------------------------------------------------
    # ORDER MANAGEMENT
    # ---------------------------------------------------------------------

    def get_orders(self):
        """Fetch all orders."""
        logging.info("Fetching all orders")
        endpoint = "orders"
        return self._send_request(endpoint, method="GET")

    def get_order_by_blitz_id(self, blitz_order_id: int):
        """Fetch a single order by BlitzOrderId."""
        logging.info(f"Fetching order for BlitzOrderId={blitz_order_id}")
        endpoint = f"orders/{blitz_order_id}"
        return self._send_request(endpoint, method="GET")

    def place_order(self, order_data: dict):
        """Place a new order."""
        logging.info(f"Placing order: {order_data}")
        endpoint = "orders/placeOrder"
        return self._send_request(endpoint, payload=order_data, method="POST")

    def modify_order(self, order_data: dict):
        """Modify an existing order."""
        logging.info(f"Modifying order: {order_data}")
        endpoint = "orders/modifyOrder"
        return self._send_request(endpoint, payload=order_data, method="PUT")

    def cancel_order(self, instrument_id: str, exchange_order_id: int):
        """Cancel an order by instrumentId and exchangeOrderId."""
        logging.info(f"Cancelling order InstrumentId={instrument_id}, ExchangeOrderId={exchange_order_id}")
        endpoint = "orders/cancelOrder"
        params = {
            "instrumentId": instrument_id,
            "exchangeOrderId": exchange_order_id
        }
        return self._send_request(endpoint, method="DELETE", params=params)


    # ---------------------------------------------------------------------
    # POSITION MANAGEMENT
    # ---------------------------------------------------------------------

    def get_positions(self):
        """Fetch all positions."""
        logging.info("Fetching positions")
        endpoint = "positions"
        return self._send_request(endpoint, method="GET")
    
    # ---------------------------------------------------------------------
    # STATICSTICS MANAGEMENT
    # ---------------------------------------------------------------------

    
    def get_statistics(self):
        """Fetch all statistcs"""
        logging.info("Fetching Stategy Statistics")
        endpoint = "strategy/statistics"
        return self._send_request(endpoint, method="GET")


    # ---------------------------------------------------------------------
    # TRADE MANAGEMENT
    # ---------------------------------------------------------------------

    def get_trades(self):
        """Fetch all trades."""
        logging.info("Fetching trades")
        endpoint = "trades"
        return self._send_request(endpoint, method="GET")
    
    def _publish_to_redis(self, channel, data):
        """Publish JSON data to a Redis channel."""
        try:
            self.redis_client.publish(channel, json.dumps(data))
            logging.debug(f"Published to Redis channel {channel}: {data}")
        except Exception as e:
            logging.error(f"Failed to publish to Redis: {e}")

    def send_signal(self, signal_request: dict):
        """Send a signal to Blitz-API and publish to Redis."""
        logging.info(f"Sending signal: {signal_request}")
        endpoint = "signals"
        result = self._send_request(endpoint, payload=signal_request, method="POST")

            # Publish to Redis if successful
        if result["status_code"] == 200:
            try:
                self.redis_client.publish("SignalChannel", json.dumps(signal_request))
                logging.info("Signal published to Redis channel: SignalChannel")
            except Exception as e:
                logging.error(f"Failed to publish signal to Redis: {e}")
        return result


