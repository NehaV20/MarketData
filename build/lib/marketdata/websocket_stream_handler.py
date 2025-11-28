import websocket
import threading
import base64
import time
import json
import logging
from .config import Web_Base_URL
from .proto import marketdata_pb2

logging.basicConfig(level=logging.INFO)

class MarketDataWebSocketClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.web_base_url = f"{Web_Base_URL}{self.access_token}"

        self.ws = None
        self.thread = None
        self._heartbeat_thread = None

        self.reconnect = True
        self.ping_interval = 30

        self.connected = False   #  NEW — reliable connection flag

        self.on_message_callback = None
        self.on_connect_callback = None
        self.on_close_callback = None

    def set_on_message(self, callback):
        self.on_message_callback = callback

    def set_on_connect(self, callback):
        self.on_connect_callback = callback

    def set_on_close(self, callback):
        self.on_close_callback = callback

    def on_open(self, ws):
        self.connected = True   #  mark connected
        if self.on_connect_callback:
            self.on_connect_callback()
        logging.info("WebSocket connection established.")

    def on_message(self, ws, message):
        try:
            decoded = base64.b64decode(message)
            md_message = marketdata_pb2.MarketDataMessageBase()
            md_message.ParseFromString(decoded)

            if self.on_message_callback:
                self.on_message_callback(md_message)

        except Exception as e:
            logging.error(f"Failed to parse WebSocket message: {e}")

    def on_error(self, ws, error):
        self.connected = False   #  mark disconnected
        logging.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False   #  mark disconnected
        logging.warning(f"WebSocket closed: {close_status_code}, {close_msg}")

        if self.on_close_callback:
            self.on_close_callback(close_status_code, close_msg)

        if self.reconnect:
            logging.info("Reconnecting in 5 seconds...")
            time.sleep(5)
            self.start()

    def _is_connected(self):
        
        try:
            return self.connected and self.ws and self.ws.sock and self.ws.sock.connected
        except:
            return False

    def _send_heartbeat(self):
        while True:
            if self._is_connected():
                try:
                    self.ws.send("ping")
                    logging.debug("Sent ping.")
                except Exception as e:
                    logging.warning(f"Ping failed: {e}")
            time.sleep(self.ping_interval)

    def _send_subscription_message(self, action, instrument_ids):
        msg = {"action": action, "instrumentIds": instrument_ids}

        if self._is_connected():
            self.ws.send(json.dumps(msg))
            logging.info(f"{action.capitalize()} message sent: {msg}")
        else:
            logging.error("WebSocket is not connected. Cannot send subscription.")

    def subscribe(self, instrument_ids):
        self._send_subscription_message("subscribe", instrument_ids)

    def unsubscribe(self, instrument_ids):
        self._send_subscription_message("unsubscribe", instrument_ids)

    def start(self):
        if self._is_connected():
            logging.info("WebSocket already running.")
            return

        self.ws = websocket.WebSocketApp(
            self.web_base_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        #  Ensure only one thread
        if not self.thread or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.ws.run_forever)
            self.thread.daemon = False
            self.thread.start()

        #  Heartbeat starts once only
        if not self._heartbeat_thread or not self._heartbeat_thread.is_alive():
            self._heartbeat_thread = threading.Thread(target=self._send_heartbeat)
            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()

    def stop(self):
        self.reconnect = False
        self.connected = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        logging.info("WebSocket client stopped.")
