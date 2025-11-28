# =============================== Initialize Client ===============================
from market_data import MarketDataClient

client = MarketDataClient(
    app_key="YOUR_APP_KEY",
    user_id="YOUR_USER_ID"
)


Instruments = ["NSECM|ADANIENT|abs", 1010010002000001, 1010010002000002, 1010010000000025]
# Instruments = [1010010002000001]
# # =============================== Ltp ===============================
#instrument_ids = [1010010002000001, 1010010002000002, 1010010000000025]
ltp = client.get_ltp(Instruments)
print("LTP Response:", ltp)

# =============================== Option Chain ===============================
symbol= "NIFTY"
expriry_date= "2025-05-29"
optionCahin = client.get_option_chain(symbol,expriry_date)
print("Option Chain Response:", optionCahin)

# # ====l=========================== Quote ===============================
# instrument_ids = [1010010002000001, 1010010002000002, 1010010000000025]
# quote_data = client.get_quote(instrument_ids)

quote_data = client.get_quote(Instruments)
print("Quote Response:", quote_data)

 # =============== Historical Data Example =================
hist_data = client.get_historical_data("IRFC", "2024-01-11", "2025-11-11")
print("Historical Data Response:", hist_data)


# =============================== Websocket Data ===============================
#instrument_ids = [1010010002000001,1010010002000001]

def my_callback_function(data):
    print("New tick data received:", data)

client.on_message = my_callback_function

client.connect_ws()

print("WebSocket connection established.")

client.subscribe_market_data(Instruments)

#client.unsubscribe_market_data(instrument_ids)
