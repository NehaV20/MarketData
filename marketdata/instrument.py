import requests
import gzip
import json

def fetch_and_load_instruments(url):
    print("Downloading instruments...")
    response = requests.get(url, timeout=15)
    decompressed = gzip.decompress(response.content)
    data = json.loads(decompressed)
    
    global INSTRUMENTS_BY_NAME, INSTRUMENTS_BY_ID

    INSTRUMENTS_BY_NAME = {
            f'{item["exchangeSegment"]}|{item["instrumentName"]}': item["instrumentId"]
            for item in data
        }
    INSTRUMENTS_BY_ID = {iid: name for name, iid in INSTRUMENTS_BY_NAME.items()}

    print(f"Loaded {len(INSTRUMENTS_BY_NAME)} instruments.")
    return INSTRUMENTS_BY_NAME

def verify_instrument_id(symbol=None, instrument_id=None):
    global INSTRUMENTS_BY_NAME, INSTRUMENTS_BY_ID

    if symbol and not instrument_id:
        result = INSTRUMENTS_BY_NAME.get(symbol)
        if not result:
            raise ValueError(f"Instrument not found for symbol: {symbol}")
        print(f"Matched → Symbol: {symbol} | Instrument ID: {result}")
        return result

    if instrument_id and not symbol:
        symbol_from_id = INSTRUMENTS_BY_ID.get(instrument_id)
        if not symbol_from_id:
            raise ValueError(f"Instrument ID {instrument_id} not found in cache.")
        print(f"Matched → Symbol: {symbol_from_id} | Instrument ID: {instrument_id}")
        return instrument_id

    if symbol and instrument_id:
        expected_id = INSTRUMENTS_BY_NAME.get(symbol)
        if not expected_id:
            raise ValueError(f"Instrument not found for symbol: {symbol}")

        if expected_id != instrument_id:
            raise ValueError(
                f"Instrument ID mismatch: provided={instrument_id}, expected={expected_id} for symbol {symbol}"
            )

        print(f"Verified → Symbol: {symbol} | Instrument ID: {instrument_id}")
        return instrument_id

    raise ValueError("Either symbol or instrument_id must be provided.")




# symbol = "011NSETEST"
# instrument_id = 2063320000036687

# print("Checking instrument...")
# verified_id = verify_instrument_id(symbol=symbol, instrument_id=instrument_id)
# print("Verified Instrument ID:", verified_id)
