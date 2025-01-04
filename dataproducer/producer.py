import __root__

import json
import os
import sys

import websocket
from confluent_kafka import Producer
from finnhub import Client

# Add the parent directory to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from utils import configutils

BOOTSTRAP_SERVERS = configutils.read_application_config("KAFKA_BROKER_URL")
API_TOKEN = configutils.read_application_config("STOCK_API_KEY")
KAFKA_TOPIC = configutils.read_application_config("KAFKA_TOPIC")

finnhub_client = Client(api_key=API_TOKEN)


def get_kafka_conf():
    return {
        # User-specific properties that you must set
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "websocket-producer",
    }


kafka_producer = Producer(get_kafka_conf())


def fetch_symbols(exchange: str = "US"):
    return finnhub_client.stock_symbols(exchange)


def fetch_company_profile(symbol: str):
    return finnhub_client.company_profile2(symbol=symbol)


def generate_mock_data():  #### For testing purpose
    return {
        "companyProfile": {
            "country": "US",
            "currency": "USD",
            "exchange": "NASDAQ/NMS (GLOBAL MARKET)",
            "ipo": "1980-12-12",
            "marketCapitalization": 1415993,
            "name": "Apple Inc",
            "phone": "14089961010",
            "shareOutstanding": 4375.47998046875,
            "ticker": "AAPL",
            "weburl": "https://www.apple.com/",
            "logo": "https://static.finnhub.io/logo/87cb30d8-80df-11ea-8951-00000000092a.png",
            "finnhubIndustry": "Technology",
        },
        "data": [
            {
                "p": 255.65,
                "s": "AAPL",
                "t": 1735451912469,
                "v": 1300,
            }
        ],
        "type": "trade",
    }


def on_message(ws, message):
    try:
        data = json.loads(message)
        if data["type"] != "ping":
            company_profile = fetch_company_profile(symbol)
            data["companyProfile"] = company_profile
        else:
            data = generate_mock_data()
        symbol = data["data"][0]["s"]
        kafka_producer.produce(KAFKA_TOPIC, key=symbol, value=json.dumps(data).encode())
        kafka_producer.flush()
        print(f"Produced message to topic: {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Error in processing message: {e}")


def on_error(ws, error):
    print(error)


def on_close(ws, status_code, msg):
    print(f"Connection closed with status code: {status_code} and message: {msg}")


def on_open(ws: websocket.WebSocketApp):
    symbols_data = fetch_symbols()
    for i in range(10):
        data = symbols_data[i]
        symbol = data["displaySymbol"]
        print(f"Sending symbol: {symbol}")
        json_dict = {"type": "subscribe", "symbol": symbol}
        ws.send(json.dumps(json_dict).encode())


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_TOKEN}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = on_open
    ws.run_forever()
