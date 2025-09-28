#import requirements
import time
import requests
import json
from kafka import KafkaProducer

#define variables for API
API_KEY="d3ao1ohr01qrtc0d24fgd3ao1ohr01qrtc0d24g0"
BASE_URL= "https://finnhub.io/api/v1/quote"
SYMBOLS=["AAPL","MSFT","TSLA","GOOGL","AMZN"]

#initialise produder
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#retreive data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None 

#looping and pushing the stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            # Corrected: We're now using the 'key' argument for partitioning.
            # This ensures all messages for 'AAPL' go to the same partition.
            producer.send("stock-quotes", key=symbol.encode("utf-8"), value=quote)
    
    # Corrected: We're moving the sleep outside the for loop
    # to avoid a long delay between each symbol's API call.
    time.sleep(6)