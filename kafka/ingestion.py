import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Config
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Update if external
KAFKA_TOPIC = 'crypto-prices'
COINGECKO_API = 'https://api.coingecko.com/api/v3/simple/price'
COINS = ['bitcoin', 'ethereum']  # Add up to 50, comma-separated IDs

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_prices():
    params = {'ids': ','.join(COINS), 'vs_currencies': 'usd'}
    response = requests.get(COINGECKO_API, params=params)
    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.utcnow().isoformat()
        for coin, prices in data.items():
            message = {'coin': coin, 'price_usd': prices['usd'], 'timestamp': timestamp}
            producer.send(KAFKA_TOPIC, message)
            print(f"Sent: {message}")
    else:
        print(f"API error: {response.status_code}")

while True:
    # fetch_prices()
    print("fetch_prices")
    time.sleep(30)  # Poll every 30 seconds