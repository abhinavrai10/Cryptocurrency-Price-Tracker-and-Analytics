import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os

# Config via environment variables
KAFKA_BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-prices')
COINGECKO_API = os.getenv('COINGECKO_API', 'https://api.coingecko.com/api/v3/simple/price')
COINS = os.getenv('COINS', 'bitcoin,ethereum,binancecoin,tether,solana').split(',')

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
    fetch_prices()
    time.sleep(30)  # Poll every 30 seconds