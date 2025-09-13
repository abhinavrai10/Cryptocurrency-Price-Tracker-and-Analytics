import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os

# Config via environment variables
KAFKA_BOOTSTRAP_SERVERS = [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')]
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-prices')
COINGECKO_API_BASE = os.getenv('COINGECKO_API', 'https://api.coingecko.com/api/v3')
COINS = os.getenv('COINS', 'bitcoin,ethereum,binancecoin,tether,solana').split(',')

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_prices(coins):
    params = {
        'vs_currency': 'usd',
        'ids': ','.join(coins),
        'order': 'market_cap_desc',
        'per_page': 25,
        'page': 1,
        'sparkline': False
    }
    url = f"{COINGECKO_API_BASE}/coins/markets"
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.utcnow().isoformat()
        for item in data:
            message = {
                'coin': item['id'],
                'price_usd': item['current_price'],
                'market_cap': item['market_cap'],
                'volume_24h': item['total_volume'],
                'high_24h': item['high_24h'],
                'low_24h': item['low_24h'],
                'last_updated': item['last_updated'],
                'timestamp': timestamp
            }
            producer.send(KAFKA_TOPIC, message)
            print(f"Sent: {message}")
    else:
        print(f"API error: {response.status_code}")

while True:
    fetch_prices(COINS)
    time.sleep(30)  # Poll every 30 seconds