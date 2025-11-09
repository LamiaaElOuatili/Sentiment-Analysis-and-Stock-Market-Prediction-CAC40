import requests
import json

BASE_URL = "http://127.0.0.1:8000"

# Exemple 1 : Tesla
response = requests.get(f"{BASE_URL}/stocks", params={"ticker": "TSLA", "period": "7d"})
data = response.json()
print(json.dumps(data, indent=2))

# Exemple 2 : Apple sur 1 jour
response = requests.get(f"{BASE_URL}/stocks", params={"ticker": "AAPL", "period": "1d", "interval": "15m"})
print("\n--- Apple (1d, 15m) ---")
print(json.dumps(response.json(), indent=2))
