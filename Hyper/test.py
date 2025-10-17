import json
with open ("tradePairs.json", "r", encoding="utf-8") as f:
    symbols = json.load(f)
print(symbols[:5])