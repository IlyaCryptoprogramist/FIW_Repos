from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from pathlib import Path
from datetime import datetime
import subprocess
import sys
import threading
import time
import ccxt
import os
from dotenv import load_dotenv
load_dotenv()   # загружает переменные из .env

# Импортируем наши модули
from coinmarketcap_integration import enrich_with_cmc
from blockchain_integration import get_token_holders

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent
LAST_UPDATE_FILE = BASE_DIR / "last_update.txt"

EXCHANGES = ['Aster', 'BingX', 'Bybite', 'Hyper', 'KuCoin', 'MexC']
EXCHANGE_NAMES = {
    'Aster': 'Aster',
    'BingX': 'BingX',
    'Bybite': 'Bybit',
    'Hyper': 'Hyper',
    'KuCoin': 'KuCoin',
    'MexC': 'MEXC'
}

_funding_cache = {}

exchange_instances = {
    'Bybit': ccxt.bybit({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}),
    'BingX': ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}),
    'KuCoin': ccxt.kucoinfutures({'enableRateLimit': True}),
    'MEXC': ccxt.mexc({'options': {'defaultType': 'swap'}, 'enableRateLimit': True}),
    'Hyper': ccxt.hyperliquid({'enableRateLimit': True}),
    'Aster': None,
}

# ---------- Основные функции работы с данными ----------
def load_funding_results(exchange_folder):
    path = BASE_DIR / exchange_folder / f"funding_results_{exchange_folder.lower()}.json"
    if not path.exists():
        files = list((BASE_DIR / exchange_folder).glob("funding_results_*.json"))
        if not files:
            return {}
        path = files[0]
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки {exchange_folder}: {e}")
        return {}

def get_funding_results(exchange):
    if exchange not in _funding_cache:
        _funding_cache[exchange] = load_funding_results(exchange)
    return _funding_cache[exchange]

def get_last_update_time():
    if LAST_UPDATE_FILE.exists():
        with open(LAST_UPDATE_FILE, 'r') as f:
            return f.read().strip()
    return None

def save_last_update_time():
    with open(LAST_UPDATE_FILE, 'w') as f:
        f.write(datetime.now().isoformat())

def run_updater():
    try:
        subprocess.run(
            [sys.executable, str(BASE_DIR / "run_all_fetch_funding_parallel.py")],
            capture_output=True,
            text=True,
            timeout=1800
        )
        save_last_update_time()
        print(f"✅ Данные обновлены в {datetime.now().isoformat()}")
        _funding_cache.clear()
    except Exception as e:
        print(f"❌ Ошибка при обновлении: {e}")

def start_scheduler():
    def schedule():
        while True:
            time.sleep(3600)
            run_updater()
    thread = threading.Thread(target=schedule, daemon=True)
    thread.start()

def find_coin_key(data: dict, coin: str) -> str:
    if not data:
        return None
    if coin in data:
        return coin
    candidate = f"{coin}/USDT:USDT"
    if candidate in data:
        return candidate
    candidate2 = f"{coin}USDT"
    if candidate2 in data:
        return candidate2
    candidate3 = f"{coin}/USDT"
    if candidate3 in data:
        return candidate3
    for k in data:
        if k.startswith(f"{coin}/"):
            return k
    for k in data:
        if coin in k:
            return k
    return None

def get_current_price(exchange_name, symbol):
    exch = exchange_instances.get(exchange_name)
    if not exch:
        return None
    try:
        ticker = exch.fetch_ticker(symbol)
        price = ticker.get('last') or ticker.get('markPrice') or ticker.get('close')
        if price is None:
            orderbook = exch.fetch_order_book(symbol, limit=1)
            if orderbook['bids'] and orderbook['asks']:
                price = (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2
        return round(float(price), 4) if price else None
    except Exception as e:
        print(f"Price error for {exchange_name} {symbol}: {e}")
        return None

def get_orderbook_stats(exchange_name, symbol):
    exch = exchange_instances.get(exchange_name)
    if not exch:
        return None, None
    try:
        orderbook = exch.fetch_order_book(symbol, limit=20)
        bids = orderbook['bids']
        asks = orderbook['asks']
        if not bids or not asks:
            return None, None
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        spread = (best_ask - best_bid) / best_bid * 100
        total_volume = 0
        for price, amount in asks[:20]:
            total_volume += price * amount
        for price, amount in bids[:20]:
            total_volume += price * amount
        return round(total_volume, 2), round(spread, 4)
    except Exception as e:
        print(f"Orderbook error for {exchange_name} {symbol}: {e}")
        return None, None

# ---------- API эндпоинты ----------
@app.route('/api/funding-data', methods=['GET'])
def get_funding_data():
    all_exchanges = {}
    for folder in EXCHANGES:
        name = EXCHANGE_NAMES.get(folder, folder)
        data = get_funding_results(folder)
        parsed = {}
        for symbol, fund_data in data.items():
            clean_symbol = symbol.split('/')[0].split(':')[0]
            parsed[clean_symbol] = {
                'currentFR': fund_data.get('currentFR'),
                '24h': fund_data.get('24h'),
                '48h': fund_data.get('48h'),
                '168h': fund_data.get('168h'),
                '720h': fund_data.get('720h'),
            }
        all_exchanges[name] = parsed
    all_symbols = set()
    for exc_data in all_exchanges.values():
        all_symbols.update(exc_data.keys())
    sorted_symbols = sorted(all_symbols)
    return jsonify({
        'exchanges': all_exchanges,
        'symbols': sorted_symbols,
        'total_exchanges': len(all_exchanges),
        'total_symbols': len(sorted_symbols),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/compare', methods=['POST'])
def compare_coin():
    data = request.get_json()
    coin = data.get('coin')
    exchange1 = data.get('exchange1')
    exchange2 = data.get('exchange2')
    if not coin or not exchange1 or not exchange2:
        return jsonify({'error': 'Missing parameters'}), 400

    exchange_to_folder = {v: k for k, v in EXCHANGE_NAMES.items()}
    folder1 = exchange_to_folder.get(exchange1)
    folder2 = exchange_to_folder.get(exchange2)
    if not folder1 or not folder2:
        return jsonify({'error': 'Unknown exchange'}), 400

    data1 = get_funding_results(folder1)
    data2 = get_funding_results(folder2)

    key1 = find_coin_key(data1, coin)
    key2 = find_coin_key(data2, coin)

    if not key1 or not key2:
        return jsonify({'error': f'Coin {coin} not found on one of the exchanges'}), 404

    coin_data1 = data1[key1]
    coin_data2 = data2[key2]

    # Orderbook и цены
    vol1, spread1 = get_orderbook_stats(exchange1, key1)
    vol2, spread2 = get_orderbook_stats(exchange2, key2)
    price1 = get_current_price(exchange1, key1)
    price2 = get_current_price(exchange2, key2)
    price_spread = None
    if price1 and price2 and price2 != 0:
        price_spread = round((price1 - price2) / price2 * 100, 4)

    oi1 = coin_data1.get('openInterest')
    oi2 = coin_data2.get('openInterest')

    # ----- 1. Получаем данные из CoinMarketCap (maxSupply, circulatingSupply) -----
    cmc_result = enrich_with_cmc(coin, [price1, price2])

    # ----- 2. Получаем данные о держателях из BaseScan -----
    holders_count, holders_concentration = get_token_holders(coin, [price1, price2])
    if holders_count is not None:
        cmc_result['holders'] = holders_count
    if holders_concentration is not None:
        cmc_result['topHoldersConcentration'] = holders_concentration

    # Формирование ответа
    result = {
        'coin': coin,
        'exchange1': exchange1,
        'exchange2': exchange2,
        'stats': {
            'volume24h': {
                exchange1: coin_data1.get('volume24hUSD', 0),
                exchange2: coin_data2.get('volume24hUSD', 0)
            },
            'openInterest': {
                exchange1: oi1,
                exchange2: oi2
            },
            'orderbookVolume': {
                exchange1: vol1,
                exchange2: vol2
            },
            'orderbookSpread': {
                exchange1: spread1,
                exchange2: spread2
            },
            'fundingRate': {
                exchange1: coin_data1.get('currentFR', 0),
                exchange2: coin_data2.get('currentFR', 0)
            },
            'fundingInterval': {
                exchange1: coin_data1.get('fundingIntervalHours', 8),
                exchange2: coin_data2.get('fundingIntervalHours', 8)
            },
            'currentPrice': {
                exchange1: price1,
                exchange2: price2
            },
            'priceSpreadPercent': price_spread
        },
        'cmc': cmc_result
    }
    return jsonify(result)

@app.route('/api/last-update', methods=['GET'])
def last_update():
    return jsonify({'lastUpdate': get_last_update_time()})

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'base_dir': str(BASE_DIR)})

if __name__ == '__main__':
    start_scheduler()
    threading.Thread(target=run_updater, daemon=True).start()
    print("🚀 Funding API запущен на http://localhost:5000")
    app.run(debug=False, port=5000)