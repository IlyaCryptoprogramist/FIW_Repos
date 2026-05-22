from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from pathlib import Path
from datetime import datetime

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent

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

def find_coin_key(data, coin):
    """
    Ищет ключ в словаре data, соответствующий монете coin.
    Сначала пробует точное совпадение, затем coin + "/USDT:USDT",
    затем любой ключ, начинающийся с coin + "/", затем любой ключ, содержащий coin как подстроку в начале.
    """
    if not coin:
        return None
    # 1. Точное совпадение
    if coin in data:
        return coin
    # 2. coin + "/USDT:USDT"
    candidate = f"{coin}/USDT:USDT"
    if candidate in data:
        return candidate
    # 3. coin + "/USDT:USDT" с другим регистром? (не нужно)
    # 4. Ищем любой ключ, который начинается с coin + "/"
    for key in data:
        if key.startswith(coin + '/'):
            return key
    # 5. Для монет без слеша (например, BTCUSDT) – ищем ключ, который начинается с coin
    for key in data:
        if key.startswith(coin):
            return key
    # 6. Ищем вхождение coin как подстроки (менее строго)
    for key in data:
        if coin in key:
            return key
    return None

@app.route('/api/funding-data', methods=['GET'])
def get_funding_data():
    all_exchanges = {}
    print("\n" + "="*60)
    print("Загрузка сырых данных (без изменений)")
    print("="*60)

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
        print(f"{name}: загружено {len(parsed)} монет")

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

    # Ищем ключ для монеты в каждой бирже
    key1 = find_coin_key(data1, coin)
    key2 = find_coin_key(data2, coin)
    if not key1 or not key2:
        missing = []
        if not key1:
            missing.append(exchange1)
        if not key2:
            missing.append(exchange2)
        return jsonify({'error': f'Coin {coin} not found on: {", ".join(missing)}'}), 404

    coin_data1 = data1[key1]
    coin_data2 = data2[key2]

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
                exchange1: None,
                exchange2: None
            },
            'orderbookVolume': {
                exchange1: None,
                exchange2: None
            },
            'orderbookSpread': {
                exchange1: None,
                exchange2: None
            },
            'fundingRate': {
                exchange1: coin_data1.get('currentFR', 0),
                exchange2: coin_data2.get('currentFR', 0)
            },
            'fundingInterval': {
                exchange1: coin_data1.get('fundingIntervalHours', 8),
                exchange2: coin_data2.get('fundingIntervalHours', 8)
            }
        },
        'cmc': {
            'maxSupply': None,
            'circulatingSupply': None,
            'holders': None,
            'topHoldersConcentration': None
        }
    }
    return jsonify(result)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'base_dir': str(BASE_DIR)})

if __name__ == '__main__':
    print("🚀 Funding API запущен на http://localhost:5000")
    app.run(debug=True, port=5000)