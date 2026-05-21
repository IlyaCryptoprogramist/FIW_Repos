from flask import Flask, jsonify
from flask_cors import CORS
import json
from pathlib import Path
from datetime import datetime

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent

# Папки с данными и отображаемые названия бирж
EXCHANGES = ['Aster', 'BingX', 'Bybite', 'Hyper', 'KuCoin', 'MexC']
EXCHANGE_NAMES = {
    'Aster': 'Aster',
    'BingX': 'BingX',
    'Bybite': 'Bybit',
    'Hyper': 'Hyper',
    'KuCoin': 'KuCoin',
    'MexC': 'MEXC'
}

def load_exchange_data(exchange_folder):
    """Загружает первый JSON-файл из папки биржи."""
    try:
        path = BASE_DIR / exchange_folder
        if not path.exists():
            return None
        # Сначала ищем funding_results_*.json
        files = list(path.glob("funding_results_*.json"))
        if not files:
            # Иначе любой *.json
            files = list(path.glob("*.json"))
        if not files:
            return None
        with open(files[0], 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки {exchange_folder}: {e}")
        return None

def parse_raw_data(data):
    """
    Извлекает из сырых данных словарь {монета: {currentFR, 24h, 48h, 168h, 720h}}
    без изменения числовых значений.
    """
    result = {}
    if not data:
        return result

    if isinstance(data, dict):
        items = data.items()
    elif isinstance(data, list):
        items = ((item.get('symbol', ''), item) for item in data)
    else:
        return result

    for symbol, fund_data in items:
        if not symbol:
            continue
        # Очищаем символ: "BTC/USDT:USDT" -> "BTC"
        clean_symbol = symbol.split('/')[0].split(':')[0]
        result[clean_symbol] = {
            'currentFR': fund_data.get('currentFR'),
            '24h': fund_data.get('24h'),
            '48h': fund_data.get('48h'),
            '168h': fund_data.get('168h'),
            '720h': fund_data.get('720h'),
        }
    return result

@app.route('/api/funding-data', methods=['GET'])
def get_funding_data():
    """Возвращает данные по всем биржам в原始 виде (без нормализации)."""
    all_exchanges = {}
    print("\n" + "="*60)
    print("Загрузка сырых данных (без изменений)")
    print("="*60)

    for folder in EXCHANGES:
        name = EXCHANGE_NAMES.get(folder, folder)
        raw = load_exchange_data(folder)
        parsed = parse_raw_data(raw)
        all_exchanges[name] = parsed
        print(f"{name}: загружено {len(parsed)} монет")

    # Собираем все уникальные символы
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

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'base_dir': str(BASE_DIR)})

if __name__ == '__main__':
    print("🚀 Funding API (raw data) запущен на http://localhost:5000")
    app.run(debug=True, port=5000)