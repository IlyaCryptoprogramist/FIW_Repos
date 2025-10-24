from flask import Flask, jsonify, request
from flask_cors import CORS
import json
from pathlib import Path

app = Flask(__name__)
CORS(app)  # Это заменит ваш @app.after_request код

# Определяем путь к проекту FIW_soft
BASE_DIR = Path("D:/Ilya/My project/FIW_soft/FIW_soft")

# Словарь: имя биржи -> путь к файлу с результатами
EXCHANGE_DATA_FILES = {
    'Gate': BASE_DIR / 'Gate' / 'funding_results_gate.json',
    'KuCoin': BASE_DIR / 'KuCoin' / 'funding_results_kucoin.json',
    'Mexc': BASE_DIR / 'Mexc' / 'funding_results_mexc.json',
    'Hyper': BASE_DIR / 'Hyper' / 'funding_results_hyper.json',
    'HTX': BASE_DIR / 'HTX' / 'funding_results_htx.json',
    'Bybit': BASE_DIR / 'Bybite' / 'funding_results_bybite.json',
    'BingX': BASE_DIR / 'BingX' / 'funding_results_bingx.json',
}

# Загружаем все данные один раз при старте
ALL_EXCHANGE_DATA = {}

def load_exchange_data():
    """Загружает данные всех бирж в память при старте сервера."""
    print("Загрузка данных с бирж...")
    for exchange, file_path in EXCHANGE_DATA_FILES.items():
        if not file_path.exists():
            print(f"[ПРЕДУПРЕЖДЕНИЕ] Файл для {exchange} не найден: {file_path}")
            ALL_EXCHANGE_DATA[exchange] = {}
            continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                ALL_EXCHANGE_DATA[exchange] = data
            print(f"  - {exchange}: загружено {len(data)} записей.")
        except json.JSONDecodeError:
            print(f"[ОШИБКА] Невозможно прочитать JSON файл для {exchange}: {file_path}")
            ALL_EXCHANGE_DATA[exchange] = {}
        except Exception as e:
            print(f"[ОШИБКА] Проблема с файлом {exchange}: {e}")
            ALL_EXCHANGE_DATA[exchange] = {}
    print("Загрузка завершена.\n")

@app.route('/api/search/<coin_name>', methods=['GET'])
def search_coin(coin_name):
    """
    API endpoint для поиска монеты по всем биржам
    """
    if not coin_name:
        return jsonify({'error': 'Coin name is required'}), 400
    
    results = {}
    coin_name_upper = coin_name.upper()

    for exchange, data in ALL_EXCHANGE_DATA.items():
        # Ищем пары, содержащие coin_name (регистронезависимо)
        matches = {pair: info for pair, info in data.items() if coin_name_upper in pair.upper()}
        
        if matches:
            results[exchange] = matches

    return jsonify({
        'coin': coin_name,
        'results': results,
        'total_matches': sum(len(matches) for matches in results.values())
    })

@app.route('/api/exchanges', methods=['GET'])
def get_exchanges():
    """API endpoint для получения списка доступных бирж"""
    return jsonify({
        'exchanges': list(ALL_EXCHANGE_DATA.keys()),
        'total_exchanges': len(ALL_EXCHANGE_DATA)
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """API endpoint для проверки работы сервера"""
    return jsonify({'status': 'healthy', 'loaded_exchanges': len(ALL_EXCHANGE_DATA)})

if __name__ == '__main__':
    # Загружаем данные при запуске сервера
    load_exchange_data()
    
    print("Funding Rates API Server started!")
    print("Available endpoints:")
    print("  GET /api/health - Health check")
    print("  GET /api/exchanges - List of exchanges")
    print("  GET /api/search/<coin_name> - Search coin across all exchanges")
    
    app.run(debug=True, port=5000, host='0.0.0.0')