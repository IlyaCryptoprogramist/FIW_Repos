from flask import Flask, jsonify
from flask_cors import CORS
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

app = Flask(__name__)
CORS(app)  # Разрешаем CORS для React

# Конфигурация
BASE_DIR = Path(__file__).parent
EXCHANGES = ['Aster', 'BingX', 'Bybite', 'Gate', 'Htx', 'Hyper', 'KuCoin', 'MexC']

# Маппинг названий папок на отображаемые имена бирж
EXCHANGE_NAMES = {
    'Aster': 'Aster',
    'BingX': 'BingX',
    'Bybite': 'Bybit',
    'Gate': 'Gate',
    'Htx': 'HTX',
    'Hyper': 'Hyper',
    'KuCoin': 'KuCoin',
    'MexC': 'MEXC'
}

def load_exchange_data(exchange_folder):
    """Загружает данные из файла funding_results_{exchange}.json"""
    try:
        # Ищем файл в папке биржи
        exchange_path = BASE_DIR / exchange_folder
        if not exchange_path.exists():
            print(f"Папка {exchange_folder} не найдена")
            return None
        
        # Ищем файл funding_results_*.json
        json_files = list(exchange_path.glob("funding_results_*.json"))
        if not json_files:
            print(f"Файл funding_results_*.json не найден в {exchange_folder}")
            return None
        
        # Берем первый найденный файл
        json_file = json_files[0]
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        print(f"Ошибка загрузки данных из {exchange_folder}: {e}")
        return None

def parse_funding_data(data, exchange_name):
    """Парсит данные в единый формат"""
    result = {}
    
    if not data:
        return result
    
    # Определяем структуру данных в зависимости от биржи
    # Предполагаем, что данные могут быть в разных форматах
    
    # Вариант 1: данные в виде списка или словаря с символами
    if isinstance(data, dict):
        for symbol, fund_data in data.items():
            # Очищаем символ (убираем /USDT:USDT и т.д.)
            clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            clean_symbol = clean_symbol.split(':')[0]
            
            # Извлекаем значения
            result[clean_symbol] = {
                'currentFR': fund_data.get('currentFR', 0),
                '24h': fund_data.get('24h', 0),
                '168h': fund_data.get('168h', 0),
                '720h': fund_data.get('720h', 0),
                'fundingIntervalHours': fund_data.get('fundingIntervalHours', 8)
            }
    elif isinstance(data, list):
        # Вариант 2: данные в виде списка объектов
        for item in data:
            symbol = item.get('symbol', '')
            clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            
            result[clean_symbol] = {
                'currentFR': item.get('currentFR', 0),
                '24h': item.get('24h', 0),
                '168h': item.get('168h', 0),
                '720h': item.get('720h', 0),
                'fundingIntervalHours': item.get('fundingIntervalHours', 8)
            }
    
    return result

@app.route('/api/funding-data', methods=['GET'])
def get_funding_data():
    """Возвращает данные по всем биржам"""
    all_exchanges_data = {}
    
    for exchange_folder in EXCHANGES:
        exchange_name = EXCHANGE_NAMES.get(exchange_folder, exchange_folder)
        raw_data = load_exchange_data(exchange_folder)
        parsed_data = parse_funding_data(raw_data, exchange_name)
        all_exchanges_data[exchange_name] = parsed_data
    
    # Также возвращаем список доступных монет
    all_symbols = set()
    for exchange_data in all_exchanges_data.values():
        all_symbols.update(exchange_data.keys())
    
    # Сортируем символы для консистентности
    sorted_symbols = sorted(list(all_symbols))
    
    return jsonify({
        'exchanges': all_exchanges_data,
        'symbols': sorted_symbols,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/funding-data/<exchange>', methods=['GET'])
def get_exchange_data(exchange):
    """Возвращает данные по конкретной бирже"""
    # Находим папку по названию биржи
    exchange_folder = None
    for folder, name in EXCHANGE_NAMES.items():
        if name.lower() == exchange.lower():
            exchange_folder = folder
            break
    
    if not exchange_folder:
        return jsonify({'error': 'Exchange not found'}), 404
    
    raw_data = load_exchange_data(exchange_folder)
    parsed_data = parse_funding_data(raw_data, exchange)
    
    return jsonify({
        'exchange': exchange,
        'data': parsed_data,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    """Возвращает список всех доступных монет"""
    all_symbols = set()
    
    for exchange_folder in EXCHANGES:
        raw_data = load_exchange_data(exchange_folder)
        if raw_data:
            if isinstance(raw_data, dict):
                for symbol in raw_data.keys():
                    clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
                    clean_symbol = clean_symbol.split(':')[0]
                    all_symbols.add(clean_symbol)
            elif isinstance(raw_data, list):
                for item in raw_data:
                    symbol = item.get('symbol', '')
                    clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
                    all_symbols.add(clean_symbol)
    
    return jsonify({
        'symbols': sorted(list(all_symbols)),
        'count': len(all_symbols)
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)