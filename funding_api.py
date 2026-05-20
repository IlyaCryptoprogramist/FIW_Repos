from flask import Flask, jsonify
from flask_cors import CORS
import json
import os
from pathlib import Path
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent

EXCHANGES = ['Aster', 'BingX', 'Bybite', 'Gate', 'Htx', 'Hyper', 'KuCoin', 'MexC']

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

# Конфигурация нормализации для каждой биржи
NORMALIZATION_CONFIG = {
    'BingX': {
        'multiply_by_100': False,  # Не умножаем, а делим если нужно
        'divide_by_100_if_abs_gt': 0.005,  # Если |value| > 0.005, делим на 100
        'description': 'BingX: 0.088 = 8.8% -> нормализуем в 0.00088'
    },
    'Bybit': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,  # Если |value| > 0.01, делим на 100
        'description': 'Bybit: 0.032715 = 3.27% -> нормализуем в 0.000327'
    },
    'MEXC': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.5,  # MEXC: -0.8936 = -89.36% -> делим на 100
        'description': 'MEXC: -0.8936 = -89.36% -> нормализуем в -0.008936'
    },
    'KuCoin': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,
        'description': 'KuCoin: стандартная нормализация'
    },
    'Gate': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,
        'description': 'Gate: стандартная нормализация'
    },
    'HTX': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,
        'description': 'HTX: стандартная нормализация'
    },
    'Hyper': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,
        'description': 'Hyper: стандартная нормализация'
    },
    'Aster': {
        'multiply_by_100': False,
        'divide_by_100_if_abs_gt': 0.01,
        'description': 'Aster: стандартная нормализация'
    }
}

def normalize_funding_value(value, exchange_name="", field_name="", coin_name=""):
    """
    Умная нормализация значений ставки финансирования
    
    Принцип: 
    - Если значение выглядит как проценты (> 0.5 или < -0.5), делим на 100
    - Для BingX и MEXC специальные правила, так как у них большие значения
    """
    if value is None:
        return None
    
    try:
        num_value = float(value)
    except (ValueError, TypeError):
        return None
    
    abs_value = abs(num_value)
    
    # Получаем конфиг для биржи
    config = NORMALIZATION_CONFIG.get(exchange_name, {})
    divide_threshold = config.get('divide_by_100_if_abs_gt', 0.5)
    
    # Специальная обработка для известных проблемных монет
    problem_coins = ['CHIP', 'ONT', 'PROMPT', 'SOON', 'SPK', 'IP']
    
    # Для MEXC с монетой CHIP - всегда делим на 100
    if exchange_name == 'MEXC' and coin_name == 'CHIP':
        return num_value / 100
    
    # Для BingX
    if exchange_name == 'BingX':
        # BingX: 0.01 = 1% (должно стать 0.0001)
        # BingX: 0.0094 = 0.94% (должно стать 0.000094)
        if abs_value > 0.005:  # Если больше 0.5%
            return num_value / 100
        return num_value
    
    # Для Bybit
    if exchange_name == 'Bybit':
        # Bybit: 0.032715 = 3.27% (должно стать 0.000327)
        if abs_value > 0.01:
            return num_value / 100
        return num_value
    
    # Для MEXC
    if exchange_name == 'MEXC':
        # MEXC: -0.8936 = -89.36% (должно стать -0.008936)
        # MEXC: 0.0234 = 2.34% (должно стать 0.000234)
        if abs_value > 0.01:
            return num_value / 100
        return num_value
    
    # Для остальных бирж
    # Если значение очень большое (> 1), точно делим
    if abs_value > 1:
        return num_value / 100
    
    # Если значение умеренное (0.01 - 1), проверяем контекст
    if 0.01 < abs_value <= 1:
        # Для полей 24h, 48h, 168h, 720h обычно значения небольшие
        # Если значение больше 0.5, скорее всего это проценты
        if abs_value > divide_threshold:
            return num_value / 100
        return num_value
    
    return num_value

def load_exchange_data(exchange_folder):
    """Загружает данные из JSON файла"""
    try:
        exchange_path = BASE_DIR / exchange_folder
        if not exchange_path.exists():
            return None
        
        json_files = list(exchange_path.glob("funding_results_*.json"))
        if not json_files:
            json_files = list(exchange_path.glob("*.json"))
            if not json_files:
                return None
        
        json_file = json_files[0]
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        print(f"Ошибка загрузки {exchange_folder}: {e}")
        return None

def parse_funding_data(data, exchange_name=""):
    """Парсит данные с нормализацией"""
    result = {}
    
    if not data:
        return result
    
    if isinstance(data, dict):
        for symbol, fund_data in data.items():
            # Очищаем символ
            clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            clean_symbol = clean_symbol.split(':')[0]
            
            # Нормализуем каждое поле
            result[clean_symbol] = {
                'currentFR': normalize_funding_value(
                    fund_data.get('currentFR'), exchange_name, 'currentFR', clean_symbol
                ),
                '24h': normalize_funding_value(
                    fund_data.get('24h'), exchange_name, '24h', clean_symbol
                ),
                '48h': normalize_funding_value(
                    fund_data.get('48h'), exchange_name, '48h', clean_symbol
                ),
                '168h': normalize_funding_value(
                    fund_data.get('168h'), exchange_name, '168h', clean_symbol
                ),
                '720h': normalize_funding_value(
                    fund_data.get('720h'), exchange_name, '720h', clean_symbol
                ),
                'fundingIntervalHours': fund_data.get('fundingIntervalHours', 8)
            }
            
            # Логирование для отладки проблемных монет
            if clean_symbol in ['SPK', 'CHIP', 'IP', 'ONT', 'PROMPT', 'SOON']:
                raw_24h = fund_data.get('24h')
                norm_24h = result[clean_symbol]['24h']
                if raw_24h is not None:
                    print(f"    {exchange_name} {clean_symbol}: 24h raw={raw_24h} -> norm={norm_24h} ({norm_24h*100 if norm_24h else 0:.4f}%)")
    
    elif isinstance(data, list):
        for item in data:
            symbol = item.get('symbol', '')
            if not symbol:
                continue
            
            clean_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            clean_symbol = clean_symbol.split(':')[0]
            
            result[clean_symbol] = {
                'currentFR': normalize_funding_value(
                    item.get('currentFR'), exchange_name, 'currentFR', clean_symbol
                ),
                '24h': normalize_funding_value(
                    item.get('24h'), exchange_name, '24h', clean_symbol
                ),
                '48h': normalize_funding_value(
                    item.get('48h'), exchange_name, '48h', clean_symbol
                ),
                '168h': normalize_funding_value(
                    item.get('168h'), exchange_name, '168h', clean_symbol
                ),
                '720h': normalize_funding_value(
                    item.get('720h'), exchange_name, '720h', clean_symbol
                ),
                'fundingIntervalHours': item.get('fundingIntervalHours', 8)
            }
    
    return result

@app.route('/api/funding-data', methods=['GET'])
def get_funding_data():
    """Возвращает данные по всем биржам"""
    all_exchanges_data = {}
    exchange_stats = {}
    
    print("\n" + "="*70)
    print("ЗАГРУЗКА ДАННЫХ С БИРЖ")
    print("="*70)
    
    for exchange_folder in EXCHANGES:
        exchange_name = EXCHANGE_NAMES.get(exchange_folder, exchange_folder)
        print(f"\n📊 {exchange_name}:")
        print(f"   Конфиг: {NORMALIZATION_CONFIG.get(exchange_name, {}).get('description', 'Стандартная')}")
        
        raw_data = load_exchange_data(exchange_folder)
        parsed_data = parse_funding_data(raw_data, exchange_name)
        all_exchanges_data[exchange_name] = parsed_data
        
        exchange_stats[exchange_name] = {
            'coin_count': len(parsed_data),
            'coins': list(parsed_data.keys())
        }
        
        print(f"   ✅ Загружено {len(parsed_data)} монет")
    
    all_symbols = set()
    for exchange_data in all_exchanges_data.values():
        all_symbols.update(exchange_data.keys())
    
    sorted_symbols = sorted(list(all_symbols))
    
    print("\n" + "="*70)
    print(f"ИТОГО: {len(all_exchanges_data)} бирж, {len(sorted_symbols)} монет")
    print("="*70 + "\n")
    
    return jsonify({
        'exchanges': all_exchanges_data,
        'symbols': sorted_symbols,
        'exchange_stats': exchange_stats,
        'total_exchanges': len(all_exchanges_data),
        'total_symbols': len(sorted_symbols),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/normalization-config', methods=['GET'])
def get_normalization_config():
    """Возвращает конфигурацию нормализации для отладки"""
    return jsonify({
        'config': NORMALIZATION_CONFIG,
        'description': 'Конфигурация нормализации значений для каждой биржи'
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Проверка работоспособности"""
    exchanges_found = []
    for exchange_folder in EXCHANGES:
        exchange_path = BASE_DIR / exchange_folder
        if exchange_path.exists():
            json_files = list(exchange_path.glob("*.json"))
            if json_files:
                exchanges_found.append({
                    'name': EXCHANGE_NAMES.get(exchange_folder, exchange_folder),
                    'folder': exchange_folder,
                    'files': [f.name for f in json_files]
                })
    
    return jsonify({
        'status': 'ok',
        'base_dir': str(BASE_DIR),
        'exchanges_configured': EXCHANGES,
        'exchanges_found': exchanges_found,
        'normalization_config': NORMALIZATION_CONFIG,
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("=" * 80)
    print("🐍 FUNDING RATE API SERVER v3.0 - Умная нормализация")
    print("=" * 80)
    print(f"📁 Базовая директория: {BASE_DIR}")
    print(f"🏦 Биржи: {', '.join(EXCHANGES)}")
    print("-" * 80)
    print("📊 Конфигурация нормализации:")
    for exchange, config in NORMALIZATION_CONFIG.items():
        print(f"   • {exchange}: {config['description']}")
    print("-" * 80)
    
    # Проверяем наличие папок
    print("🔍 Проверка папок с данными:")
    for exchange_folder in EXCHANGES:
        exchange_path = BASE_DIR / exchange_folder
        if exchange_path.exists():
            json_files = list(exchange_path.glob("*.json"))
            status = "✅" if json_files else "⚠️"
            print(f"  {status} {exchange_folder}: {len(json_files)} JSON файлов")
        else:
            print(f"  ❌ {exchange_folder}: папка не найдена")
    
    print("-" * 80)
    print("🌐 Запуск сервера на http://localhost:5000")
    print("📖 Доступные эндпоинты:")
    print("   GET /api/health - проверка статуса")
    print("   GET /api/funding-data - данные всех бирж")
    print("   GET /api/normalization-config - конфигурация нормализации")
    print("=" * 80)
    print("")
    
    app.run(debug=True, port=5000, host='0.0.0.0')