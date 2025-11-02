# find_coin_data.py

import json
import os
from pathlib import Path
import sys

# Add the common directory to the path so we can import from it
sys.path.append(os.path.join(os.path.dirname(__file__), 'common'))

from common.executor import UniversalExecutor  # Import from the new architecture


def load_exchange_data():
    """Загружает данные всех бирж в память при старте скрипта."""
    print("Загрузка данных с бирж...")
    
    # Read config to get exchange directories and file patterns
    with open("common/config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    
    base_dir = Path(__file__).parent
    exchange_dirs = [base_dir / d for d in config["exchange_directories"] if (base_dir / d).is_dir()]
    
    # Dictionary to store data for each exchange
    all_exchange_data = {}
    
    for exchange_dir in exchange_dirs:
        exchange_name = exchange_dir.name
        # Look for funding results files in each exchange directory
        funding_files = list(exchange_dir.glob("funding_results_*.json"))
        if funding_files:
            # Take the first funding results file found
            file_path = funding_files[0]
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        all_exchange_data[exchange_name] = data
                    print(f"  - {exchange_name}: загружено {len(data)} записей.")
                except json.JSONDecodeError:
                    print(f"[ОШИБКА] Невозможно прочитать JSON файл для {exchange_name}: {file_path}")
                    all_exchange_data[exchange_name] = {}
                except Exception as e:
                    print(f"[ОШИБКА] Проблема с файлом {exchange_name}: {e}")
                    all_exchange_data[exchange_name] = {}
            else:
                print(f"[ПРЕДУПРЕЖДЕНИЕ] Файл для {exchange_name} не найден: {file_path}")
                all_exchange_data[exchange_name] = {}
        else:
            print(f"[ПРЕДУПРЕЖДЕНИЕ] Файл funding_results не найден для {exchange_name} в {exchange_dir}")
            all_exchange_data[exchange_name] = {}
    
    print("Загрузка завершена.\n")
    return all_exchange_data


def find_coin_data(coin_name: str, all_exchange_data: dict):
    """
    Ищет данные по монете на всех биржах.
    coin_name: строка с именем монеты, например 'BTC', 'ETH'.
    """
    print(f"\n--- Поиск данных для монеты: {coin_name.upper()} ---\n")
    found_any = False

    for exchange, data in all_exchange_data.items():
        # Ищем пары, содержащие coin_name (регистронезависимо)
        matches = {pair: info for pair, info in data.items() if coin_name.upper() in pair.upper()}

        if matches:
            found_any = True
            print(f"--- {exchange} ---")
            for pair, info in matches.items():
                print(f"  Пара: {pair}")
                # Проверяем, есть ли нужные ключи в данных
                h24 = info.get('24h', 'N/A')
                h48 = info.get('48h', 'N/A')
                h168 = info.get('168h', 'N/A')
                print(f"    24ч: {h24}")
                print(f"    48ч: {h48}")
                print(f"    168ч: {h168}")
                # Выводим другие поля, если нужно, например, текущий FR
                current_fr = info.get('currentFR', 'N/A')
                print(f"    Текущий FR: {current_fr}")
                print("  ---")
            print("") # Пустая строка между биржами
        else:
            # print(f"[INFO] {exchange}: данные для {coin_name.upper()} не найдены.")
            pass # Не выводим, если нет совпадений

    if not found_any:
        print(f"Данные для монеты {coin_name.upper()} не найдены ни на одной из бирж.")


def main():
    all_exchange_data = load_exchange_data()
    
    print("Скрипт поиска данных о монетах запущен.")
    print("Введите имя монеты (например, BTC) для поиска.")
    print("Введите 'quit' или 'exit' для выхода из скрипта.\n")

    while True:
        coin_to_search = input("Введите имя монеты (или 'quit'/'exit' для выхода): ").strip()

        if not coin_to_search:
            print("Имя монеты не может быть пустым. Попробуйте снова.\n")
            continue

        if coin_to_search.lower() in ('quit', 'exit'):
            print("Завершение работы скрипта.")
            break

        find_coin_data(coin_to_search, all_exchange_data)
        print("-" * 40) # Разделитель между поисками


if __name__ == "__main__":
    main()