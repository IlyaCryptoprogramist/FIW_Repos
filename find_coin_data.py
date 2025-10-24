# find_coin_data.py

import json
import os
from pathlib import Path

# Определяем путь к проекту FIW_soft
BASE_DIR = Path("D:/Ilya/My project/FIW_soft/FIW_soft")

# Словарь: имя биржи -> путь к файлу с результатами
EXCHANGE_DATA_FILES = {
    'Gate': BASE_DIR / 'Gate' / 'funding_results_gate.json',
    'KuCoin': BASE_DIR / 'KuCoin' / 'funding_results_kucoin.json', # Уточни имя файла
    'Mexc': BASE_DIR / 'Mexc' / 'funding_results_mexc.json',       # Уточни имя файла
    'Hyper': BASE_DIR / 'Hyper' / 'funding_results_hyper.json',    # Уточни имя файла
    'HTX': BASE_DIR / 'HTX' / 'funding_results_htx.json',          # Уточни имя файла
    'Bybit': BASE_DIR / 'Bybite' / 'funding_results_bybite.json',    # Bybite -> Bybit, уточни имя файла
    'BingX': BASE_DIR / 'BingX' / 'funding_results_bingx.json',    # Уточни имя файла
}

# Загружаем все данные один раз при старте
ALL_EXCHANGE_DATA = {}

def load_exchange_data():
    """Загружает данные всех бирж в память при старте скрипта."""
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


def find_coin_data(coin_name: str):
    """
    Ищет данные по монете на всех биржах.
    coin_name: строка с именем монеты, например 'BTC', 'ETH'.
    """
    print(f"\n--- Поиск данных для монеты: {coin_name.upper()} ---\n")
    found_any = False

    for exchange, data in ALL_EXCHANGE_DATA.items():
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
    load_exchange_data()
    
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

        find_coin_data(coin_to_search)
        print("-" * 40) # Разделитель между поисками


if __name__ == "__main__":
    main()
