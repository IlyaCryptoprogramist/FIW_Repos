import asyncio
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from common.executor import UniversalExecutor
from common.file_utils import save_top10_results
import urllib3 # <-- Добавлен импорт urllib3

# Отключаем предупреждения urllib3, связанные с SSL (например, InsecureRequestWarning)
# Это нужно делать один раз при запуске программы.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


async def run_all_getSymbols():
    """Run getSymbols operation across all exchanges."""
    print("[INFO] Запуск операции getSymbols...")
    executor = UniversalExecutor("common/config.json")
    results = await executor.run_operation("getSymbols")
    return results


async def run_all_exchanges():
    """Run fetch_funding operation across all exchanges."""
    print("[INFO] Запуск операции fetch_funding...")
    executor = UniversalExecutor("common/config.json")
    results = await executor.run_operation("fetch_funding")
    return results


async def run_all_top10():
    """Process top10 data from all exchanges."""
    print("[INFO] Запуск операции process_top10...")
    executor = UniversalExecutor("common/config.json")
    
    # Run the JSON processing
    results = await executor.run_operation("process_top10")
    
    # Collect results and save
    all_exchange_data = {}
    successful_runs = 0
    for exchange_name, data, success in results:
        if success and data is not None:
            all_exchange_data[exchange_name] = data
            successful_runs += 1

    total_runs = len(results)
    print(f"\n[INFO] Все JSON-файлы топ-10 обработаны. Успешно: {successful_runs}/{total_runs}.")

    # Save all results to a single file
    save_top10_results(all_exchange_data)

    return results


def run_global_top10():
    """Process global top10 data from all exchanges."""
    print("[INFO] Запуск операции global_top10...")
    
    # Read config to get exchange directories
    with open("common/config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    
    base_dir = Path(__file__).parent
    exchange_dirs = [base_dir / d for d in config["exchange_directories"] if (base_dir / d).is_dir()]

    # Load data from all exchanges
    all_data_with_exchange = []
    for exchange_dir in exchange_dirs:
        exchange_name = exchange_dir.name
        expected_file_name = f"funding_results_{exchange_dir.name.lower()}.json"
        file_path = exchange_dir / expected_file_name
        if file_path.exists() and file_path.is_file():
            try:
                print(f"[INFO] Загружаю данные из {file_path} (биржа: {exchange_name})...")
                # Попробовать открыть в UTF-8
                with open(file_path, "r", encoding="utf-8") as f:
                    exchange_data = json.load(f)
                
            except UnicodeDecodeError:
                # Если UTF-8 не сработал, попробовать автоопределение
                print(f"[WARNING] UTF-8 не сработал для {file_path}, пробую автоопределение кодировки...")
                try:
                    import chardet
                    raw_data = file_path.read_bytes()
                    detected_encoding = chardet.detect(raw_data)['encoding']
                    print(f"[INFO] Обнаружена кодировка для {file_path}: {detected_encoding}")
                    with open(file_path, "r", encoding=detected_encoding) as f:
                        exchange_data = json.load(f)
                except ImportError:
                    print(f"[ERROR] Не удалось декодировать {file_path}. Установите 'chardet' для автоопределения кодировки.")
                    continue # Пропустить этот файл
                except Exception as e:
                    print(f"[ERROR] Не удалось декодировать {file_path} даже с автоопределением: {e}")
                    continue # Пропустить этот файл
            
            except json.JSONDecodeError:
                print(f"[ERROR] Файл {file_path} повреждён или не является JSON.")
                continue # Пропустить этот файл
            except Exception as e:
                print(f"[EXCEPTION] Ошибка при загрузке {file_path}: {e}")
                continue # Пропустить этот файл

            # Add each symbol from exchange to global list with exchange info
            for symbol, values in exchange_data.items():
                all_data_with_exchange.append({
                    'symbol': symbol,
                    'data': values,
                    'exchange': exchange_name
                })
        else:
            print(f"[WARNING] Файл funding_results не найден в {exchange_dir} по пути: {file_path}")

    if not all_data_with_exchange:
        print("[ERROR] Не найдено ни одного файла funding_results или все файлы пусты/повреждены.")
        return

    print(f"[INFO] Загружено данных по {len(all_data_with_exchange)} записям (символ + биржа) из всех бирж.")

    # Create global top 10
    sorted_24h_global = sorted(all_data_with_exchange, key=lambda x: x['data'].get('24h', 0), reverse=True)[:10]
    sorted_48h_global = sorted(all_data_with_exchange, key=lambda x: x['data'].get('48h', 0), reverse=True)[:10]
    sorted_168h_global = sorted(all_data_with_exchange, key=lambda x: x['data'].get('168h', 0), reverse=True)[:10]

    def print_top_list_global(title, sorted_list, fr_key):
        print(f"\n--- {title} (Глобальный топ-10 по всем биржам) ---")
        print("-" * 120) 
        print(f"{'Актив':<10} | {'Биржа':<10} | {'FR (накопл.)':>12} | {'Текущий FR':>12} | {'Интервал':>9} | {'Ask Vol':>12} | {'Bid Vol':>12}")
        print("-" * 120)
        for item in sorted_list:
            symbol = item['symbol']
            values = item['data']
            exchange = item['exchange']
            base = symbol.split('/')[0]
            fr_val = values.get(fr_key, 0)
            cur_fr = values.get('currentFR', None)
            interval = values.get('fundingIntervalHours', '?')
            ask_vol = values.get('askTotalVolume', 0)
            bid_vol = values.get('bidTotalVolume', 0)
            cur_fr_str = f"{cur_fr:>7.4f}%" if cur_fr is not None else "    N/A"
            print(f"{base:<10} | {exchange:<10} | {fr_val:>11.4f}% | {cur_fr_str:>12} | {interval:>8}ч | {ask_vol:>12.2f} | {bid_vol:>12.2f}")

    print_top_list_global("Топ-10 по Funding Rate (24h) — Все Биржи", sorted_24h_global, '24h')
    print_top_list_global("Топ-10 по Funding Rate (48h) — Все Биржи", sorted_48h_global, '48h')
    print_top_list_global("Топ-10 по Funding Rate (168h) — Все Биржи", sorted_168h_global, '168h')

    # Save global top 10 to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f"top10_all_exchanges_global_{timestamp}.json"
    output_file_path = base_dir / output_file_name
    
    try:
        sorted_global_results = {
            "top_10_by_24h": sorted_24h_global,
            "top_10_by_48h": sorted_48h_global,
            "top_10_by_168h": sorted_168h_global
        }
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(sorted_global_results, f, indent=4, ensure_ascii=False)
        print(f"\n[INFO] Глобальный топ-10 по всем биржам сохранён в: {output_file_path}")
    except Exception as e:
        print(f"[ERROR] Ошибка при сохранении глобального файла: {e}")


async def main():
    """Main function that runs the sequence: getSymbols -> exchanges -> top10."""
    print("[INFO] Запуск основного процесса...")
    
    # Step 1: Run all getSymbols
    await run_all_getSymbols()
    
    # Step 2: Run all exchanges (fetch funding)
    await run_all_exchanges()
    
    # Step 3: Process top10 data
    await run_all_top10()
    
    print("[INFO] Все операции завершены.")


if __name__ == "__main__":
    asyncio.run(main())