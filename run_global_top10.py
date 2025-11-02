import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def main():
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
                with open(file_path, "r", encoding="utf-8") as f:
                    exchange_data = json.load(f)
                
                # Add each symbol from exchange to global list with exchange info
                for symbol, values in exchange_data.items():
                    all_data_with_exchange.append({
                        'symbol': symbol,
                        'data': values,
                        'exchange': exchange_name
                    })
            except json.JSONDecodeError:
                print(f"[ERROR] Файл {file_path} повреждён или не является JSON.")
            except Exception as e:
                print(f"[EXCEPTION] Ошибка при загрузке {file_path}: {e}")
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


if __name__ == "__main__":
    main()