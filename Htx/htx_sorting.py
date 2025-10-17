# sorting_htx.py

import json

DATA_DIR = "D:/Ilya/My project/FIW_soft/FIW_soft/Htx"
input_file = f"{DATA_DIR}/funding_results_htx.json"

try:
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Файл {input_file} не найден.")
    data = {}
except json.JSONDecodeError:
    print(f"Файл {input_file} повреждён.")
    data = {}

if not data:
    print("Файл данных пуст или не содержит результатов.")
else:
    sorted_24h = sorted(data.items(), key=lambda x: x[1].get('24h', 0), reverse=True)[:10]
    sorted_48h = sorted(data.items(), key=lambda x: x[1].get('48h', 0), reverse=True)[:10]
    sorted_168h = sorted(data.items(), key=lambda x: x[1].get('168h', 0), reverse=True)[:10]

    def print_top_list(title, sorted_list, fr_key):
        print(f"\n{title}:")
        print("-" * 110)
        print(f"{'Актив':<10} | {'FR (накопл.)':>12} | {'Текущий FR':>12} | {'Интервал':>9} | {'Ask Vol':>12} | {'Bid Vol':>12}")
        print("-" * 110)
        for symbol, values in sorted_list:
            base = symbol.split('/')[0]  # ← HTX использует формат BTC-USDT
            fr_val = values.get(fr_key, 0)
            cur_fr = values.get('currentFR', None)
            interval = values.get('fundingIntervalHours', '?')
            ask_vol = values.get('askTotalVolume', 0)
            bid_vol = values.get('bidTotalVolume', 0)
            cur_fr_str = f"{cur_fr:>7.4f}%" if cur_fr is not None else "    N/A"
            print(f"{base:<10} | {fr_val:>11.4f}% | {cur_fr_str:>12} | {interval:>8}ч | {ask_vol:>12.2f} | {bid_vol:>12.2f}")

    print_top_list("Топ-10 по Funding Rate (24h) — HTX", sorted_24h, '24h')
    print_top_list("Топ-10 по Funding Rate (48h) — HTX", sorted_48h, '48h')
    print_top_list("Топ-10 по Funding Rate (168h) — HTX", sorted_168h, '168h')

    # Сохранение
    output_file = f"{DATA_DIR}/top10_sorted_funding_results_htx.json"
    try:
        sorted_results = {
            "top_10_by_24h": dict(sorted_24h),
            "top_10_by_48h": dict(sorted_48h),
            "top_10_by_168h": dict(sorted_168h)
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(sorted_results, f, indent=4, ensure_ascii=False)
        print(f"\nТоп-10 HTX сохранён в: {output_file}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")