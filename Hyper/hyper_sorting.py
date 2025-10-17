# sorting.py

import json

DATA_DIR = "D:\Ilya\My project\FIW_soft\FIW_soft\Hyper"
# Загружаем данные из JSON файла
input_filename = f"{DATA_DIR}/funding_results_hyper.json"
try:
    with open(input_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Файл {input_filename} не найден.")
    data = {}
except json.JSONDecodeError:
    print(f"Файл {input_filename} не содержит валидный JSON.")
    data = {}

# Сортируем по каждому интервалу
sorted_24h = sorted(data.items(), key=lambda item: item[1].get('24h', 0), reverse=True)[:10]
sorted_48h = sorted(data.items(), key=lambda item: item[1].get('48h', 0), reverse=True)[:10]
sorted_168h = sorted(data.items(), key=lambda item: item[1].get('168h', 0), reverse=True)[:10]

# Обновлённая функция: выводит и накопленный FR, и текущий
def print_top_list(title, sorted_list, fr_key):
    print(f"\n{title}:")
    print("-" * 90)
    print(f"{'Актив':<10} | {'FR (накопл.)':>12} | {'Текущий FR':>12} | {'Ask Vol':>12} | {'Bid Vol':>12}")
    print("-" * 90)
    for symbol, values in sorted_list:
        base = symbol.split('/')[0]
        fr_value = values.get(fr_key, 0)          # накопленный за период
        current_fr = values.get('currentFR', None)  # текущий (real-time)
        ask_vol = values.get('askTotalVolume', 0)
        bid_vol = values.get('bidTotalVolume', 0)

        # Форматируем текущий FR: если None — пишем "N/A"
        current_fr_str = f"{current_fr:>7.4f}%" if current_fr is not None else "    N/A"
        print(f"{base:<10} | {fr_value:>11.4f}% | {current_fr_str:>12} | {ask_vol:>12.2f} | {bid_vol:>12.2f}")

# Выводим топы с правильными интервалами
print_top_list("Топ-10 по Funding Rate (24h)", sorted_24h, '24h')
print_top_list("Топ-10 по Funding Rate (48h)", sorted_48h, '48h')
print_top_list("Топ-10 по Funding Rate (168h)", sorted_168h, '168h')

# --- Сохранение отсортированных данных (всё как раньше) ---
sorted_results = {
    "top_10_by_24h": {symbol: values for symbol, values in sorted_24h},
    "top_10_by_48h": {symbol: values for symbol, values in sorted_48h},
    "top_10_by_168h": {symbol: values for symbol, values in sorted_168h}
}

output_filename = f"{DATA_DIR}/top10_sorted_funding_results_hyper.json"
try:
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(sorted_results, f, indent=4, ensure_ascii=False)
    print(f"\nТоп-10 результаты сохранены в файл: {output_filename}")
except Exception as e:
    print(f"Ошибка при сохранении файла: {e}")