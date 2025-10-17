import ccxt
import json

DATA_DIR = "D:/Ilya/My project/FIW_soft/FIW_soft/Gate"

# Создаём экземпляр Gate.io
ex = ccxt.gateio()

# Загружаем рынки
print("Загрузка рынков Gate.io...")
ex.load_markets()

# Фильтруем: только perpetual (swap) и котировка USDT
perp_symbols = [
    symbol for symbol, market in ex.markets.items()
    if market.get('swap') is True and market.get('quote') == 'USDT'
]

# Сортируем для удобства
perp_symbols.sort()

print(f"Найдено {len(perp_symbols)} perpetual-пар с USDT котировкой.")
print("Примеры:", perp_symbols[:5])

# Сохраняем в файл
with open(f"{DATA_DIR}/tradePairsGate.json", "w", encoding="utf-8") as f:
    json.dump(perp_symbols, f, indent=4, ensure_ascii=False)

print("\nСписок успешно сохранён в tradePairsGate.json")