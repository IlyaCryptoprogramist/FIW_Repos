import ccxt
import json

DATA_DIR = "D:/Ilya/My project/FIW_soft/FIW_soft/Htx"

# Создаём экземпляр HTX Futures
ex = ccxt.htx({
    'options': {
        'defaultType': 'swap',  # важно для фьючерсов
    },
    'verify': False  # <- Временно отключаем проверку SSL
})

# Загружаем рынки
print("Загрузка рынков HTX Futures...")
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
with open(f"{DATA_DIR}/tradePairsHtx.json", "w", encoding="utf-8") as f:
    json.dump(perp_symbols, f, indent=4, ensure_ascii=False)

print("\nСписок успешно сохранён в tradePairsHtx.json")