# hyper_getSymbols.py
import ccxt
import json

# Путь к папке с данными Hyperliquid
DATA_DIR = "D:/Ilya/My project/FIW_soft/FIW_soft/Hyper"

# Создаём экземпляр Hyperliquid
ex = ccxt.hyperliquid({
    'timeout': 30000,  # Увеличиваем таймаут до 30 секунд
})

# Загружаем рынки
print("Загрузка рынков Hyperliquid...")
markets = ex.load_markets()

# Фильтруем: только perpetual (swap) контракты
# У Hyperliquid часто используется формат BASE/USDC:USDC для perpetual
perp_symbols = [
    symbol for symbol, market in markets.items()
    if market.get('contract') is True and market.get('type') == 'swap'
]

# Сортируем для удобства
perp_symbols.sort()

print(f"Найдено {len(perp_symbols)} perpetual-контрактов.")
print("Примеры:", perp_symbols[:5])

# Сохраняем в файл
output_file_path = f"{DATA_DIR}/tradePairsHyper.json"
with open(output_file_path, "w", encoding="utf-8") as f:
    json.dump(perp_symbols, f, indent=4, ensure_ascii=False)

print(f"\nСписок успешно сохранён в {output_file_path}")