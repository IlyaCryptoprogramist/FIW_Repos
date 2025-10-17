import ccxt.async_support as ccxt
import asyncio

async def test():
    hyper = ccxt.hyperliquid({'timeout': 3000})
    # Загружаем рынки
    markets = await hyper.load_markets()
    
    # Найдём perpetual-контракты (обычно это контракты с 'swap' или 'contract')
    perpetual_symbols = []
    for symbol, market in markets.items():
        if market.get('contract') and market.get('type') == 'swap':
            perpetual_symbols.append(symbol)
    
    if not perpetual_symbols:
        print("❌ Не найдено ни одного perpetual-контракта.")
        print("Все доступные рынки (пример):")
        for sym, det in list(markets.items())[:10]: # Показываем первые 10
            print(f"  {sym}: {det.get('type')}, contract: {det.get('contract')}")
        await hyper.close()
        return

    print(f"✅ Найдено {len(perpetual_symbols)} perpetual-контрактов. Примеры:")
    for symbol in perpetual_symbols[:5]: # Показываем первые 5
        print(f"  - {symbol}")

    # Попробуем вызвать метод history для первого из найденных perpetual-символов
    symbol_to_test = perpetual_symbols[0]
    print(f"\n🔍 Тестируем {symbol_to_test}...")
    try:
        # Попробуйте вызвать метод без `since` и `limit`
        history = await hyper.fetch_funding_rate_history(symbol_to_test)
        print(f"История для {symbol_to_test}: {len(history)} записей")
        print(history[:2]) # Покажет первые 2 записи, если есть
    except Exception as e:
        print(f"❌ Ошибка при получении истории для {symbol_to_test}: {e}")

    # Также попробуем получить текущий FR
    try:
        current_fr = await hyper.fetch_funding_rate(symbol_to_test)
        print(f"Текущий FR для {symbol_to_test}: {current_fr}")
    except Exception as e:
        print(f"❌ Ошибка при получении текущего FR для {symbol_to_test}: {e}")

    await hyper.close()

asyncio.run(test())