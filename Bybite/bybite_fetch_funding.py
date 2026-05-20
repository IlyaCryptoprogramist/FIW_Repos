# import ccxt.async_support as ccxt
# import asyncio
# import json
# from datetime import datetime, timedelta
# from collections import Counter
# import time
# from pathlib import Path

# # Use relative path based on current file location
# DATA_DIR = Path(__file__).parent

# bybit = ccxt.bybit({
#     'timeout': 1000,
#     'options': {
#         'defaultType': 'swap',
#     }
# })

# semaphore = asyncio.Semaphore(5)

# # Глобальный рейт-лимит: ждём между каждым вызовом API
# GLOBAL_RATE_LIMIT_MS = bybit.rateLimit * 2
# print(f"Установлен глобальный рейт-лимит: {GLOBAL_RATE_LIMIT_MS} мс")

# last_request_time = time.time()


# async def wait_for_rate_limit():
#     global last_request_time
#     elapsed = (time.time() - last_request_time) * 1000
#     if elapsed < GLOBAL_RATE_LIMIT_MS:
#         delay = (GLOBAL_RATE_LIMIT_MS - elapsed) / 1000
#         await asyncio.sleep(delay)
#     last_request_time = time.time()


# async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 200):
#     all_history = []
#     current_since = start_time_ms
#     max_iterations = 20
#     iteration_count = 0

#     while iteration_count < max_iterations:
#         await wait_for_rate_limit()
#         try:
#             partial_history = await bybit.fetch_funding_rate_history(
#                 symbol=symbol,
#                 since=current_since,
#                 limit=limit
#             )

#             if not partial_history:
#                 break

#             all_history.extend(partial_history)
#             latest_ts = max(entry['timestamp'] for entry in partial_history)

#             if latest_ts >= end_time_ms:
#                 break

#             current_since = latest_ts + 1
#             iteration_count += 1

#         except Exception as e:
#             print(f"Ошибка при частичном запросе истории FR для {symbol} (since {current_since}): {e}")
#             break

#     return all_history


# async def detect_funding_interval(history):
#     if len(history) < 2:
#         return None

#     history = sorted(history, key=lambda x: x['timestamp'])
#     intervals_ms = []
#     for i in range(1, len(history)):
#         diff = history[i]['timestamp'] - history[i - 1]['timestamp']
#         intervals_ms.append(diff)

#     counter = Counter(intervals_ms)
#     most_common_ms, _ = counter.most_common(1)[0]
#     hours = round(most_common_ms / (1000 * 3600))
#     return hours if hours > 0 else None


# async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict, 
#                          min_volume_usd: float = 1000, orderbook_depth: int = 20):
#     async with semaphore:
#         try:
#             await wait_for_rate_limit()

#             # Получаем стакан с заданной глубиной (по умолчанию 20 уровней)
#             order_book = await bybit.fetch_order_book(symbol, limit=orderbook_depth)
#             bids = order_book['bids'][:orderbook_depth]
#             asks = order_book['asks'][:orderbook_depth]

#             # Расчет объема в USD (цена * количество монет)
#             ask_total_usd = sum(price * volume for price, volume in asks)
#             bid_total_usd = sum(price * volume for price, volume in bids)
            
#             # Расчет среднего объема
#             avg_volume_usd = (ask_total_usd + bid_total_usd) / 2
            
#             # Также считаем количество монет для отладки
#             ask_total_coins = sum(volume for price, volume in asks)
#             bid_total_coins = sum(volume for price, volume in bids)
            
#             # Фильтр по СРЕДНЕМУ объему в USD
#             is_liquid = avg_volume_usd > min_volume_usd
            
#             # Для отладки проблемных монет
#             debug_symbols = ["LAB", "ZRO", "ZRX", "BTC", "ETH"]
#             if any(debug in symbol for debug in debug_symbols):
#                 print(f"\n🔍 {symbol}:")
#                 print(f"   Ask объем: ${ask_total_usd:.2f} (монет: {ask_total_coins:.2f})")
#                 print(f"   Bid объем: ${bid_total_usd:.2f} (монет: {bid_total_coins:.2f})")
#                 print(f"   Средний объем: ${avg_volume_usd:.2f}")
#                 print(f"   Фильтр (средний > {min_volume_usd}$): {'✅ ПРОХОДИТ' if is_liquid else '❌ НЕ ПРОХОДИТ'}")
            
#             if not is_liquid:
#                 # Если не прошли фильтр - сохраняем базовую информацию
#                 results[symbol] = {
#                     "24h": 0,
#                     "48h": 0,
#                     "168h": 0,
#                     "720h": 0,
#                     "currentFR": None,
#                     "fundingIntervalHours": None,
#                     "nextFundingTime": None,
#                     "askTotalVolume": round(ask_total_usd, 2),
#                     "bidTotalVolume": round(bid_total_usd, 2),
#                     "avgVolumeUSD": round(avg_volume_usd, 2),
#                     "askTotalCoins": round(ask_total_coins, 2),
#                     "bidTotalCoins": round(bid_total_coins, 2),
#                     "is_liquid": False,
#                     "orderbook_depth": orderbook_depth,
#                     "filter_reason": f"AvgVolume={avg_volume_usd:.1f}$ (min {min_volume_usd}$)"
#                 }
#                 print(f"⚠️ [FILTERED] {symbol}: Средний объем=${avg_volume_usd:.1f}$ (нужно >{min_volume_usd}$)")
#                 return
            
#             # Если прошли фильтр - собираем полные данные
#             print(f"✅ [LIQUID] {symbol}: Средний объем=${avg_volume_usd:.0f}$ (Ask=${ask_total_usd:.0f}, Bid=${bid_total_usd:.0f})")
            
#             # Текущий funding rate
#             current_funding = None
#             next_funding_time_str = None
#             try:
#                 fr_data = await bybit.fetch_funding_rate(symbol)
#                 current_funding = fr_data.get('fundingRate')
#                 next_ts = fr_data.get('nextFundingTimestamp')
#                 if next_ts:
#                     next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
#                 if current_funding is not None:
#                     current_funding *= 100
#             except Exception as e:
#                 print(f"Ошибка текущего FR для {symbol}: {e}")

#             # Сбор истории за 720 часов (30 дней)
#             start_time_ms_30d = int((now - timedelta(hours=720)).timestamp() * 1000)
#             end_time_ms = int(now.timestamp() * 1000)

#             await wait_for_rate_limit()

#             try:
#                 full_funding_history = await fetch_full_funding_history(
#                     symbol=symbol,
#                     start_time_ms=start_time_ms_30d,
#                     end_time_ms=end_time_ms,
#                     limit=200
#                 )
#             except Exception as e:
#                 print(f"Ошибка получения полной истории FR для {symbol}: {e}")
#                 full_funding_history = []

#             # Сортировка и фильтрация по периодам
#             full_funding_history.sort(key=lambda x: x['timestamp'])

#             total_24h = total_48h = total_168h = total_720h = 0.0

#             for entry in full_funding_history:
#                 ts = entry['timestamp']
#                 rate = entry['fundingRate'] * 100

#                 if timestamps["24h"] < ts < end_time_ms:
#                     total_24h += rate
#                 if timestamps["48h"] < ts < end_time_ms:
#                     total_48h += rate
#                 if timestamps["168h"] < ts < end_time_ms:
#                     total_168h += rate
#                 if timestamps["720h"] < ts < end_time_ms:
#                     total_720h += rate

#             funding_interval_hours = await detect_funding_interval(full_funding_history)

#             # Сохраняем полные данные
#             results[symbol] = {
#                 "24h": round(total_24h, 6),
#                 "48h": round(total_48h, 6),
#                 "168h": round(total_168h, 6),
#                 "720h": round(total_720h, 6),
#                 "currentFR": round(current_funding, 6) if current_funding is not None else None,
#                 "fundingIntervalHours": funding_interval_hours if funding_interval_hours is not None else 8,
#                 "nextFundingTime": next_funding_time_str,
#                 "askTotalVolume": round(ask_total_usd, 2),
#                 "bidTotalVolume": round(bid_total_usd, 2),
#                 "avgVolumeUSD": round(avg_volume_usd, 2),
#                 "askTotalCoins": round(ask_total_coins, 2),
#                 "bidTotalCoins": round(bid_total_coins, 2),
#                 "is_liquid": True,
#                 "orderbook_depth": orderbook_depth,
#                 "total_records": len(full_funding_history)
#             }

#             # Выводим статус с фандингом
#             print(f"   → 30д={total_720h:.4f}%, 7д={total_168h:.4f}%, записей={len(full_funding_history)}")

#         except Exception as e:
#             print(f"❌ Ошибка при обработке {symbol} на Bybit: {e}")
#             results[symbol] = {
#                 "24h": 0,
#                 "48h": 0,
#                 "168h": 0,
#                 "720h": 0,
#                 "currentFR": None,
#                 "fundingIntervalHours": None,
#                 "nextFundingTime": None,
#                 "askTotalVolume": 0,
#                 "bidTotalVolume": 0,
#                 "is_liquid": False,
#                 "error": str(e)
#             }


# async def main():
#     now = datetime.now()
#     timestamps = {
#         "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
#         "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
#         "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
#         "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
#     }

#     input_file = DATA_DIR / "tradePairsBybite.json"
#     with open(input_file, "r", encoding="utf-8") as f:
#         symbols = json.load(f)

#     # НАСТРОЙКИ ФИЛЬТРАЦИИ
#     MIN_VOLUME_USD = 1000      # Минимальный средний объем в USD (было 500, стало 1000)
#     ORDERBOOK_DEPTH = 20       # Глубина стакана (было 50, стало 20)

#     print(f"\n{'='*60}")
#     print(f"📊 Начинаем сбор данных для {len(symbols)} символов")
#     print(f"📌 Метод фильтрации: СРЕДНИЙ объем (Ask+Bid)/2")
#     print(f"📌 Минимальный средний объем: {MIN_VOLUME_USD}$")
#     print(f"📌 Глубина стакана: {ORDERBOOK_DEPTH} уровней")
#     print(f"{'='*60}\n")

#     results = {}
#     tasks = [process_symbol(symbol, timestamps, now, results, MIN_VOLUME_USD, ORDERBOOK_DEPTH) 
#              for symbol in symbols]
#     await asyncio.gather(*tasks)

#     output_file = DATA_DIR / "funding_results_bybite.json"
#     try:
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(results, f, indent=4, ensure_ascii=False)
        
#         # Статистика
#         total = len(results)
#         liquid = sum(1 for r in results.values() if r.get('is_liquid', False))
#         filtered_out = [(sym, r) for sym, r in results.items() if not r.get('is_liquid', False)]
        
#         print(f"\n{'='*60}")
#         print(f"✅ Результаты Bybit сохранены в: {output_file}")
#         print(f"📊 СТАТИСТИКА:")
#         print(f"   Всего символов в файле: {total}")
#         print(f"   Ликвидных (средний объем >{MIN_VOLUME_USD}$): {liquid} ({liquid/total*100:.1f}%)")
#         print(f"   Отфильтровано (средний объем <{MIN_VOLUME_USD}$): {len(filtered_out)} ({len(filtered_out)/total*100:.1f}%)")
        
#         # Топ-10 отфильтрованных по среднему объему
#         if filtered_out:
#             print(f"\n📋 ТОП-10 СИМВОЛОВ, ОТФИЛЬТРОВАННЫХ ПО СРЕДНЕМУ ОБЪЕМУ:")
#             filtered_sorted = sorted(filtered_out, key=lambda x: x[1].get('avgVolumeUSD', 0), reverse=True)[:10]
#             for sym, data in filtered_sorted:
#                 avg_vol = data.get('avgVolumeUSD', 0)
#                 ask_vol = data.get('askTotalVolume', 0)
#                 bid_vol = data.get('bidTotalVolume', 0)
#                 print(f"   {sym:<20} средний=${avg_vol:.1f}$ (Ask=${ask_vol:.1f}, Bid=${bid_vol:.1f})")
        
#         # Проверяем конкретные символы
#         check_symbols = ["LAB/USDT:USDT", "ZRO/USDT:USDT", "ZRX/USDT:USDT"]
#         print(f"\n🎯 РЕЗУЛЬТАТЫ ДЛЯ ПРОВЕРЯЕМЫХ СИМВОЛОВ:")
#         for check_sym in check_symbols:
#             if check_sym in results:
#                 data = results[check_sym]
#                 print(f"   {check_sym}:")
#                 print(f"      Средний объем: ${data.get('avgVolumeUSD', 0):.2f}")
#                 print(f"      Ask: ${data.get('askTotalVolume', 0):.2f}, Bid: ${data.get('bidTotalVolume', 0):.2f}")
#                 print(f"      Прошел фильтр: {'✅ ДА' if data.get('is_liquid', False) else '❌ НЕТ'}")
        
#         print(f"{'='*60}")
        
#     except Exception as e:
#         print(f"Ошибка сохранения: {e}")

#     await bybit.close()


# if __name__ == "__main__":
#     asyncio.run(main())

import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time
from pathlib import Path

# Use relative path based on current file location
DATA_DIR = Path(__file__).parent

bybit = ccxt.bybit({
    'timeout': 1000,
    'options': {
        'defaultType': 'swap',
    }
})

semaphore = asyncio.Semaphore(5)

# Глобальный рейт-лимит
GLOBAL_RATE_LIMIT_MS = bybit.rateLimit * 2
print(f"Установлен глобальный рейт-лимит: {GLOBAL_RATE_LIMIT_MS} мс")

last_request_time = time.time()


async def wait_for_rate_limit():
    global last_request_time
    elapsed = (time.time() - last_request_time) * 1000
    if elapsed < GLOBAL_RATE_LIMIT_MS:
        delay = (GLOBAL_RATE_LIMIT_MS - elapsed) / 1000
        await asyncio.sleep(delay)
    last_request_time = time.time()


async def fetch_24h_volume(symbol: str) -> float:
    """
    Получает объем торгов за последние 24 часа в USD
    Использует метод fetch_ticker, который возвращает quoteVolume
    """
    try:
        ticker = await bybit.fetch_ticker(symbol)
        # quoteVolume - объем в USDT (котируемой валюте)
        volume_24h_usd = ticker.get('quoteVolume', 0)
        return volume_24h_usd
    except Exception as e:
        print(f"Ошибка получения ticker для {symbol}: {e}")
        return 0


async def fetch_recent_trades_volume(symbol: str, hours_back: int = 24) -> float:
    """
    Альтернативный метод: получает последние сделки и суммирует объем
    Более точный, но требует больше запросов
    """
    try:
        # Получаем timestamp для hours_back часов назад
        since = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        # Получаем последние сделки (до 1000 штук)
        trades = await bybit.fetch_trades(symbol, since=since, limit=1000)
        
        # Суммируем объем в USD
        total_volume_usd = sum(trade['cost'] for trade in trades)
        return total_volume_usd
    except Exception as e:
        print(f"Ошибка получения сделок для {symbol}: {e}")
        return 0


async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 200):
    all_history = []
    current_since = start_time_ms
    max_iterations = 20
    iteration_count = 0

    while iteration_count < max_iterations:
        await wait_for_rate_limit()
        try:
            partial_history = await bybit.fetch_funding_rate_history(
                symbol=symbol,
                since=current_since,
                limit=limit
            )

            if not partial_history:
                break

            all_history.extend(partial_history)
            latest_ts = max(entry['timestamp'] for entry in partial_history)

            if latest_ts >= end_time_ms:
                break

            current_since = latest_ts + 1
            iteration_count += 1

        except Exception as e:
            print(f"Ошибка при частичном запросе истории FR для {symbol}: {e}")
            break

    return all_history


async def detect_funding_interval(history):
    if len(history) < 2:
        return None

    history = sorted(history, key=lambda x: x['timestamp'])
    intervals_ms = []
    for i in range(1, len(history)):
        diff = history[i]['timestamp'] - history[i - 1]['timestamp']
        intervals_ms.append(diff)

    counter = Counter(intervals_ms)
    most_common_ms, _ = counter.most_common(1)[0]
    hours = round(most_common_ms / (1000 * 3600))
    return hours if hours > 0 else None


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict, 
                         min_volume_usd: float = 50000,  # Минимальный объем торгов за 24ч в USD
                         use_ticker: bool = True):      # True = ticker, False = сделки
    async with semaphore:
        try:
            await wait_for_rate_limit()

            # ========== НОВАЯ ФИЛЬТРАЦИЯ ПО ОБЪЕМУ ТОРГОВ ==========
            if use_ticker:
                # Способ 1: через ticker (быстро, 1 запрос)
                volume_24h_usd = await fetch_24h_volume(symbol)
            else:
                # Способ 2: через сделки (медленнее, но точнее)
                volume_24h_usd = await fetch_recent_trades_volume(symbol, hours_back=24)
            
            # Проверяем объем торгов за 24 часа
            is_liquid = volume_24h_usd > min_volume_usd
            
            # Для отладки проблемных монет
            debug_symbols = ["TAG", "LAB", "ZRO", "ZRX", "BTC", "ETH"]
            if any(debug in symbol for debug in debug_symbols):
                print(f"\n🔍 {symbol}:")
                print(f"   Объем торгов за 24ч: ${volume_24h_usd:,.2f}")
                print(f"   Фильтр (объем > {min_volume_usd:,.0f}$): {'✅ ПРОХОДИТ' if is_liquid else '❌ НЕ ПРОХОДИТ'}")
            
            if not is_liquid:
                results[symbol] = {
                    "24h": 0,
                    "48h": 0,
                    "168h": 0,
                    "720h": 0,
                    "currentFR": None,
                    "fundingIntervalHours": None,
                    "nextFundingTime": None,
                    "volume24hUSD": round(volume_24h_usd, 2),
                    "is_liquid": False,
                    "filter_reason": f"Volume24h={volume_24h_usd:,.0f}$ (min {min_volume_usd:,.0f}$)"
                }
                print(f"⚠️ [FILTERED] {symbol}: Объем за 24ч=${volume_24h_usd:,.0f}$ (нужно >{min_volume_usd:,.0f}$)")
                return
            
            print(f"✅ [LIQUID] {symbol}: Объем за 24ч=${volume_24h_usd:,.0f}$")
            
            # ========== ДАЛЬШЕ СТАНДАРТНЫЙ СБОР ДАННЫХ ==========
            
            # Текущий funding rate
            current_funding = None
            next_funding_time_str = None
            try:
                fr_data = await bybit.fetch_funding_rate(symbol)
                current_funding = fr_data.get('fundingRate')
                next_ts = fr_data.get('nextFundingTimestamp')
                if next_ts:
                    next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                if current_funding is not None:
                    current_funding *= 100
            except Exception as e:
                print(f"Ошибка текущего FR для {symbol}: {e}")

            # Сбор истории за 720 часов (30 дней)
            start_time_ms_30d = int((now - timedelta(hours=720)).timestamp() * 1000)
            end_time_ms = int(now.timestamp() * 1000)

            await wait_for_rate_limit()

            try:
                full_funding_history = await fetch_full_funding_history(
                    symbol=symbol,
                    start_time_ms=start_time_ms_30d,
                    end_time_ms=end_time_ms,
                    limit=200
                )
            except Exception as e:
                print(f"Ошибка получения полной истории FR для {symbol}: {e}")
                full_funding_history = []

            # Сортировка и фильтрация по периодам
            full_funding_history.sort(key=lambda x: x['timestamp'])

            total_24h = total_48h = total_168h = total_720h = 0.0

            for entry in full_funding_history:
                ts = entry['timestamp']
                rate = entry['fundingRate'] * 100

                if timestamps["24h"] < ts < end_time_ms:
                    total_24h += rate
                if timestamps["48h"] < ts < end_time_ms:
                    total_48h += rate
                if timestamps["168h"] < ts < end_time_ms:
                    total_168h += rate
                if timestamps["720h"] < ts < end_time_ms:
                    total_720h += rate

            funding_interval_hours = await detect_funding_interval(full_funding_history)

            # Сохраняем полные данные
            results[symbol] = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": round(current_funding, 6) if current_funding is not None else None,
                "fundingIntervalHours": funding_interval_hours if funding_interval_hours is not None else 8,
                "nextFundingTime": next_funding_time_str,
                "volume24hUSD": round(volume_24h_usd, 2),
                "is_liquid": True,
                "total_records": len(full_funding_history)
            }

            print(f"   → 30д={total_720h:.4f}%, 7д={total_168h:.4f}%, записей={len(full_funding_history)}")

        except Exception as e:
            print(f"❌ Ошибка при обработке {symbol} на Bybit: {e}")
            results[symbol] = {
                "24h": 0,
                "48h": 0,
                "168h": 0,
                "720h": 0,
                "currentFR": None,
                "fundingIntervalHours": None,
                "nextFundingTime": None,
                "volume24hUSD": 0,
                "is_liquid": False,
                "error": str(e)
            }


async def main():
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
    }

    input_file = DATA_DIR / "tradePairsBybite.json"
    with open(input_file, "r", encoding="utf-8") as f:
        symbols = json.load(f)

    # НАСТРОЙКИ ФИЛЬТРАЦИИ (НОВЫЕ!)
    MIN_VOLUME_24H_USD = 50000   # Минимальный объем торгов за 24 часа в USD (50k$)
    USE_TICKER = True            # True = быстрый метод (ticker), False = точный (сделки)

    print(f"\n{'='*60}")
    print(f"📊 Начинаем сбор данных для {len(symbols)} символов")
    print(f"📌 МЕТОД ФИЛЬТРАЦИИ: Объем торгов за 24 часа")
    print(f"📌 Минимальный объем за 24ч: ${MIN_VOLUME_24H_USD:,.0f}")
    print(f"📌 Метод получения объема: {'Ticker (быстрый)' if USE_TICKER else 'Trades (точный)'}")
    print(f"{'='*60}\n")

    results = {}
    tasks = [process_symbol(symbol, timestamps, now, results, MIN_VOLUME_24H_USD, USE_TICKER) 
             for symbol in symbols]
    await asyncio.gather(*tasks)

    output_file = DATA_DIR / "funding_results_bybite.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        
        # Статистика
        total = len(results)
        liquid = sum(1 for r in results.values() if r.get('is_liquid', False))
        
        # Топ-10 по объему торгов
        sorted_by_volume = sorted([(sym, r) for sym, r in results.items()], 
                                  key=lambda x: x[1].get('volume24hUSD', 0), reverse=True)[:10]
        
        print(f"\n{'='*60}")
        print(f"✅ Результаты Bybit сохранены в: {output_file}")
        print(f"📊 СТАТИСТИКА:")
        print(f"   Всего символов в файле: {total}")
        print(f"   Ликвидных (объем >${MIN_VOLUME_24H_USD:,.0f}): {liquid} ({liquid/total*100:.1f}%)")
        print(f"   Неликвидных (объем <${MIN_VOLUME_24H_USD:,.0f}): {total - liquid} ({(total-liquid)/total*100:.1f}%)")
        
        print(f"\n🏆 ТОП-10 ПО ОБЪЕМУ ТОРГОВ ЗА 24 ЧАСА:")
        for sym, data in sorted_by_volume:
            volume = data.get('volume24hUSD', 0)
            is_liq = data.get('is_liquid', False)
            status = "✅" if is_liq else "⚠️"
            print(f"   {status} {sym:<25} ${volume:>15,.0f}")
        
        # Проверяем 1000TAGUSDT
        tag_key = "1000TAG/USDT:USDT"
        if tag_key in results:
            tag_data = results[tag_key]
            print(f"\n🎯 РЕЗУЛЬТАТ ДЛЯ 1000TAG/USDT:USDT:")
            print(f"   Объем за 24ч: ${tag_data.get('volume24hUSD', 0):,.2f}")
            print(f"   Прошел фильтр: {'✅ ДА' if tag_data.get('is_liquid', False) else '❌ НЕТ'}")
        else:
            # Ищем похожие символы с TAG
            tag_similar = [s for s in results.keys() if 'TAG' in s]
            if tag_similar:
                print(f"\n🎯 ПОХОЖИЕ СИМВОЛЫ С TAG:")
                for sym in tag_similar:
                    data = results[sym]
                    print(f"   {sym}: объем=${data.get('volume24hUSD', 0):,.2f}")
        
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

    await bybit.close()


if __name__ == "__main__":
    asyncio.run(main())