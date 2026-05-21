# fetch_htx_funding.py
import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time
from pathlib import Path

DATA_DIR = Path(__file__).parent

htx = ccxt.htx({
    'timeout': 60000,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
    },
})

semaphore = asyncio.Semaphore(5)

GLOBAL_RATE_LIMIT_MS = htx.rateLimit * 2
print(f"Установлен глобальный рейт-лимит: {GLOBAL_RATE_LIMIT_MS} мс")

last_request_time = time.time()


async def wait_for_rate_limit():
    global last_request_time
    elapsed = (time.time() - last_request_time) * 1000
    if elapsed < GLOBAL_RATE_LIMIT_MS:
        delay = (GLOBAL_RATE_LIMIT_MS - elapsed) / 1000
        await asyncio.sleep(delay)
    last_request_time = time.time()


async def analyze_trades_activity(symbol: str, hours_back: int = 1):
    """Для HTX публичные сделки недоступны без авторизации, возвращаем нули"""
    return {
        'total_volume_usd': 0,
        'trade_count': 0,
        'avg_trade_size': 0,
        'time_since_last_trade': None,
        'trades_per_hour': 0,
        'is_active': False
    }


async def fetch_ticker_info(symbol: str):
    """Получает объём 24ч из тикера swap-рынка"""
    try:
        # Явно указываем тип swap
        ticker = await htx.fetch_ticker(symbol, params={'type': 'swap'})
        volume_24h_usd = ticker.get('quoteVolume', 0)
        if volume_24h_usd is None:
            volume_24h_usd = 0
        return {
            'volume_24h_usd': float(volume_24h_usd),
            'high_24h': ticker.get('high', 0) or 0,
            'low_24h': ticker.get('low', 0) or 0,
            'change_24h': ticker.get('change', 0) or 0,
            'percentage_24h': ticker.get('percentage', 0) or 0
        }
    except Exception as e:
        print(f"Ошибка получения ticker для {symbol}: {e}")
        return {
            'volume_24h_usd': 0,
            'high_24h': 0,
            'low_24h': 0,
            'change_24h': 0,
            'percentage_24h': 0
        }


async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 200):
    all_history = []
    current_since = start_time_ms
    max_iterations = 20
    iteration_count = 0
    while iteration_count < max_iterations:
        await wait_for_rate_limit()
        try:
            partial_history = await htx.fetch_funding_rate_history(
                symbol=symbol,
                since=current_since,
                limit=limit,
                params={'type': 'swap'}  # явно указываем swap
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
                         min_volume_24h_usd: float = 500000,
                         min_trade_count_per_hour: int = 0,
                         max_time_since_last_trade_seconds: int = 0,
                         min_avg_trade_size_usd: float = 0):
    async with semaphore:
        try:
            await wait_for_rate_limit()

            ticker_info = await fetch_ticker_info(symbol)
            volume_24h_usd = ticker_info['volume_24h_usd']

            trades_info = await analyze_trades_activity(symbol, hours_back=1)

            filters_passed = {
                'min_volume_24h': volume_24h_usd >= min_volume_24h_usd,
                'min_trade_count': True,
                'recent_trades': True,
                'min_avg_trade_size': True
            }
            is_liquid = filters_passed['min_volume_24h']

            print(f"\n🔍 {symbol}:")
            print(f"   📊 Объем 24ч: ${volume_24h_usd:,.2f} (нужно >{min_volume_24h_usd:,.0f}$) {'✅' if filters_passed['min_volume_24h'] else '❌'}")
            print(f"   📈 Сделок/час: {trades_info['trade_count']} (фильтр отключён)")
            print(f"   ⏱️ Последняя сделка: Н/Д (фильтр отключён)")
            print(f"   💰 Средний чек: ${trades_info['avg_trade_size']:,.2f} (фильтр отключён)")

            if not is_liquid:
                print(f"   ❌ НЕ ПРОХОДИТ ФИЛЬТР - монета НЕ будет сохранена")
                return

            print(f"   ✅ ПРОХОДИТ ФИЛЬТР - собираем данные по фандингу")

            # Текущий фандинг
            current_funding = None
            next_funding_time_str = None
            try:
                fr_data = await htx.fetch_funding_rate(symbol, params={'type': 'swap'})
                current_funding = fr_data.get('fundingRate')
                next_ts = fr_data.get('nextFundingTimestamp')
                if next_ts:
                    next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                if current_funding is not None:
                    current_funding *= 100
            except Exception as e:
                print(f"   Ошибка текущего FR: {e}")

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
                print(f"   Ошибка получения истории FR: {e}")
                full_funding_history = []

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

            results[symbol] = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": round(current_funding, 6) if current_funding is not None else None,
                "fundingIntervalHours": funding_interval_hours if funding_interval_hours is not None else 8,
                "nextFundingTime": next_funding_time_str,
                "volume24hUSD": round(volume_24h_usd, 2),
                "tradeCountLastHour": 0,
                "avgTradeSizeUSD": 0,
                "timeSinceLastTradeSeconds": None,
                "tradesPerHour": 0,
                "total_records": len(full_funding_history)
            }
            print(f"   → 30д={total_720h:.4f}%, 7д={total_168h:.4f}%, записей={len(full_funding_history)}")

        except Exception as e:
            print(f"❌ Ошибка при обработке {symbol} на HTX: {e}")


async def main():
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
    }

    input_file = DATA_DIR / "tradePairsHtx.json"
    with open(input_file, "r", encoding="utf-8") as f:
        symbols = json.load(f)

    FILTERS = {
        'min_volume_24h_usd': 500000,
        'min_trade_count_per_hour': 0,
        'max_time_since_last_trade_seconds': 0,
        'min_avg_trade_size_usd': 0
    }

    print(f"\n{'='*80}")
    print(f"📊 Начинаем сбор данных для {len(symbols)} символов (HTX)")
    print(f"{'='*80}")
    print(f"📌 ФИЛЬТР ЛИКВИДНОСТИ:")
    print(f"   • Объем торгов за 24ч: > ${FILTERS['min_volume_24h_usd']:,.0f}")
    print(f"   • Фильтры по сделкам ОТКЛЮЧЕНЫ (публичные сделки недоступны)")
    print(f"{'='*80}")
    print(f"⚠️ ВНИМАНИЕ: В файл попадут ТОЛЬКО монеты, прошедшие ВСЕ фильтры!")
    print(f"{'='*80}\n")

    results = {}
    tasks = [process_symbol(symbol, timestamps, now, results,
                           FILTERS['min_volume_24h_usd'],
                           FILTERS['min_trade_count_per_hour'],
                           FILTERS['max_time_since_last_trade_seconds'],
                           FILTERS['min_avg_trade_size_usd'])
             for symbol in symbols]
    await asyncio.gather(*tasks)

    output_file = DATA_DIR / "funding_results_htx.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        total_checked = len(symbols)
        liquid_count = len(results)

        print(f"\n{'='*80}")
        print(f"✅ Результаты HTX сохранены в: {output_file}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   Проверено монет: {total_checked}")
        print(f"   Прошли фильтры (объём >500k$): {liquid_count} ({liquid_count/total_checked*100:.1f}%)")
        print(f"   Отфильтровано: {total_checked - liquid_count} ({(total_checked-liquid_count)/total_checked*100:.1f}%)")
        print(f"{'='*80}")

    except Exception as e:
        print(f"Ошибка сохранения: {e}")

    await htx.close()


if __name__ == "__main__":
    asyncio.run(main())