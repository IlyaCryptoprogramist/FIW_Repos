# fetch_mexc_funding.py (оптимизированная версия)
import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time
from pathlib import Path

DATA_DIR = Path(__file__).parent

mexc = ccxt.mexc({
    'timeout': 10000,
    'enableRateLimit': True,      # встроенный рейт-лимит
    'options': {
        'defaultType': 'swap',
    }
})

semaphore = asyncio.Semaphore(20)   # до 20 параллельных задач


async def fetch_all_tickers():
    """Пытается получить все тикеры одним запросом. Если не поддерживается – возвращает None."""
    try:
        tickers = await mexc.fetch_tickers()
        return {symbol: ticker for symbol, ticker in tickers.items()}
    except Exception as e:
        print(f"⚠️ MEXC не поддерживает fetch_tickers (или ошибка): {e}")
        return None


async def analyze_trades_activity(symbol: str, hours_back: int = 1):
    """Анализирует активность по сделкам за последний час."""
    try:
        since = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        trades = await mexc.fetch_trades(symbol, since=since, limit=1000)
        if not trades:
            return {
                'total_volume_usd': 0,
                'trade_count': 0,
                'avg_trade_size': 0,
                'time_since_last_trade': None,
                'trades_per_hour': 0,
                'is_active': False
            }
        total_volume_usd = sum(trade.get('cost', 0) for trade in trades)
        trade_count = len(trades)
        avg_trade_size = total_volume_usd / trade_count if trade_count > 0 else 0
        latest_trade_time = max(trade['timestamp'] for trade in trades)
        time_since_last_trade = (datetime.now().timestamp() * 1000 - latest_trade_time) / 1000
        trades_per_hour = trade_count / hours_back
        return {
            'total_volume_usd': total_volume_usd,
            'trade_count': trade_count,
            'avg_trade_size': avg_trade_size,
            'time_since_last_trade': time_since_last_trade,
            'trades_per_hour': trades_per_hour,
            'is_active': True
        }
    except Exception as e:
        print(f"⚠️ Ошибка анализа сделок для {symbol}: {e}")
        return {
            'total_volume_usd': 0,
            'trade_count': 0,
            'avg_trade_size': 0,
            'time_since_last_trade': None,
            'trades_per_hour': 0,
            'is_active': False
        }


async def fetch_ticker_info(symbol: str):
    """Получает ticker для одного символа (используется, если fetch_all_tickers не работает)."""
    try:
        ticker = await mexc.fetch_ticker(symbol)
        return {
            'volume_24h_usd': ticker.get('quoteVolume', 0),
            'high_24h': ticker.get('high', 0),
            'low_24h': ticker.get('low', 0),
            'change_24h': ticker.get('change', 0),
            'percentage_24h': ticker.get('percentage', 0)
        }
    except Exception as e:
        print(f"⚠️ Ошибка получения ticker для {symbol}: {e}")
        return {
            'volume_24h_usd': 0,
            'high_24h': 0,
            'low_24h': 0,
            'change_24h': 0,
            'percentage_24h': 0
        }


async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 500):
    """Собирает всю историю фандинга (максимальными порциями)."""
    all_history = []
    current_since = start_time_ms
    max_iterations = 20
    iteration = 0

    while iteration < max_iterations:
        try:
            partial = await mexc.fetch_funding_rate_history(
                symbol=symbol,
                since=current_since,
                limit=limit
            )
            if not partial:
                break
            all_history.extend(partial)
            latest_ts = max(entry['timestamp'] for entry in partial)
            if latest_ts >= end_time_ms:
                break
            current_since = latest_ts + 1
            iteration += 1
        except Exception as e:
            print(f"❌ Ошибка при запросе истории FR для {symbol}: {e}")
            break
    return all_history


async def detect_funding_interval(history):
    if len(history) < 2:
        return None
    sorted_history = sorted(history, key=lambda x: x['timestamp'])
    intervals = [sorted_history[i]['timestamp'] - sorted_history[i-1]['timestamp']
                 for i in range(1, len(sorted_history))]
    if not intervals:
        return None
    counter = Counter(intervals)
    most_common = counter.most_common(1)[0][0]
    hours = round(most_common / (1000 * 3600))
    return hours if 1 <= hours <= 24 else 8


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict,
                         tickers_map: dict,
                         min_volume_24h_usd: float,
                         min_trade_count_per_hour: int,
                         max_time_since_last_trade_seconds: int,
                         min_avg_trade_size_usd: float):
    async with semaphore:
        try:
            # Объём из тикера
            if tickers_map is not None:
                ticker = tickers_map.get(symbol, {})
                volume_24h_usd = ticker.get('quoteVolume', 0)
            else:
                ticker_info = await fetch_ticker_info(symbol)
                volume_24h_usd = ticker_info['volume_24h_usd']

            # Анализ сделок
            trades_info = await analyze_trades_activity(symbol, hours_back=1)

            filters = {
                'min_volume_24h': volume_24h_usd >= min_volume_24h_usd,
                'min_trade_count': trades_info['trade_count'] >= min_trade_count_per_hour,
                'recent_trades': trades_info.get('time_since_last_trade') is not None and
                                 trades_info['time_since_last_trade'] <= max_time_since_last_trade_seconds,
                'min_avg_trade_size': trades_info['avg_trade_size'] >= min_avg_trade_size_usd
            }
            is_liquid = all(filters.values())

            print(f"\n🔍 {symbol}:")
            print(f"   📊 Объём 24ч: ${volume_24h_usd:,.2f} (нужно >{min_volume_24h_usd:,.0f}$) {'✅' if filters['min_volume_24h'] else '❌'}")
            print(f"   📈 Сделок/час: {trades_info['trade_count']} (нужно >{min_trade_count_per_hour}) {'✅' if filters['min_trade_count'] else '❌'}")
            if trades_info['time_since_last_trade']:
                print(f"   ⏱️ Посл. сделка: {trades_info['time_since_last_trade']:.1f} сек (нужно <{max_time_since_last_trade_seconds} сек) {'✅' if filters['recent_trades'] else '❌'}")
            else:
                print(f"   ⏱️ Посл. сделка: НЕТ СДЕЛОК ❌")
            print(f"   💰 Средний чек: ${trades_info['avg_trade_size']:,.2f} (нужно >{min_avg_trade_size_usd:,.0f}$) {'✅' if filters['min_avg_trade_size'] else '❌'}")

            if not is_liquid:
                print(f"   ❌ НЕ ПРОХОДИТ ФИЛЬТР – монета НЕ будет сохранена")
                return

            print(f"   ✅ ПРОХОДИТ ФИЛЬТР – собираем данные по фандингу")

            # Текущий фандинг
            current_funding = None
            next_funding_time_str = None
            try:
                fr_data = await mexc.fetch_funding_rate(symbol)
                current_funding = fr_data.get('fundingRate')
                next_ts = fr_data.get('nextFundingTimestamp')
                if next_ts:
                    next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                if current_funding is not None:
                    current_funding *= 100
            except Exception as e:
                print(f"   ⚠️ Ошибка текущего FR: {e}")

            # История за 30 дней
            start_time_ms_30d = int((now - timedelta(hours=720)).timestamp() * 1000)
            end_time_ms = int(now.timestamp() * 1000)

            full_history = await fetch_full_funding_history(symbol, start_time_ms_30d, end_time_ms, limit=500)

            if not full_history:
                print(f"   ⚠️ Нет истории фандинга за 30 дней")
                return

            full_history.sort(key=lambda x: x['timestamp'])

            total_24h = total_48h = total_168h = total_720h = 0.0
            for entry in full_history:
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

            interval_hours = await detect_funding_interval(full_history)

            results[symbol] = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": round(current_funding, 6) if current_funding is not None else None,
                "fundingIntervalHours": interval_hours if interval_hours is not None else 8,
                "nextFundingTime": next_funding_time_str,
                "volume24hUSD": round(volume_24h_usd, 2),
                "tradeCountLastHour": trades_info['trade_count'],
                "avgTradeSizeUSD": round(trades_info['avg_trade_size'], 2),
                "timeSinceLastTradeSeconds": round(trades_info['time_since_last_trade'], 1) if trades_info['time_since_last_trade'] else None,
                "tradesPerHour": round(trades_info['trades_per_hour'], 2),
                "total_records": len(full_history)
            }
            print(f"   → 30д={total_720h:.4f}%, 7д={total_168h:.4f}%, записей={len(full_history)}")

        except Exception as e:
            print(f"❌ Ошибка при обработке {symbol} на MEXC: {e}")


async def main():
    start_time = time.time()
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
    }

    input_file = DATA_DIR / "tradePairsMexc.json"
    with open(input_file, "r", encoding="utf-8") as f:
        symbols = json.load(f)

    FILTERS = {
        'min_volume_24h_usd': 500000,
        'min_trade_count_per_hour': 100,
        'max_time_since_last_trade_seconds': 25,
        'min_avg_trade_size_usd': 10
    }

    print(f"\n{'='*80}")
    print(f"📊 Начинаем сбор данных для {len(symbols)} символов (MEXC)")
    print(f"{'='*80}")
    print(f"📌 ЖЁСТКИЕ ФИЛЬТРЫ ЛИКВИДНОСТИ:")
    print(f"   • Объём торгов за 24ч: > ${FILTERS['min_volume_24h_usd']:,.0f}")
    print(f"   • Количество сделок за час: > {FILTERS['min_trade_count_per_hour']}")
    print(f"   • Свежесть последней сделки: < {FILTERS['max_time_since_last_trade_seconds']} секунд")
    print(f"   • Средний размер сделки: > ${FILTERS['min_avg_trade_size_usd']:,.0f}")
    print(f"{'='*80}")
    print(f"⚠️ ВНИМАНИЕ: В файл попадут ТОЛЬКО монеты, прошедшие ВСЕ фильтры!")
    print(f"{'='*80}\n")

    # Пытаемся получить все тикеры (если поддерживается)
    tickers_map = await fetch_all_tickers()
    if tickers_map is None:
        print("⚠️ MEXC не поддерживает массовое получение тикеров, будем получать их по одному (медленнее).")
    else:
        print(f"📡 Получено {len(tickers_map)} тикеров одним запросом.")

    results = {}
    tasks = [
        process_symbol(symbol, timestamps, now, results, tickers_map,
                       FILTERS['min_volume_24h_usd'],
                       FILTERS['min_trade_count_per_hour'],
                       FILTERS['max_time_since_last_trade_seconds'],
                       FILTERS['min_avg_trade_size_usd'])
        for symbol in symbols
    ]
    await asyncio.gather(*tasks)

    output_file = DATA_DIR / "funding_results_mexc.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        total_checked = len(symbols)
        liquid_count = len(results)
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        print(f"\n{'='*80}")
        print(f"✅ Результаты MEXC сохранены в: {output_file}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   Проверено монет: {total_checked}")
        print(f"   Прошли фильтры и сохранены: {liquid_count} ({liquid_count/total_checked*100:.1f}%)")
        print(f"   Отфильтровано (НЕ сохранены): {total_checked - liquid_count} ({(total_checked-liquid_count)/total_checked*100:.1f}%)")
        print(f"   ⏱️ Время выполнения: {minutes} мин {seconds} сек")
        print(f"{'='*80}")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

    await mexc.close()


if __name__ == "__main__":
    asyncio.run(main())