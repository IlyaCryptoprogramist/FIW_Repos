# fetch_hyper_funding_optimized.py
import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time
from pathlib import Path

DATA_DIR = Path(__file__).parent

hyper = ccxt.hyperliquid({
    'timeout': 30000,
    'enableRateLimit': True,
})

semaphore = asyncio.Semaphore(20)  # увеличен параллелизм


async def fetch_ticker_info(symbol: str):
    """Быстрое получение объёма за 24ч."""
    try:
        ticker = await hyper.fetch_ticker(symbol)
        info = ticker.get('info', {})
        volume = ticker.get('quoteVolume') or info.get('dayNtlVlm', 0)
        return {'volume_24h_usd': float(volume) if volume else 0.0}
    except Exception:
        return {'volume_24h_usd': 0.0}


async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 1000):
    """Собирает историю фандинга (максимальными порциями)."""
    all_history = []
    current_since = start_time_ms
    max_iterations = 5   # достаточно 5 итераций по 1000 записей
    iteration = 0
    while iteration < max_iterations:
        try:
            partial = await hyper.fetch_funding_rate_history(
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
        except Exception:
            break
    return all_history


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict,
                         min_volume_usd: float):
    async with semaphore:
        try:
            ticker = await fetch_ticker_info(symbol)
            volume = ticker['volume_24h_usd']
            if volume < min_volume_usd:
                return
            print(f"✅ {symbol}: объём ${volume:,.0f}")

            # Текущий фандинг (в процентах)
            current_funding = None
            try:
                fr_data = await hyper.fetch_funding_rate(symbol)
                cf = fr_data.get('fundingRate')
                if cf is not None:
                    current_funding = round(cf * 100, 6)
            except Exception:
                pass

            # История за 30 дней
            start = int((now - timedelta(hours=720)).timestamp() * 1000)
            end = int(now.timestamp() * 1000)
            full_history = await fetch_full_funding_history(symbol, start, end, limit=1000)
            if not full_history:
                return

            full_history.sort(key=lambda x: x['timestamp'])
            total_24h = total_48h = total_168h = total_720h = 0.0
            for entry in full_history:
                ts = entry['timestamp']
                rate = entry['fundingRate'] * 100
                if timestamps["24h"] < ts < end:
                    total_24h += rate
                if timestamps["48h"] < ts < end:
                    total_48h += rate
                if timestamps["168h"] < ts < end:
                    total_168h += rate
                if timestamps["720h"] < ts < end:
                    total_720h += rate

            # Определение интервала (по умолчанию 1 час)
            interval = 1
            if len(full_history) > 1:
                diffs = [full_history[i]['timestamp'] - full_history[i-1]['timestamp']
                         for i in range(1, len(full_history))]
                if diffs:
                    most_common = Counter(diffs).most_common(1)[0][0]
                    interval = round(most_common / (1000 * 3600))
                    if interval < 1:
                        interval = 1

            results[symbol] = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": current_funding,
                "fundingIntervalHours": interval,
                "nextFundingTime": None,
                "volume24hUSD": round(volume, 2),
                "tradeCountLastHour": 0,
                "avgTradeSizeUSD": 0.0,
                "timeSinceLastTradeSeconds": None,
                "tradesPerHour": 0.0,
                "total_records": len(full_history)
            }
        except Exception as e:
            print(f"❌ {symbol}: {e}")


async def main():
    start_time = time.time()
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
    }

    input_file = DATA_DIR / "tradePairsHyper.json"
    with open(input_file, "r", encoding="utf-8") as f:
        symbols = json.load(f)

    MIN_VOLUME_USD = 500000

    print(f"\n{'='*80}")
    print(f"📊 Начинаем сбор данных для {len(symbols)} символов (Hyperliquid)")
    print(f"{'='*80}")
    print(f"📌 ФИЛЬТР: объём за 24ч > ${MIN_VOLUME_USD:,.0f}")
    print(f"{'='*80}\n")

    results = {}
    tasks = [process_symbol(sym, timestamps, now, results, MIN_VOLUME_USD) for sym in symbols]
    await asyncio.gather(*tasks)

    output_file = DATA_DIR / "funding_results_hyper.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    elapsed = time.time() - start_time
    print(f"\n✅ Сохранено в {output_file}")
    print(f"   Прошли: {len(results)} / {len(symbols)} ({len(results)/len(symbols)*100:.1f}%)")
    print(f"   ⏱️ Время: {int(elapsed//60)} мин {int(elapsed%60)} сек")

    await hyper.close()


if __name__ == "__main__":
    asyncio.run(main())