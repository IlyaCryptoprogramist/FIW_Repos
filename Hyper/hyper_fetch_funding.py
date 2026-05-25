import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
from pathlib import Path
import time
import sys

sys.path.append(str(Path(__file__).parent.parent))
from common.db import init_db, get_last_timestamp, save_funding_rates, get_history

DATA_DIR = Path(__file__).parent

hyper = ccxt.hyperliquid({'timeout': 30000, 'enableRateLimit': True})
semaphore = asyncio.Semaphore(20)
EXCHANGE_NAME = 'Hyper'


async def fetch_ticker_volume(symbol: str):
    try:
        ticker = await hyper.fetch_ticker(symbol)
        info = ticker.get('info', {})
        vol = ticker.get('quoteVolume') or info.get('dayNtlVlm', 0)
        return float(vol) if vol else 0.0
    except Exception:
        return 0.0


async def fetch_new_funding_history(symbol: str, start_ms: int, end_ms: int, limit=1000):
    all_history = []
    since = start_ms
    while since <= end_ms:
        try:
            partial = await hyper.fetch_funding_rate_history(symbol, since=since, limit=limit)
            if not partial:
                break
            partial = [p for p in partial if p['timestamp'] <= end_ms]
            if not partial:
                break
            all_history.extend(partial)
            latest = max(p['timestamp'] for p in partial)
            if latest >= end_ms:
                break
            since = latest + 1
        except Exception:
            break
    return all_history


def detect_funding_interval(history):
    if len(history) < 2:
        return 1
    sorted_hist = sorted(history, key=lambda x: x['timestamp'])
    intervals = [sorted_hist[i]['timestamp'] - sorted_hist[i-1]['timestamp']
                 for i in range(1, len(sorted_hist))]
    if not intervals:
        return 1
    most_common = Counter(intervals).most_common(1)[0][0]
    hours = round(most_common / (1000 * 3600))
    return hours if 1 <= hours <= 24 else 1


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict,
                         min_vol: float):
    async with semaphore:
        try:
            volume = await fetch_ticker_volume(symbol)
            if volume < min_vol:
                return
            print(f"✅ {symbol}: volume=${volume:,.0f}")

            current_fr = None
            try:
                fr = await hyper.fetch_funding_rate(symbol)
                cf = fr.get('fundingRate')
                if cf is not None:
                    current_fr = round(cf * 100, 6)
            except Exception:
                pass

            # Open Interest – недоступен в Hyperliquid через CCXT
            open_interest = None

            start_30d = int((now - timedelta(hours=720)).timestamp() * 1000)
            end = int(now.timestamp() * 1000)
            last_ts = await get_last_timestamp(EXCHANGE_NAME, symbol)
            since = max(last_ts + 1, start_30d)
            if since <= end:
                new = await fetch_new_funding_history(symbol, since, end, limit=1000)
                if new:
                    to_save = [{'timestamp': r['timestamp'], 'rate': r['fundingRate']} for r in new]
                    await save_funding_rates(EXCHANGE_NAME, symbol, to_save)
                    print(f"   💾 Сохранено {len(to_save)} записей")
            hist = await get_history(EXCHANGE_NAME, symbol, start_30d, end)
            if not hist:
                return
            interval = detect_funding_interval(hist)
            total_24h = total_48h = total_168h = total_720h = 0.0
            for h in hist:
                ts = h['timestamp']
                rate = h['rate'] * 100
                if timestamps["24h"] < ts < end:
                    total_24h += rate
                if timestamps["48h"] < ts < end:
                    total_48h += rate
                if timestamps["168h"] < ts < end:
                    total_168h += rate
                if timestamps["720h"] < ts < end:
                    total_720h += rate

            results[symbol] = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": current_fr,
                "fundingIntervalHours": interval,
                "nextFundingTime": None,
                "volume24hUSD": round(volume, 2),
                "tradeCountLastHour": 0,
                "avgTradeSizeUSD": 0.0,
                "timeSinceLastTradeSeconds": None,
                "tradesPerHour": 0.0,
                "total_records": len(hist),
                "openInterest": open_interest
            }
            print(f"   → 30д={total_720h:.4f}%, 7д={total_168h:.4f}%, записей={len(hist)}")
        except Exception as e:
            print(f"❌ Ошибка {symbol}: {e}")


async def main():
    start = time.time()
    await init_db()
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
    }
    with open(DATA_DIR / "tradePairsHyper.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

    MIN_VOL = 500000
    results = {}
    tasks = [process_symbol(s, timestamps, now, results, MIN_VOL) for s in symbols]
    await asyncio.gather(*tasks)

    out_file = DATA_DIR / "funding_results_hyper.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    elapsed = time.time() - start
    print(f"\n✅ Сохранено в {out_file}, время: {int(elapsed//60)}:{int(elapsed%60):02d}, монет: {len(results)}")
    await hyper.close()


if __name__ == "__main__":
    asyncio.run(main())