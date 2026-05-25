import aiohttp
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
CONFIG = {
    'base_url': 'https://www.asterdex.com',
    'ticker_endpoint': '/fapi/v1/ticker/24hr',
    'funding_endpoint': '/bapi/futures/v1/public/future/common/get-funding-rate-history',
    'source_code': 'astherus',
    'headers': {'content-type': 'application/json'}
}

semaphore = asyncio.Semaphore(20)
EXCHANGE_NAME = 'Aster'


async def fetch_all_tickers(session):
    url = CONFIG['base_url'] + CONFIG['ticker_endpoint']
    try:
        async with session.get(url, headers=CONFIG['headers']) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return {item['symbol']: item for item in data}
                elif isinstance(data, dict) and 'data' in data:
                    return {item['symbol']: item for item in data['data']}
    except Exception as e:
        print(f"❌ Ошибка получения тикеров: {e}")
    return {}


async def fetch_funding_history(symbol: str, start_ms: int, end_ms: int, rows=336):
    url = CONFIG['base_url'] + CONFIG['funding_endpoint']
    payload = {
        'symbol': symbol,
        'page': 1,
        'rows': rows,
        'sourceCode': CONFIG['source_code']
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=CONFIG['headers']) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('code') == '000000' and 'data' in data:
                        records = data['data']
                        return [{'timestamp': r['calcTime'], 'rate': float(r['lastFundingRate'])} for r in records if start_ms <= r['calcTime'] <= end_ms]
    except Exception:
        pass
    return []


async def fetch_new_funding_history(symbol: str, start_ms: int, end_ms: int):
    return await fetch_funding_history(symbol, start_ms, end_ms, rows=336)


def detect_funding_interval(history):
    if len(history) < 2:
        return 8
    sorted_hist = sorted(history, key=lambda x: x['timestamp'])
    intervals = [sorted_hist[i]['timestamp'] - sorted_hist[i-1]['timestamp']
                 for i in range(1, len(sorted_hist))]
    if not intervals:
        return 8
    most_common = Counter(intervals).most_common(1)[0][0]
    hours = round(most_common / (1000 * 3600))
    return hours if 1 <= hours <= 24 else 8


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict,
                         tickers: dict, min_vol: float, min_trades_24h: int):
    async with semaphore:
        try:
            ticker = tickers.get(symbol, {})
            volume = float(ticker.get('quoteVolume', 0))
            trades_24h = int(ticker.get('count', 0))
            if volume < min_vol or trades_24h < min_trades_24h:
                return
            print(f"✅ {symbol}: volume=${volume:,.0f}, trades_24h={trades_24h}")

            # Текущий фандинг (берём последнюю запись)
            current_fr = None
            try:
                hist_now = await fetch_funding_history(symbol, now.timestamp() * 1000 - 86400000, now.timestamp() * 1000, rows=10)
                if hist_now:
                    latest = max(hist_now, key=lambda x: x['timestamp'])
                    current_fr = round(latest['rate'] * 100, 6)
            except Exception:
                pass

            start_30d = int((now - timedelta(hours=720)).timestamp() * 1000)
            end = int(now.timestamp() * 1000)
            last_ts = await get_last_timestamp(EXCHANGE_NAME, symbol)
            since = max(last_ts + 1, start_30d)
            if since <= end:
                new = await fetch_new_funding_history(symbol, since, end)
                if new:
                    await save_funding_rates(EXCHANGE_NAME, symbol, new)
                    print(f"   💾 Сохранено {len(new)} записей")
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
                "openInterest": None
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
    with open(DATA_DIR / "tradePairsAster.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

    MIN_VOL = 500000
    MIN_TRADES_24H = 100
    async with aiohttp.ClientSession() as session:
        print("📡 Получаем 24h тикеры...")
        tickers = await fetch_all_tickers(session)
        if not tickers:
            return
        results = {}
        tasks = [process_symbol(s, timestamps, now, results, tickers, MIN_VOL, MIN_TRADES_24H) for s in symbols]
        await asyncio.gather(*tasks)
    out_file = DATA_DIR / "funding_results_aster.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    elapsed = time.time() - start
    print(f"\n✅ Сохранено в {out_file}, время: {int(elapsed//60)}:{int(elapsed%60):02d}, монет: {len(results)}")


if __name__ == "__main__":
    asyncio.run(main())