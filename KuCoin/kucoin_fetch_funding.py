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

kucoin = ccxt.kucoinfutures({'timeout': 30000, 'enableRateLimit': True})
semaphore = asyncio.Semaphore(20)
EXCHANGE_NAME = 'KuCoin'


def normalize_symbol(raw: str) -> str:
    if not raw:
        return None
    no_m = raw[:-1] if raw.endswith('M') else raw
    if no_m.endswith('USDT'):
        base = no_m[:-4]
    else:
        base = no_m
    if base == 'XBT':
        base = 'BTC'
    return f"{base}/USDT:USDT"


async def fetch_all_contracts():
    try:
        resp = await kucoin.futuresPublicGetContractsActive()
        data = resp.get('data', [])
        result = {}
        for c in data:
            raw = c.get('symbol')
            if raw:
                norm = normalize_symbol(raw)
                result[norm] = {
                    'volume': float(c.get('turnoverOf24h', 0)),
                    'fundingRate': float(c.get('fundingFeeRate', 0)) if c.get('fundingFeeRate') is not None else None
                }
        return result
    except Exception as e:
        print(f"❌ Ошибка загрузки контрактов: {e}")
        return {}


async def fetch_new_funding_history(symbol: str, start_ms: int, end_ms: int, limit=1000):
    all_history = []
    since = start_ms
    while since <= end_ms:
        try:
            partial = await kucoin.fetch_funding_rate_history(symbol, since=since, limit=limit)
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
                         contracts: dict, min_vol: float):
    async with semaphore:
        try:
            info = contracts.get(symbol, {})
            volume = info.get('volume', 0)
            if volume < min_vol:
                return
            print(f"✅ {symbol}: volume=${volume:,.0f}")

            current_fr = info.get('fundingRate')
            if current_fr is not None:
                current_fr = round(current_fr * 100, 6)
            next_time = None
            try:
                fr = await kucoin.fetch_funding_rate(symbol)
                nts = fr.get('nextFundingTimestamp')
                if nts:
                    next_time = datetime.utcfromtimestamp(nts / 1000).strftime('%Y-%m-%d %H:%M UTC')
            except Exception:
                pass

            # Open Interest
            open_interest = None
            try:
                oi_data = await kucoin.fetch_open_interest(symbol)
                open_interest = oi_data.get('openInterestValue') or oi_data.get('openInterestAmount')
                if open_interest:
                    open_interest = round(float(open_interest), 2)
            except Exception as e:
                print(f"   ⚠️ Ошибка OI: {e}")

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
                "nextFundingTime": next_time,
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
    with open(DATA_DIR / "tradePairsKuCoin.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

    MIN_VOL = 500000
    print("📡 Загружаем контракты...")
    contracts = await fetch_all_contracts()
    if not contracts:
        await kucoin.close()
        return
    results = {}
    tasks = [process_symbol(s, timestamps, now, results, contracts, MIN_VOL) for s in symbols]
    await asyncio.gather(*tasks)

    out_file = DATA_DIR / "funding_results_kucoin.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    elapsed = time.time() - start
    print(f"\n✅ Сохранено в {out_file}, время: {int(elapsed//60)}:{int(elapsed%60):02d}, монет: {len(results)}")
    await kucoin.close()


if __name__ == "__main__":
    asyncio.run(main())