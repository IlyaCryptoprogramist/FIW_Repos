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

bybit = ccxt.bybit({
    'timeout': 10000,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

semaphore = asyncio.Semaphore(20)
EXCHANGE_NAME = 'Bybit'


async def fetch_all_tickers():
    try:
        tickers = await bybit.fetch_tickers()
        return {symbol: ticker for symbol, ticker in tickers.items()}
    except Exception as e:
        print(f"❌ Ошибка получения всех тикеров: {e}")
        return {}


async def analyze_trades_activity(symbol: str, hours_back: int = 1):
    try:
        since = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        trades = await bybit.fetch_trades(symbol, since=since, limit=1000)
        if not trades:
            return {'total_volume_usd': 0, 'trade_count': 0, 'avg_trade_size': 0,
                    'time_since_last_trade': None, 'trades_per_hour': 0}
        total = sum(t['cost'] for t in trades)
        cnt = len(trades)
        avg = total / cnt
        latest = max(t['timestamp'] for t in trades)
        last_sec = (datetime.now().timestamp() * 1000 - latest) / 1000
        return {
            'total_volume_usd': total,
            'trade_count': cnt,
            'avg_trade_size': avg,
            'time_since_last_trade': last_sec,
            'trades_per_hour': cnt / hours_back
        }
    except Exception as e:
        print(f"⚠️ Ошибка анализа сделок для {symbol}: {e}")
        return {'total_volume_usd': 0, 'trade_count': 0, 'avg_trade_size': 0,
                'time_since_last_trade': None, 'trades_per_hour': 0}


async def fetch_new_funding_history(symbol: str, start_ms: int, end_ms: int, limit=1000):
    all_history = []
    since = start_ms
    while since <= end_ms:
        try:
            partial = await bybit.fetch_funding_rate_history(symbol, since=since, limit=limit)
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
                         tickers_map: dict,
                         min_vol: float, min_trades: int, max_last_sec: int, min_avg: float):
    async with semaphore:
        try:
            ticker = tickers_map.get(symbol, {})
            volume = ticker.get('quoteVolume', 0)
            trades_info = await analyze_trades_activity(symbol)
            if not (volume >= min_vol and trades_info['trade_count'] >= min_trades and
                    trades_info.get('time_since_last_trade') is not None and
                    trades_info['time_since_last_trade'] <= max_last_sec and
                    trades_info['avg_trade_size'] >= min_avg):
                return
            print(f"✅ {symbol}: volume=${volume:,.0f}, trades/h={trades_info['trade_count']}")

            # Текущий фандинг
            current_funding = None
            next_funding_time_str = None
            try:
                fr_data = await bybit.fetch_funding_rate(symbol)
                current_funding = fr_data.get('fundingRate')
                nts = fr_data.get('nextFundingTimestamp')
                if nts:
                    next_funding_time_str = datetime.utcfromtimestamp(nts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                if current_funding is not None:
                    current_funding = round(current_funding * 100, 6)
            except Exception as e:
                print(f"   ⚠️ Ошибка текущего FR: {e}")

            # Open Interest
            open_interest = None
            try:
                oi_data = await bybit.fetch_open_interest(symbol)
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
                "currentFR": current_funding,
                "fundingIntervalHours": interval,
                "nextFundingTime": next_funding_time_str,
                "volume24hUSD": round(volume, 2),
                "tradeCountLastHour": trades_info['trade_count'],
                "avgTradeSizeUSD": round(trades_info['avg_trade_size'], 2),
                "timeSinceLastTradeSeconds": round(trades_info['time_since_last_trade'], 1) if trades_info['time_since_last_trade'] else None,
                "tradesPerHour": round(trades_info['trades_per_hour'], 2),
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
    with open(DATA_DIR / "tradePairsBybite.json", "r", encoding="utf-8") as f:
        symbols = json.load(f)

    FILTERS = {'min_vol': 500000, 'min_trades': 100, 'max_last_sec': 25, 'min_avg': 10}
    print("📡 Получаем 24-часовую статистику...")
    tickers = await fetch_all_tickers()
    if not tickers:
        await bybit.close()
        return
    results = {}
    tasks = [process_symbol(s, timestamps, now, results, tickers, **FILTERS) for s in symbols]
    await asyncio.gather(*tasks)

    out_file = DATA_DIR / "funding_results_bybite.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    elapsed = time.time() - start
    print(f"\n✅ Сохранено в {out_file}, время: {int(elapsed//60)}:{int(elapsed%60):02d}, монет: {len(results)}")
    await bybit.close()


if __name__ == "__main__":
    asyncio.run(main())