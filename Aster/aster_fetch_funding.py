# aster_fetch_funding.py
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from aster_config import ASTER_CONFIG


class AsterFundingCollector:
    def __init__(self, symbols_file: str = "tradePairsAster.json", output_dir: str = "funding_data"):
        self.config = ASTER_CONFIG
        self.symbols = self.load_symbols(symbols_file)
        self.output_dir = output_dir
        self.base_url = self.config["base_url"]
        self.funding_endpoint = self.config["endpoints"]["funding_rate_history"]
        self.ticker_endpoint = self.config["endpoints"]["ticker_24hr"]
        self.headers = self.config["headers"]
        self.source_code = self.config["source_code"]
        self.semaphore = asyncio.Semaphore(10)

        os.makedirs(output_dir, exist_ok=True)

    def load_symbols(self, symbols_file: str) -> List[str]:
        with open(symbols_file, 'r', encoding='utf-8') as f:
            symbols = json.load(f)
        if not symbols:
            raise ValueError(f"Файл {symbols_file} пуст")
        print(f"✅ Загружено {len(symbols)} символов")
        return symbols

    async def fetch_all_tickers(self, session: aiohttp.ClientSession) -> Dict[str, Dict]:
        url = self.base_url + self.ticker_endpoint
        try:
            async with session.get(url, headers=self.headers, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        return {item['symbol']: item for item in data}
                    elif isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                        return {item['symbol']: item for item in data['data']}
                    else:
                        print("⚠️ Неизвестный формат ответа тикеров")
                        return {}
                else:
                    print(f"❌ Ошибка получения тикеров: HTTP {resp.status}")
                    return {}
        except Exception as e:
            print(f"❌ Ошибка получения тикеров: {e}")
            return {}

    async def get_funding_history(self, session: aiohttp.ClientSession, symbol: str, rows: int = 336) -> Dict[str, Any]:
        url = self.base_url + self.funding_endpoint
        payload = {
            "symbol": symbol,
            "page": 1,
            "rows": rows,
            "sourceCode": self.source_code
        }
        async with self.semaphore:
            try:
                async with session.post(url, headers=self.headers, json=payload, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("code") == "000000" and "data" in data:
                            funding_data = data["data"]
                            filtered = self.filter_last_30_days(funding_data)
                            return {
                                "symbol": symbol,
                                "success": True,
                                "total_records": len(funding_data),
                                "filtered_records": len(filtered),
                                "data": filtered,
                                "last_update": datetime.now().isoformat()
                            }
                        else:
                            return {"symbol": symbol, "success": False, "error": f"API error: {data.get('message')}"}
                    else:
                        return {"symbol": symbol, "success": False, "error": f"HTTP {resp.status}"}
            except Exception as e:
                return {"symbol": symbol, "success": False, "error": str(e)}

    def filter_last_30_days(self, data: List[Dict]) -> List[Dict]:
        if not data:
            return []
        thirty_days_ago = datetime.now() - timedelta(days=30)
        filtered = []
        for record in data:
            calc_time = record.get('calcTime')
            if calc_time:
                try:
                    record_date = datetime.fromtimestamp(calc_time / 1000)
                    if record_date >= thirty_days_ago:
                        record['calcTimeReadable'] = record_date.isoformat()
                        filtered.append(record)
                except Exception:
                    filtered.append(record)
            else:
                filtered.append(record)
        return filtered

    def detect_funding_interval(self, funding_data: List[Dict]) -> int:
        if len(funding_data) < 2:
            return 8
        sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0))
        intervals = []
        for i in range(1, len(sorted_data)):
            diff = sorted_data[i]['calcTime'] - sorted_data[i-1]['calcTime']
            if diff > 0:
                intervals.append(diff)
        if not intervals:
            return 8
        intervals.sort()
        median = intervals[len(intervals)//2]
        hours = round(median / (1000 * 3600))
        return hours if 1 <= hours <= 24 else 8

    def get_next_funding_time(self, funding_data: List[Dict]) -> str:
        if not funding_data:
            return None
        interval_hours = self.detect_funding_interval(funding_data)
        sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0))
        last_time = sorted_data[-1]['calcTime']
        next_time = datetime.fromtimestamp(last_time / 1000) + timedelta(hours=interval_hours)
        return next_time.strftime('%Y-%m-%d %H:%M UTC')

    async def collect_all_symbols(self, min_volume_usd: float = 500000, min_trade_count_24h: int = 100) -> Dict[str, Any]:
        print(f"\n{'='*60}")
        print(f"Начинаем сбор данных для {len(self.symbols)} символов (Aster DEX)")
        print(f"Фильтры: объём за 24ч > ${min_volume_usd:,.0f}, сделок за 24ч > {min_trade_count_24h}")
        print(f"{'='*60}\n")

        async with aiohttp.ClientSession() as session:
            print("📡 Получаем 24-часовую статистику для всех символов...")
            tickers_map = await self.fetch_all_tickers(session)
            if not tickers_map:
                print("❌ Не удалось получить статистику. Работа невозможна.")
                return {}

            print("📡 Загружаем историю фандинга для всех символов (параллельно)...")
            tasks = [self.get_funding_history(session, sym) for sym in self.symbols]
            funding_results = await asyncio.gather(*tasks)

        results = {}
        main_data = {}
        all_data = []
        passed_count = 0
        filtered_count = 0
        error_count = 0

        for funding_res in funding_results:
            symbol = funding_res["symbol"]
            if not funding_res["success"]:
                results[symbol] = {"status": "failed", "error": funding_res["error"]}
                error_count += 1
                continue

            ticker = tickers_map.get(symbol)
            if not ticker:
                results[symbol] = {"status": "failed", "error": "no ticker data"}
                error_count += 1
                continue

            quote_volume = float(ticker.get('quoteVolume', 0))
            trade_count_24h = int(ticker.get('count', 0))

            if quote_volume < min_volume_usd or trade_count_24h < min_trade_count_24h:
                results[symbol] = {"status": "filtered", "volume": quote_volume, "trades": trade_count_24h}
                filtered_count += 1
                continue

            funding_history = funding_res.get("data", [])
            if not funding_history:
                results[symbol] = {"status": "failed", "error": "no funding history"}
                error_count += 1
                continue

            passed_count += 1

            now = datetime.now()
            timestamps = {
                "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
                "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
                "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
                "720h": int((now - timedelta(hours=720)).timestamp() * 1000),
            }
            end_time_ms = int(now.timestamp() * 1000)

            total_24h = total_48h = total_168h = total_720h = 0.0
            for rec in funding_history:
                ct = rec.get('calcTime')
                if not ct:
                    continue
                try:
                    raw_rate = float(rec.get('lastFundingRate', '0'))
                    rate_percent = raw_rate * 100   # перевод в проценты
                except:
                    continue
                if timestamps["24h"] < ct < end_time_ms:
                    total_24h += rate_percent
                if timestamps["48h"] < ct < end_time_ms:
                    total_48h += rate_percent
                if timestamps["168h"] < ct < end_time_ms:
                    total_168h += rate_percent
                if timestamps["720h"] < ct < end_time_ms:
                    total_720h += rate_percent

            funding_interval_hours = self.detect_funding_interval(funding_history)

            # текущая ставка – БЕЗ умножения на 100
            current_fr = None
            if funding_history:
                latest = sorted(funding_history, key=lambda x: x.get('calcTime', 0), reverse=True)[0]
                cf = latest.get('lastFundingRate')
                if cf is not None:
                    current_fr = round(float(cf), 6)

            next_time = self.get_next_funding_time(funding_history)

            result_entry = {
                "24h": round(total_24h, 6),
                "48h": round(total_48h, 6),
                "168h": round(total_168h, 6),
                "720h": round(total_720h, 6),
                "currentFR": current_fr,
                "fundingIntervalHours": funding_interval_hours,
                "nextFundingTime": next_time,
                "volume24hUSD": round(quote_volume, 2),
                "tradeCountLastHour": 0,
                "avgTradeSizeUSD": 0.0,
                "timeSinceLastTradeSeconds": None,
                "tradesPerHour": 0.0,
                "total_records": len(funding_history),
                "priceChangePercent": ticker.get('priceChangePercent'),
                "highPrice": ticker.get('highPrice'),
                "lowPrice": ticker.get('lowPrice'),
                "lastPrice": ticker.get('lastPrice')
            }

            main_data[symbol] = result_entry
            all_data.append({
                "symbol": symbol,
                "success": True,
                "data": result_entry,
                "ticker": ticker
            })
            results[symbol] = {
                "status": "success",
                "records": len(funding_history),
                "volume24hUSD": quote_volume,
                "tradeCount24h": trade_count_24h
            }

        self.save_results(main_data, all_data, results)

        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   Всего символов: {len(self.symbols)}")
        print(f"   ✅ Прошли фильтр: {passed_count}")
        print(f"   ❌ Отфильтровано (объём/сделки): {filtered_count}")
        print(f"   ⚠️ Ошибки (нет данных): {error_count}")
        print(f"{'='*60}")

        return {
            "total_symbols": len(self.symbols),
            "passed": passed_count,
            "filtered": filtered_count,
            "errors": error_count,
            "results": results
        }

    def save_results(self, main_data: Dict, all_data: List[Dict], results: Dict):
        # основной результат сохраняем прямо в корень self.output_dir
        funding_file = os.path.join(self.output_dir, "funding_results_aster.json")
        with open(funding_file, 'w', encoding='utf-8') as f:
            json.dump(main_data, f, indent=4, ensure_ascii=False)

        # вспомогательные файлы – в подпапку funding_data_aux
        aux_dir = os.path.join(self.output_dir, "funding_data_aux")
        os.makedirs(aux_dir, exist_ok=True)

        all_file = os.path.join(aux_dir, "all_funding_data.json")
        with open(all_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

        stats_file = os.path.join(aux_dir, "collection_stats.json")
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        report_file = os.path.join(aux_dir, "collection_report.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("ОТЧЁТ О СБОРЕ ДАННЫХ ASTER DEX\n")
            f.write("="*60 + "\n\n")
            f.write(f"Дата: {datetime.now().isoformat()}\n")
            successful = sum(1 for r in results.values() if r.get("status") == "success")
            f.write(f"Прошли фильтр: {successful}\n")
            filtered = sum(1 for r in results.values() if r.get("status") == "filtered")
            f.write(f"Отфильтровано: {filtered}\n")
            errors = sum(1 for r in results.values() if r.get("status") == "failed")
            f.write(f"Ошибки: {errors}\n\n")
            for sym, r in results.items():
                if r.get("status") == "success":
                    f.write(f"✅ {sym}: {r['records']} зап., объём={r['volume24hUSD']:.0f}$, сделок={r['tradeCount24h']}\n")
                elif r.get("status") == "filtered":
                    f.write(f"⏭️ {sym}: отфильтрован (объём={r['volume']:.0f}$, сделок={r['trades']})\n")
                else:
                    f.write(f"❌ {sym}: {r.get('error', 'unknown')}\n")

        print(f"\n✅ Основной результат сохранён в {funding_file}")
        print(f"   Вспомогательные файлы в {aux_dir}")


async def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    symbols_path = os.path.join(script_dir, "tradePairsAster.json")
    output_path = script_dir   # ← сохраняем прямо в папку скрипта

    if not os.path.exists(symbols_path):
        print(f"❌ Файл {symbols_path} не найден!")
        return

    print(f"📁 Файл символов: {symbols_path}")
    collector = AsterFundingCollector(symbols_file=symbols_path, output_dir=output_path)

    print("\n🚀 Запуск сбора данных...")
    stats = await collector.collect_all_symbols(
        min_volume_usd=500000,
        min_trade_count_24h=100
    )


if __name__ == "__main__":
    asyncio.run(main())