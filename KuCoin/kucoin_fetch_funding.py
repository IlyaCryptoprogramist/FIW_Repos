# fetch_kucoin_funding.py

import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time

# Путь к папке с данными KuCoin
DATA_DIR = "D:/Ilya/My project/FIW_soft/FIW_soft/KuCoin"

kucoin = ccxt.kucoinfutures({
    'timeout': 3000,
})

# Ограничитель: не более 5 одновременных запросов
semaphore = asyncio.Semaphore(5)

# Глобальный рейт-лимит: ждём между каждым вызовом API
GLOBAL_RATE_LIMIT_MS = kucoin.rateLimit * 2  # например, 400 мс
print(f"Установлен глобальный рейт-лимит: {GLOBAL_RATE_LIMIT_MS} мс")

# Переменная для отслеживания времени последнего вызова API
last_request_time = time.time()


async def wait_for_rate_limit():
    global last_request_time
    elapsed = (time.time() - last_request_time) * 1000  # в мс
    if elapsed < GLOBAL_RATE_LIMIT_MS:
        delay = (GLOBAL_RATE_LIMIT_MS - elapsed) / 1000  # в сек
        await asyncio.sleep(delay)
    last_request_time = time.time()


async def fetch_full_funding_history(symbol: str, start_time_ms: int, end_time_ms: int, limit: int = 100):
    """
    Собирает всю историю funding rate для символа в заданном диапазоне.
    Возвращает список записей.
    """
    all_history = []
    current_since = start_time_ms
    max_iterations = 20  # Защита от бесконечного цикла
    iteration_count = 0

    while iteration_count < max_iterations:
        await wait_for_rate_limit()  # Уважаем рейт-лимиты
        try:
            # Запрашиваем историю с текущего 'since'
            partial_history = await kucoin.fetch_funding_rate_history(
                symbol=symbol,
                since=current_since,
                limit=limit
            )

            if not partial_history:
                # Нет новых данных, выходим
                break

            all_history.extend(partial_history)

            # Находим самый поздний timestamp в полученных данных
            latest_ts = max(entry['timestamp'] for entry in partial_history)

            if latest_ts >= end_time_ms:
                # Достигли конца нужного диапазона
                break

            # Следующий 'since' — на 1 мс позже последней записи, чтобы не дублировать
            current_since = latest_ts + 1
            iteration_count += 1

        except Exception as e:
            print(f"Ошибка при частичном запросе истории FR для {symbol} (since {current_since}): {e}")
            break  # Останавливаем сбор, если ошибка

    return all_history


async def detect_funding_interval(history):
    """
    Определяет интервал выплаты funding rate в часах на основе истории.
    Поддерживает 1ч, 2ч, 4ч, 8ч и другие.
    """
    if len(history) < 2:
        return None

    # Сортируем по времени (на случай, если API вернул вразнобой)
    history = sorted(history, key=lambda x: x['timestamp'])

    # Считаем интервалы между выплатами (в миллисекундах)
    intervals_ms = []
    for i in range(1, len(history)):
        diff = history[i]['timestamp'] - history[i - 1]['timestamp']
        intervals_ms.append(diff)

    # Находим самый частый интервал
    counter = Counter(intervals_ms)
    most_common_ms, _ = counter.most_common(1)[0]

    # Переводим в часы (округляем до ближайшего целого)
    hours = round(most_common_ms / (1000 * 3600))
    return hours if hours > 0 else None


async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict):
    async with semaphore:
        try:
            # Ждём между вызовами API
            await wait_for_rate_limit()

            # Получаем стакан
            try:
                # KuCoin требует limit = 20 или 100
                order_book = await kucoin.fetch_order_book(symbol, limit=20)
            except Exception as e:
                print(f"Ошибка получения стакана для {symbol}: {e}")
                return  # прерываем обработку этого символа

            bids = order_book['bids'][:5]  # Берём первые 5
            asks = order_book['asks'][:5]  # Берём первые 5

            askTotalVolume = 0
            bidTotalVolume = 0

            # Используем безопасное извлечение цены и объёма
            for i in range(min(5, len(bids), len(asks))):
                bidPrice, bidVolume = bids[i][:2]
                askPrice, askVolume = asks[i][:2]

                askTotalVolume += askPrice * askVolume
                bidTotalVolume += bidPrice * bidVolume

            if askTotalVolume > 3000 and bidTotalVolume > 3000:
                # Ждём между вызовами API
                await wait_for_rate_limit()

                # Текущий funding rate
                current_funding = None
                next_funding_time_str = None
                try:
                    fr_data = await kucoin.fetch_funding_rate(symbol)
                    current_funding = fr_data.get('fundingRate')
                    next_ts = fr_data.get('nextFundingTimestamp')
                    if next_ts:
                        next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                    if current_funding is not None:
                        current_funding *= 100  # в %
                except Exception as e:
                    print(f"Ошибка текущего FR для {symbol}: {e}")

                # --- НОВАЯ ЛОГИКА: Сбор всей истории за 168 часов ---
                # Определяем диапазон: от (now - 168 часов) до (now)
                start_time_ms = int((now - timedelta(hours=168)).timestamp() * 1000)
                end_time_ms = int(now.timestamp() * 1000)

                # Ждём между вызовами API
                await wait_for_rate_limit()

                try:
                    full_funding_history = await fetch_full_funding_history(
                        symbol=symbol,
                        start_time_ms=start_time_ms,
                        end_time_ms=end_time_ms,
                        limit=100
                    )
                except Exception as e:
                    print(f"Ошибка получения полной истории FR для {symbol}: {e}")
                    return  # прерываем обработку этого символа

                # --- [DEBUG] Выводим информацию о собранной истории ---
                print(f"[DEBUG] {symbol}: получено {len(full_funding_history)} записей истории FR")
                if full_funding_history:
                    latest_ts = max(entry['timestamp'] for entry in full_funding_history)
                    oldest_ts = min(entry['timestamp'] for entry in full_funding_history)
                    print(f"[DEBUG] {symbol}: от {datetime.utcfromtimestamp(oldest_ts / 1000)} до {datetime.utcfromtimestamp(latest_ts / 1000)}")
                # --- [/DEBUG] ---

                # --- Сортировка и фильтрация по периодам ---
                # Сортируем по возрастанию времени (для корректной обработки)
                full_funding_history.sort(key=lambda x: x['timestamp'])

                # Суммируем payout за периоды
                total_24h = total_48h = total_168h = 0.0

                for entry in full_funding_history:
                    ts = entry['timestamp']
                    rate = entry['fundingRate'] * 100  # в %

                    if timestamps["24h"] < ts < end_time_ms:
                        total_24h += rate
                    if timestamps["48h"] < ts < end_time_ms:
                        total_48h += rate
                    if timestamps["168h"] < ts < end_time_ms:
                        total_168h += rate

                # Определяем интервал выплат на основе собранной истории
                funding_interval_hours = await detect_funding_interval(full_funding_history)

                # Сохраняем всё
                results[symbol] = {
                    "24h": round(total_24h, 6),
                    "48h": round(total_48h, 6),
                    "168h": round(total_168h, 6),
                    "currentFR": round(current_funding, 6) if current_funding is not None else None,
                    "fundingIntervalHours": funding_interval_hours,  # ← 1, 2, 4, 8 и т.д.
                    "nextFundingTime": next_funding_time_str,
                    "askTotalVolume": round(askTotalVolume, 2),
                    "bidTotalVolume": round(bidTotalVolume, 2)
                }

        except Exception as e:
            print(f"Неожиданная ошибка при обработке {symbol} на KuCoin: {e}")


async def main():
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
    }

    input_file = f"{DATA_DIR}/tradePairsKuCoin.json"
    with open(input_file, "r", encoding="utf-8") as f:
        symbols = json.load(f)

    results = {}
    tasks = [process_symbol(symbol, timestamps, now, results) for symbol in symbols]
    await asyncio.gather(*tasks)

    output_file = f"{DATA_DIR}/funding_results_kucoin.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(f"Результаты KuCoin сохранены в: {output_file}")
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

    await kucoin.close()


if __name__ == "__main__":
    asyncio.run(main())