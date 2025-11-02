# fetch_hyper_funding.py

import ccxt.async_support as ccxt
import asyncio
import json
from datetime import datetime, timedelta
from collections import Counter
import time
from tqdm.asyncio import tqdm
from pathlib import Path

# Use relative path based on current file location
DATA_DIR = Path(__file__).parent

hyper = ccxt.hyperliquid({
    'timeout': 3000,
})

# Ограничитель: не более 5 одновременных запросов
semaphore = asyncio.Semaphore(5)

# Глобальный рейт-лимит: ждём между каждым вызовом API
GLOBAL_RATE_LIMIT_MS = max(hyper.rateLimit * 2, 100) # Установим минимальный лимит 100мс
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

async def process_symbol(symbol: str, timestamps: dict, now: datetime, results: dict):
    async with semaphore:
        try:
            # Ждём между вызовами API
            await wait_for_rate_limit()

            # Получаем стакан
            try:
                order_book = await hyper.fetch_order_book(symbol, limit=5)
            except Exception as e:
                print(f"❌ Ошибка получения стакана для {symbol}: {e}")
                return  # прерываем обработку этого символа

            bids = order_book['bids'][:5]
            asks = order_book['asks'][:5]

            askTotalVolume = 0.0
            bidTotalVolume = 0.0

            # Используем безопасное извлечение цены и объёма
            for i in range(min(5, len(bids), len(asks))):
                bidPrice, bidVolume = bids[i][:2]
                askPrice, askVolume = asks[i][:2]

                askTotalVolume += askVolume
                bidTotalVolume += bidVolume

            if askTotalVolume > 3000 and bidTotalVolume > 3000:
                # Ждём между вызовами API
                await wait_for_rate_limit()

                # Текущий funding rate и время следующего
                current_funding = None
                next_funding_time_str = None
                try:
                    fr_data = await hyper.fetch_funding_rate(symbol)
                    current_funding = fr_data.get('fundingRate')
                    next_ts = fr_data.get('fundingTimestamp') # Используем fundingTimestamp, если он есть
                    if not next_ts:
                         next_ts = fr_data.get('nextFundingTimestamp') # Резервный вариант
                    if next_ts:
                        next_funding_time_str = datetime.utcfromtimestamp(next_ts / 1000).strftime('%Y-%m-%d %H:%M UTC')
                    if current_funding is not None:
                        current_funding *= 100  # в %
                except Exception as e:
                    print(f"❌ Ошибка текущего FR для {symbol}: {e}")
                    return # Если не получили FR, нечего рассчитывать

                # --- НОВАЯ ЛОГИКА: Получаем историю ОДИН РАЗ ---
                await wait_for_rate_limit()
                try:
                    full_funding_history = await hyper.fetch_funding_rate_history(symbol)
                    print(f"[DEBUG] {symbol}: получено {len(full_funding_history)} записей истории FR от API.")
                except Exception as e:
                    print(f"❌ Ошибка получения истории FR для {symbol}: {e}")
                    # Если история недоступна, используем только текущий FR (см. ниже)
                    full_funding_history = []

                # --- АНАЛИЗ ПОЛУЧЕННОЙ ИСТОРИИ ---
                # Добавляем переменную для 30 дней
                total_24h = total_48h = total_168h = total_720h = 0.0
                funding_interval_hours = None

                if full_funding_history:
                    # Сортируем по возрастанию времени (для корректной обработки)
                    full_funding_history.sort(key=lambda x: x['timestamp'])

                    # Фильтруем записи, которые находятся В ПРОШЛОМ и в нужном диапазоне (до 30 дней)
                    now_ms = int(now.timestamp() * 1000)
                    # Используем timestamps['720h'] как начало 30-дневного периода
                    history_in_range = [
                        entry for entry in full_funding_history
                        if entry['timestamp'] < now_ms and entry['timestamp'] > timestamps['720h'] # Проверяем до 30 дней назад
                    ]

                    print(f"[DEBUG] {symbol}: {len(history_in_range)} записей попало в диапазон за последние 720 часов (до текущего времени).")

                    # Если есть прошедшие данные в диапазоне
                    if history_in_range:
                        for entry in history_in_range:
                            ts = entry['timestamp']
                            rate = entry['fundingRate'] * 100  # в %

                            if timestamps["24h"] < ts < now_ms:
                                total_24h += rate
                            if timestamps["48h"] < ts < now_ms:
                                total_48h += rate
                            if timestamps["168h"] < ts < now_ms:
                                total_168h += rate
                            if timestamps["720h"] < ts < now_ms: # Добавляем фильтрацию для 30 дней
                                total_720h += rate

                        # Пытаемся определить интервал на основе *прошедших* данных
                        if len(history_in_range) > 1:
                            intervals_ms = [
                                history_in_range[i]['timestamp'] - history_in_range[i - 1]['timestamp']
                                for i in range(1, len(history_in_range))
                            ]
                            if intervals_ms:
                                counter = Counter(intervals_ms)
                                most_common_ms, _ = counter.most_common(1)[0]
                                funding_interval_hours = round(most_common_ms / (1000 * 3600))
                                funding_interval_hours = funding_interval_hours if funding_interval_hours > 0 else None
                                print(f"[DEBUG] {symbol}: определён интервал фандинга из истории: {funding_interval_hours}ч")
                    else:
                        print(f"[DEBUG] {symbol}: в API-истории нет данных за последние 720 часов. Используем текущий FR как приближение.")
                        # Если в истории нет прошедших данных, используем текущий FR как приближение
                        # Это может быть не очень точно, но это лучшее, что можно сделать, если API не возвращает прошлые ставки
                        # Часто фандинг на Hyperliquid 1-часовой
                        assumed_interval_hours = 1 # Уточнено: часто 1ч на Hyperliquid
                        funding_interval_hours = assumed_interval_hours
                        # Считаем приближённо: FR * количество_часов
                        # total_24h = current_funding * 24
                        # total_48h = current_funding * 48
                        # total_168h = current_funding * 168
                        # total_720h = current_funding * 720 # <- Так было бы для простого усреднения
                        # --- ИЛИ ---
                        # Считаем приближённо: FR * количество_фандингов (округлённое)
                        num_fundings_24h = round(24 / assumed_interval_hours)
                        num_fundings_48h = round(48 / assumed_interval_hours)
                        num_fundings_168h = round(168 / assumed_interval_hours)
                        num_fundings_720h = round(720 / assumed_interval_hours) # Добавляем для 30 дней

                        total_24h = round(current_funding * num_fundings_24h, 6) if current_funding is not None else 0.0
                        total_48h = round(current_funding * num_fundings_48h, 6) if current_funding is not None else 0.0
                        total_168h = round(current_funding * num_fundings_168h, 6) if current_funding is not None else 0.0
                        total_720h = round(current_funding * num_fundings_720h, 6) if current_funding is not None else 0.0 # Добавляем результат
                        print(f"[DEBUG] {symbol}: приближённые суммы (на основе текущего FR): 24h={total_24h}, 48h={total_48h}, 168h={total_168h}, 720h={total_720h}")

                else:
                    # Если fetch_funding_rate_history не вернул ничего
                    print(f"[DEBUG] {symbol}: API истории недоступно. Используем текущий FR как приближение.")
                    assumed_interval_hours = 1 # Уточнено: часто 1ч на Hyperliquid
                    funding_interval_hours = assumed_interval_hours
                    num_fundings_24h = round(24 / assumed_interval_hours)
                    num_fundings_48h = round(48 / assumed_interval_hours)
                    num_fundings_168h = round(168 / assumed_interval_hours)
                    num_fundings_720h = round(720 / assumed_interval_hours) # Добавляем для 30 дней

                    total_24h = round(current_funding * num_fundings_24h, 6) if current_funding is not None else 0.0
                    total_48h = round(current_funding * num_fundings_48h, 6) if current_funding is not None else 0.0
                    total_168h = round(current_funding * num_fundings_168h, 6) if current_funding is not None else 0.0
                    total_720h = round(current_funding * num_fundings_720h, 6) if current_funding is not None else 0.0 # Добавляем результат
                    print(f"[DEBUG] {symbol}: приближённые суммы (на основе текущего FR): 24h={total_24h}, 48h={total_48h}, 168h={total_168h}, 720h={total_720h}")


                # Сохраняем данные
                results[symbol] = {
                    "24h": round(total_24h, 6),
                    "48h": round(total_48h, 6),
                    "168h": round(total_168h, 6),
                    "720h": round(total_720h, 6), # Добавляем результат за 30 дней
                    "currentFR": round(current_funding, 6) if current_funding is not None else None,
                    "fundingIntervalHours": funding_interval_hours, # Может быть None, если не удалось определить
                    "nextFundingTime": next_funding_time_str,
                    "askTotalVolume": round(askTotalVolume, 2),
                    "bidTotalVolume": round(bidTotalVolume, 2)
                }
                # print(f"📊 Обработан {symbol}") # Можно убрать для уменьшения вывода

        except Exception as e:
            print(f"❌ Неожиданная ошибка при обработке {symbol} на Hyperliquid: {e}")


async def main():
    # Загружаем рынки Hyperliquid для проверки контрактов
    await hyper.load_markets()

    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000), # Добавляем метку времени для 30 дней
    }

    input_file = DATA_DIR / "tradePairsHyper.json"
    with open(input_file, "r", encoding="utf-8") as f:
        raw_symbols = json.load(f)

    # Фильтруем только perpetual-контракты с правильным форматом
    valid_symbols = []
    for symbol in raw_symbols:
        # Проверяем, существует ли такой символ в загруженных рынках Hyperliquid
        if symbol in hyper.markets and hyper.markets[symbol].get('contract', False):
            valid_symbols.append(symbol)
        else:
            # Попробуем преобразовать формат, если он не совпадает
            # Наш тест показал BASE/USDC:USDC
            # Если в tradePairsHyper.json, например, "BTC/USDT", пробуем "BTC/USDC:USDC"
            parts = symbol.split('/')
            if len(parts) == 2:
                 base, quote = parts
                 # Попробуем USDC как квоту и в суффиксе
                 converted_symbol = f"{base}/USDC:USDC"
                 if converted_symbol in hyper.markets and hyper.markets[converted_symbol].get('contract', False):
                     valid_symbols.append(converted_symbol)
                     print(f"🔄 Символ {symbol} преобразован в {converted_symbol}")
                 else:
                      print(f"⚠️ Пропущен {symbol}: не найден в рынках Hyperliquid как perpetual (проверено: {symbol}, {converted_symbol})")
            else:
                 print(f"⚠️ Пропущен {symbol}: неправильный формат символа")

    if not valid_symbols:
        print("❌ Нет валидных perpetual-контрактов для обработки!")
        await hyper.close()
        return

    print(f"✅ Найдено {len(valid_symbols)} perpetual-контрактов для обработки.")
    results = {}

    # Создаём задачи
    tasks = [process_symbol(symbol, timestamps, now, results) for symbol in valid_symbols]

    # Запускаем с прогресс-баром
    await tqdm.gather(*tasks, desc="Обработка символов Hyperliquid", total=len(tasks))

    output_file = DATA_DIR / "funding_results_hyper.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(f"\n✅ Результаты Hyperliquid сохранены в: {output_file}")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

    await hyper.close()


if __name__ == "__main__":
    asyncio.run(main())