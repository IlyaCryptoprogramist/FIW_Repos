import json
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import os

# Используем относительный путь
DATA_DIR = Path(__file__).parent
FUNDING_DATA_DIR = DATA_DIR / "funding_data"


def load_all_funding_data():
    """Загружает все данные из all_funding_data.json"""
    input_file = FUNDING_DATA_DIR / "all_funding_data.json"
    
    if not input_file.exists():
        print(f"❌ Файл {input_file} не найден!")
        print("Сначала запустите aster_fetch_funding.py для сбора данных")
        return None
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Загружены данные для {len(data)} символов")
    return data


def calculate_funding_summary(funding_data, timestamps, end_time_ms):
    """
    Рассчитывает суммарный фандинг за периоды
    funding_data: список записей с полями calcTime (timestamp) и lastFundingRate
    """
    total_24h = 0.0
    total_48h = 0.0
    total_168h = 0.0
    total_720h = 0.0
    
    for record in funding_data:
        calc_time = record.get('calcTime')
        if not calc_time:
            continue
        
        # Конвертируем fundingRate из строки в число
        try:
            funding_rate = float(record.get('lastFundingRate', '0'))
        except (ValueError, TypeError):
            continue
        
        if timestamps["24h"] < calc_time < end_time_ms:
            total_24h += funding_rate
        if timestamps["48h"] < calc_time < end_time_ms:
            total_48h += funding_rate
        if timestamps["168h"] < calc_time < end_time_ms:
            total_168h += funding_rate
        if timestamps["720h"] < calc_time < end_time_ms:
            total_720h += funding_rate
    
    return {
        "24h": round(total_24h, 6),
        "48h": round(total_48h, 6),
        "168h": round(total_168h, 6),
        "720h": round(total_720h, 6)
    }


def detect_funding_interval(funding_data):
    """
    Определяет интервал выплаты funding rate в часах на основе данных
    """
    if len(funding_data) < 2:
        return None
    
    # Сортируем по времени
    sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0))
    
    # Считаем интервалы между выплатами (в миллисекундах)
    intervals_ms = []
    for i in range(1, len(sorted_data)):
        time_diff = sorted_data[i]['calcTime'] - sorted_data[i-1]['calcTime']
        if time_diff > 0:
            intervals_ms.append(time_diff)
    
    if not intervals_ms:
        return None
    
    # Находим медианный интервал
    intervals_ms.sort()
    median_interval = intervals_ms[len(intervals_ms) // 2]
    
    # Переводим в часы
    hours = round(median_interval / (1000 * 3600))
    
    if 1 <= hours <= 24:
        return hours
    else:
        # Проверяем fundingIntervalHours из данных
        if funding_data and 'fundingIntervalHours' in funding_data[0]:
            return funding_data[0].get('fundingIntervalHours')
        return 8  # По умолчанию 8 часов для Aster


def get_current_funding_rate(funding_data):
    """Получает последнюю известную ставку фандинга"""
    if not funding_data:
        return None
    
    # Сортируем по времени и берем последнюю запись
    sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0), reverse=True)
    latest_rate = sorted_data[0].get('lastFundingRate')
    
    if latest_rate:
        try:
            return round(float(latest_rate), 6)
        except (ValueError, TypeError):
            return None
    return None


def get_next_funding_time(funding_data):
    """Определяет время следующей выплаты фандинга"""
    if not funding_data:
        return None
    
    interval_hours = detect_funding_interval(funding_data)
    if not interval_hours:
        return None
    
    sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0))
    last_time = sorted_data[-1]['calcTime']
    last_time_dt = datetime.fromtimestamp(last_time / 1000)
    
    next_time = last_time_dt + timedelta(hours=interval_hours)
    
    return next_time.strftime('%Y-%m-%d %H:%M UTC')


def format_symbol_for_output(symbol: str) -> str:
    """
    Форматирует символ в формат "BASETOKEN/QUOTETOKEN:QUOTETOKEN"
    Например: BTCUSDT -> BTC/USDT:USDT
    """
    if symbol.endswith('USDT'):
        base = symbol[:-4]  # Убираем USDT
        return f"{base}/USDT:USDT"
    return symbol


def process_aster_funding(all_data):
    """
    Обрабатывает все данные по фандингу Aster
    Возвращает словарь в формате:
    {
        "SYMBOL/USDT:USDT": {
            "24h": 0.03,
            "48h": 0.06,
            "168h": 0.21,
            "720h": 1.177439,
            "currentFR": 0.01,
            "fundingIntervalHours": 8,
            "nextFundingTime": null,
            "askTotalVolume": 65950.0,
            "bidTotalVolume": 37290.0
        }
    }
    """
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "168h": int((now - timedelta(hours=168)).timestamp() * 1000),  # 7 дней
        "720h": int((now - timedelta(hours=720)).timestamp() * 1000),  # 30 дней
    }
    end_time_ms = int(now.timestamp() * 1000)
    
    results = {}
    
    print(f"\n{'='*60}")
    print("Обработка данных Aster DEX")
    print(f"Текущее время: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")
    
    for idx, symbol_data in enumerate(all_data, 1):
        if not symbol_data.get('success'):
            continue
        
        original_symbol = symbol_data['symbol']
        funding_history = symbol_data.get('data', [])
        
        if not funding_history:
            print(f"[{idx}/{len(all_data)}] {original_symbol}: ⚠️ Нет данных за 30 дней")
            continue
        
        # Рассчитываем суммы фандинга по периодам
        funding_sums = calculate_funding_summary(funding_history, timestamps, end_time_ms)
        
        # Получаем текущую ставку
        current_fr = get_current_funding_rate(funding_history)
        
        # Определяем интервал выплат
        interval = detect_funding_interval(funding_history)
        
        # Время следующей выплаты
        next_time = get_next_funding_time(funding_history)
        
        # Форматируем символ для вывода
        formatted_symbol = format_symbol_for_output(original_symbol)
        
        results[formatted_symbol] = {
            "24h": funding_sums["24h"],
            "48h": funding_sums["48h"],
            "168h": funding_sums["168h"],
            "720h": funding_sums["720h"],
            "currentFR": current_fr if current_fr is not None else 0,
            "fundingIntervalHours": interval if interval is not None else 8,
            "nextFundingTime": next_time,
            "askTotalVolume": 0,  # TODO: Добавить реальные данные при наличии API стакана
            "bidTotalVolume": 0   # TODO: Добавить реальные данные при наличии API стакана
        }
        
        print(f"[{idx}/{len(all_data)}] {formatted_symbol}: "
              f"30д={funding_sums['720h']:.6f} | "
              f"7д={funding_sums['168h']:.6f} | "
              f"тек={current_fr:.6f} | "
              f"инт={interval}ч")
    
    return results


def save_results(results):
    """Сохраняет результаты в JSON файл в нужном формате"""
    output_file = DATA_DIR / "funding_results_aster.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"✅ Результаты сохранены в: {output_file}")
    print(f"📊 Обработано символов: {len(results)}")
    print(f"{'='*60}")
    
    return output_file


def print_top_10(results, period, period_name):
    """
    Выводит топ-10 символов по указанному периоду
    """
    # Фильтруем символы с данными за период
    valid_symbols = [(symbol, data[period]) for symbol, data in results.items() 
                     if data[period] is not None]
    
    if not valid_symbols:
        print(f"\n❌ Нет данных для периода {period_name}")
        return
    
    # Сортируем по убыванию
    sorted_symbols = sorted(valid_symbols, key=lambda x: x[1], reverse=True)[:10]
    
    print(f"\n{'='*60}")
    print(f"🏆 ТОП-10 ПО НАКОПЛЕННОМУ ФАНДИНГУ ЗА {period_name}")
    print(f"{'='*60}")
    print(f"{'Символ':<20} {'Фандинг %':>12} {'Текущий FR':>12} {'Интервал':>10}")
    print("-"*60)
    
    for symbol, funding_sum in sorted_symbols:
        current_fr = results[symbol].get('currentFR', 0)
        interval = results[symbol].get('fundingIntervalHours', 'N/A')
        
        # Конвертируем в проценты для отображения
        funding_sum_percent = funding_sum * 100
        current_fr_percent = current_fr * 100
        
        print(f"{symbol:<20} {funding_sum_percent:>11.4f}% {current_fr_percent:>11.6f}% {interval:>10}")
    
    # Худшие 5
    negative_symbols = [(symbol, data[period]) for symbol, data in results.items() 
                        if data[period] is not None and data[period] < 0]
    if negative_symbols:
        worst_symbols = sorted(negative_symbols, key=lambda x: x[1])[:5]
        print(f"\n📉 ХУДШИЕ 5 ПО {period_name} (ОТРИЦАТЕЛЬНЫЙ ФАНДИНГ):")
        for symbol, funding_sum in worst_symbols:
            funding_sum_percent = funding_sum * 100
            print(f"   {symbol:<20} {funding_sum_percent:>11.4f}%")


def generate_statistics(results):
    """Генерирует статистику по результатам"""
    if not results:
        return
    
    print(f"\n{'='*60}")
    print("📊 ОБЩАЯ СТАТИСТИКА ПО ФАНДИНГУ ASTER DEX")
    print(f"{'='*60}\n")
    
    # Статистика по каждому периоду (в процентах)
    periods = [
        ("24h", "24 ЧАСА"),
        ("48h", "48 ЧАСОВ"),
        ("168h", "7 ДНЕЙ"),
        ("720h", "30 ДНЕЙ")
    ]
    
    for period_key, period_name in periods:
        valid_data = [data[period_key] * 100 for data in results.values() 
                     if data[period_key] is not None]
        
        if valid_data:
            print(f"📈 {period_name}:")
            print(f"   Средний: {sum(valid_data)/len(valid_data):.4f}%")
            print(f"   Максимальный: {max(valid_data):.4f}%")
            print(f"   Минимальный: {min(valid_data):.4f}%")
            print(f"   Положительных: {len([v for v in valid_data if v > 0])}/{len(valid_data)}")
            print(f"   Отрицательных: {len([v for v in valid_data if v < 0])}/{len(valid_data)}")
            print()
    
    # Статистика по интервалам
    intervals = defaultdict(int)
    for data in results.values():
        interval = data['fundingIntervalHours']
        if interval:
            intervals[interval] += 1
    
    if intervals:
        print(f"⏰ РАСПРЕДЕЛЕНИЕ ПО ИНТЕРВАЛАМ ФАНДИНГА:")
        for interval in sorted(intervals.keys()):
            count = intervals[interval]
            print(f"   {interval:2} часов: {count:3} символов ({count/len(results)*100:.1f}%)")


def main():
    print("🚀 Запуск анализа фандинга Aster DEX")
    print(f"📁 Директория данных: {FUNDING_DATA_DIR}")
    
    # Загружаем данные
    all_data = load_all_funding_data()
    if not all_data:
        return
    
    # Обрабатываем данные
    results = process_aster_funding(all_data)
    
    # Сохраняем результаты в нужном формате
    output_file = save_results(results)
    
    # Выводим топ-10 для разных периодов
    print_top_10(results, "24h", "24 ЧАСА")
    print_top_10(results, "168h", "7 ДНЕЙ")
    print_top_10(results, "720h", "30 ДНЕЙ")
    
    # Генерируем общую статистику
    generate_statistics(results)
    
    print(f"\n✨ Готово! Результаты сохранены в {output_file}")
    print(f"\n📋 Пример записи в файле:")
    # Показываем первый символ как пример
    if results:
        first_symbol = list(results.keys())[0]
        print(json.dumps({first_symbol: results[first_symbol]}, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()