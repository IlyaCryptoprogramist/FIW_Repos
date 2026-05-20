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
    total_72h = 0.0
    total_720h = 0.0
    
    for record in funding_data:
        calc_time = record.get('calcTime')
        if not calc_time:
            continue
        
        # Конвертируем fundingRate из строки в число и умножаем на 100 для процентов
        try:
            funding_rate = float(record.get('lastFundingRate', '0')) * 100
        except (ValueError, TypeError):
            continue
        
        if timestamps["24h"] < calc_time < end_time_ms:
            total_24h += funding_rate
        if timestamps["48h"] < calc_time < end_time_ms:
            total_48h += funding_rate
        if timestamps["72h"] < calc_time < end_time_ms:
            total_72h += funding_rate
        if timestamps["720h"] < calc_time < end_time_ms:
            total_720h += funding_rate
    
    return {
        "24h": round(total_24h, 6),
        "48h": round(total_48h, 6),
        "72h": round(total_72h, 6),
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
        if time_diff > 0:  # Только положительные интервалы
            intervals_ms.append(time_diff)
    
    if not intervals_ms:
        return None
    
    # Находим медианный интервал (более устойчив к выбросам)
    intervals_ms.sort()
    median_interval = intervals_ms[len(intervals_ms) // 2]
    
    # Переводим в часы
    hours = round(median_interval / (1000 * 3600))
    
    # Проверяем, что интервал реалистичен (1-24 часа)
    if 1 <= hours <= 24:
        return hours
    else:
        # Если интервал не в ожидаемом диапазоне, пробуем определить по fundingIntervalHours из данных
        if funding_data and 'fundingIntervalHours' in funding_data[0]:
            return funding_data[0].get('fundingIntervalHours')
        return None


def get_current_funding_rate(funding_data):
    """Получает последнюю известную ставку фандинга"""
    if not funding_data:
        return None
    
    # Сортируем по времени и берем последнюю запись
    sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0), reverse=True)
    latest_rate = sorted_data[0].get('lastFundingRate')
    
    if latest_rate:
        try:
            # Конвертируем в проценты
            return round(float(latest_rate) * 100, 6)
        except (ValueError, TypeError):
            return None
    return None


def get_next_funding_time(funding_data):
    """Определяет время следующей выплаты фандинга"""
    if not funding_data:
        return None
    
    # Находим интервал выплат
    interval_hours = detect_funding_interval(funding_data)
    if not interval_hours:
        return None
    
    # Сортируем по времени
    sorted_data = sorted(funding_data, key=lambda x: x.get('calcTime', 0))
    last_time = sorted_data[-1]['calcTime']
    last_time_dt = datetime.fromtimestamp(last_time / 1000)
    
    # Следующее время выплаты
    next_time = last_time_dt + timedelta(hours=interval_hours)
    
    return next_time.strftime('%Y-%m-%d %H:%M UTC')


def get_funding_interval_from_data(funding_data):
    """Получает интервал фандинга из данных API"""
    if funding_data and 'fundingIntervalHours' in funding_data[0]:
        return funding_data[0].get('fundingIntervalHours')
    return detect_funding_interval(funding_data)


def calculate_total_volumes(symbol_data):
    """
    Рассчитывает объемы из стакана (если есть данные)
    Для Aster пока нет API для стакана, оставляем заглушку
    """
    # TODO: Добавить реальные данные из API стакана Aster если необходимо
    return {
        "askTotalVolume": 0,
        "bidTotalVolume": 0
    }


def process_aster_funding(all_data):
    """
    Обрабатывает все данные по фандингу Aster
    """
    now = datetime.now()
    timestamps = {
        "24h": int((now - timedelta(hours=24)).timestamp() * 1000),
        "48h": int((now - timedelta(hours=48)).timestamp() * 1000),
        "72h": int((now - timedelta(hours=72)).timestamp() * 1000),
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
        
        symbol = symbol_data['symbol']
        funding_history = symbol_data.get('data', [])
        
        if not funding_history:
            print(f"[{idx}/{len(all_data)}] {symbol}: ⚠️ Нет данных за 30 дней")
            continue
        
        # Рассчитываем суммы фандинга по периодам
        funding_sums = calculate_funding_summary(funding_history, timestamps, end_time_ms)
        
        # Получаем текущую ставку
        current_fr = get_current_funding_rate(funding_history)
        
        # Определяем интервал выплат
        interval = get_funding_interval_from_data(funding_history)
        
        # Время следующей выплаты
        next_time = get_next_funding_time(funding_history)
        
        # Объемы (пока заглушка)
        volumes = calculate_total_volumes(symbol_data)
        
        results[symbol] = {
            "24h": funding_sums["24h"],
            "48h": funding_sums["48h"],
            "72h": funding_sums["72h"],
            "720h": funding_sums["720h"],
            "currentFR": current_fr,
            "fundingIntervalHours": interval,
            "nextFundingTime": next_time,
            "askTotalVolume": volumes["askTotalVolume"],
            "bidTotalVolume": volumes["bidTotalVolume"],
            "totalRecords": len(funding_history)
        }
        
        print(f"[{idx}/{len(all_data)}] {symbol}: "
              f"30д={funding_sums['720h']:.4f}% | "
              f"3д={funding_sums['72h']:.4f}% | "
              f"тек={current_fr:.6f}% | "
              f"инт={interval}ч | "
              f"записей={len(funding_history)}")
    
    return results


def print_top_10(results, period, period_name):
    """
    Выводит топ-10 символов по указанному периоду
    period: ключ в данных ('24h', '72h', '720h')
    period_name: название для вывода ('24 ЧАСА', '72 ЧАСА', '30 ДНЕЙ')
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
    print(f"{'Символ':<15} {'Фандинг %':>12} {'Текущий FR':>12} {'Интервал':>10} {'Записей':>8}")
    print("-"*65)
    
    for symbol, funding_sum in sorted_symbols:
        current_fr = results[symbol].get('currentFR', 0) or 0
        interval = results[symbol].get('fundingIntervalHours', 'N/A')
        records = results[symbol].get('totalRecords', 0)
        
        print(f"{symbol:<15} {funding_sum:>12.4f}% {current_fr:>12.6f}% {interval:>10} {records:>8}")
    
    # Дополнительно показываем символы с отрицательным фандингом
    negative_symbols = [(symbol, data[period]) for symbol, data in results.items() 
                        if data[period] is not None and data[period] < 0]
    if negative_symbols:
        worst_symbols = sorted(negative_symbols, key=lambda x: x[1])[:5]  # Самые отрицательные
        print(f"\n📉 ХУДШИЕ 5 ПО {period_name} (ОТРИЦАТЕЛЬНЫЙ ФАНДИНГ):")
        for symbol, funding_sum in worst_symbols:
            print(f"   {symbol:<15} {funding_sum:>12.4f}%")


def save_results(results):
    """Сохраняет результаты в JSON файл"""
    output_file = DATA_DIR / "funding_results_aster.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"✅ Результаты сохранены в: {output_file}")
    print(f"📊 Обработано символов: {len(results)}")
    print(f"{'='*60}")
    
    return output_file


def generate_statistics(results):
    """Генерирует статистику по результатам"""
    if not results:
        return
    
    print(f"\n{'='*60}")
    print("📊 ОБЩАЯ СТАТИСТИКА ПО ФАНДИНГУ ASTER DEX")
    print(f"{'='*60}\n")
    
    # Статистика по каждому периоду
    periods = [
        ("24h", "24 ЧАСА"),
        ("72h", "72 ЧАСА (3 ДНЯ)"),
        ("720h", "30 ДНЕЙ")
    ]
    
    for period_key, period_name in periods:
        valid_data = [data[period_key] for data in results.values() 
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
    
    # Символы с полными данными (30 дней)
    full_data_symbols = [s for s, d in results.items() if d['totalRecords'] >= 90]
    print(f"\n✅ Символы с полными данными за 30 дней: {len(full_data_symbols)}/{len(results)}")


def main():
    print("🚀 Запуск анализа фандинга Aster DEX")
    print(f"📁 Директория данных: {FUNDING_DATA_DIR}")
    
    # Загружаем данные
    all_data = load_all_funding_data()
    if not all_data:
        return
    
    # Обрабатываем данные
    results = process_aster_funding(all_data)
    
    # Сохраняем результаты
    output_file = save_results(results)
    
    # Выводим топ-10 для разных периодов
    print_top_10(results, "24h", "24 ЧАСА")
    print_top_10(results, "72h", "72 ЧАСА (3 ДНЯ)")
    print_top_10(results, "720h", "30 ДНЕЙ")
    
    # Генерируем общую статистику
    generate_statistics(results)
    
    print(f"\n✨ Готово! Результаты сохранены в {output_file}")
    print(f"📋 Для сравнения с Bybit запустите: python compare_funding.py")


if __name__ == "__main__":
    main()