# run_all_top10.py
import asyncio
import json
import sys
from pathlib import Path
from tqdm.asyncio import tqdm
from datetime import datetime

async def process_top10_file(file_path):
    """Обрабатывает один файл top10 и возвращает его содержимое."""
    exchange_name = file_path.parent.name # Имя папки как имя биржи
    print(f"[INFO] Обрабатываю файл {file_path} для биржи {exchange_name}...")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Предполагаем, что структура файла такая:
        # {
        #   "top_10_by_24h": {...},
        #   "top_10_by_48h": {...},
        #   "top_10_by_168h": {...}
        # }
        top_24h = data.get("top_10_by_24h", {})
        top_48h = data.get("top_10_by_48h", {})
        top_168h = data.get("top_10_by_168h", {})

        # Возвращаем данные для этой биржи
        result = {
            "top_10_by_24h": top_24h,
            "top_10_by_48h": top_48h,
            "top_10_by_168h": top_168h
        }
        print(f"[SUCCESS] Файл {file_path} обработан.")
        return exchange_name, result, True

    except FileNotFoundError:
        print(f"[ERROR] Файл {file_path} не найден.")
        return exchange_name, None, False
    except json.JSONDecodeError:
        print(f"[ERROR] Файл {file_path} повреждён или не является JSON.")
        return exchange_name, None, False
    except Exception as e:
        print(f"[EXCEPTION] Ошибка при обработке {file_path}: {e}")
        return exchange_name, None, False

async def main():
    # Определяем директорию, где лежат папки бирж
    base_dir = Path(__file__).parent  # Текущая директория (где лежит run_all_top10.py)
    exchange_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name != "common"] # Если есть папка "common", исключим её

    # --- ПОИСК И ОБРАБОТКА ФАЙЛОВ ТОП-10 ---
    # Ищем файлы по шаблону top10_sorted_funding_results_*.json
    json_files_to_process = []
    for exchange_dir in exchange_dirs:
        # Ищем файл вида top10_sorted_funding_results_{exchange_dir.name.lower()}.json
        expected_file_name = f"top10_sorted_funding_results_{exchange_dir.name.lower()}.json"
        file_path = exchange_dir / expected_file_name
        if file_path.exists() and file_path.is_file():
            json_files_to_process.append(file_path)
        else:
            print(f"[WARNING] Файл топ-10 не найден в {exchange_dir} по пути: {file_path}")

    if not json_files_to_process:
        print("[WARNING] Не найдено ни одного JSON-файла топ-10 для обработки.")
        return
    else:
        print(f"[INFO] Найдено {len(json_files_to_process)} JSON-файлов топ-10 для обработки.")

        # Создаём задачи asyncio
        tasks = [process_top10_file(file_path) for file_path in json_files_to_process]

        # Используем tqdm.gather для отслеживания прогресса
        results = await tqdm.gather(*tasks, desc="Обработка JSON-файлов топ-10", total=len(tasks))

        # Собираем результаты и считаем успешные
        all_exchange_data = {}
        successful_runs = 0
        for exchange_name, data, success in results:
            if success and data is not None:
                all_exchange_data[exchange_name] = data
                successful_runs += 1
            # else: # Если нужно логировать неуспешные, раскомментируйте
            #     print(f"[SKIP] Данные для {exchange_name} не загружены.")

        total_runs = len(results)
        print(f"\n[INFO] Все JSON-файлы топ-10 обработаны. Успешно: {successful_runs}/{total_runs}.")

        # --- СОХРАНЕНИЕ ВСЕХ РЕЗУЛЬТАТОВ В ОДИН ФАЙЛ С УНИКАЛЬНЫМ ИМЕНЕМ ---
        # Генерируем имя файла с текущей датой и временем
        # Формат: YYYYMMDD_HHMMSS (например, 20251016_123045)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_name = f"top10_all_exchanges_{timestamp}.json"
        output_file_path = base_dir / output_file_name
        
        try:
            with open(output_file_path, "w", encoding="utf-8") as f:
                json.dump(all_exchange_data, f, indent=4, ensure_ascii=False)
            print(f"[INFO] Все результаты топ-10 сохранены в: {output_file_path}")
        except Exception as e:
            print(f"[ERROR] Ошибка при сохранении общего файла: {e}")


if __name__ == "__main__":
    asyncio.run(main())