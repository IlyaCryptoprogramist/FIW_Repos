# run_all_exchanges.py
import asyncio
import os
import sys
from pathlib import Path
from tqdm.asyncio import tqdm  # Импортируем tqdm для асинхронных задач

async def run_script(script_path):
    """Асинхронно запускает один скрипт."""
    print(f"[INFO] Запускаю {script_path}...")
    try:
        # Запускаем скрипт как подпроцесс
        process = await asyncio.create_subprocess_exec(
            sys.executable, script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            print(f"[SUCCESS] {script_path} завершён.")
            # print(f"STDOUT {script_path}: {stdout.decode()}") # Включите при необходимости
            return True
        else:
            print(f"[ERROR] {script_path} завершился с кодом {process.returncode}.")
            print(f"STDERR {script_path}: {stderr.decode()}")
            return False

    except Exception as e:
        print(f"[EXCEPTION] Ошибка при запуске {script_path}: {e}")
        return False

async def main():
    # Определяем директорию, где лежат скрипты бирж
    base_dir = Path(__file__).parent  # Текущая директория (FIW_soft)
    exchange_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name != "common"] # Если есть папка "common", исключим её

    # Собираем задачи для всех скриптов
    script_tasks = []
    scripts_to_run = []
    for exchange_dir in exchange_dirs:
        exchange_name = exchange_dir.name
        expected_script_name = f"{exchange_name.lower()}_fetch_funding.py" # Пример: fetch_bingx_funding.py
        script_path = exchange_dir / expected_script_name
        if script_path.exists() and script_path.is_file():
            scripts_to_run.append(str(script_path))
        else:
            print(f"[WARNING] Скрипт не найден в {exchange_dir} по пути: {script_path}")

    if not scripts_to_run:
        print("[WARNING] Не найдено ни одного скрипта для запуска.")
        return

    print(f"[INFO] Найдено {len(scripts_to_run)} скриптов для запуска: {scripts_to_run}")

    # Создаём задачи asyncio
    tasks = [run_script(script_path) for script_path in scripts_to_run]

    # Используем tqdm.gather для отслеживания прогресса
    # tqdm.gather автоматически оборачивает список задач
    results = await tqdm.gather(*tasks, desc="Выполнение скриптов бирж", total=len(tasks))

    successful_runs = sum(results)
    total_runs = len(results)
    print(f"\n[INFO] Все скрипты завершены. Успешно: {successful_runs}/{total_runs}.")

if __name__ == "__main__":
    asyncio.run(main())