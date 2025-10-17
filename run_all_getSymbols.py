# run_all_getSymbols.py
import asyncio
import sys
from pathlib import Path
from tqdm.asyncio import tqdm

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
    # Определяем директорию, где лежат папки бирж
    base_dir = Path(__file__).parent  # Текущая директория (где лежит run_all_getSymbols.py)
    exchange_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name != "common"] # Если есть папка "common", исключим её

    # --- ПОИСК И ЗАПУСК СКРИПТОВ getSymbols ---
    # Предположим, что скрипт для получения символов в каждой папке называется, например, {имя_биржи}_getSymbols.py
    # Ищем файл по шаблону: {имя_папки}_getSymbols.py
    getSymbols_script_tasks = []
    getSymbols_scripts_to_run = []
    for exchange_dir in exchange_dirs:
        exchange_name = exchange_dir.name
        expected_script_name = f"{exchange_name.lower()}_getSymbols.py" # Пример: bingx_getSymbols.py
        script_path = exchange_dir / expected_script_name
        if script_path.exists() and script_path.is_file():
            getSymbols_scripts_to_run.append(str(script_path))
        else:
            print(f"[WARNING] Скрипт getSymbols не найден в {exchange_dir} по пути: {script_path}")

    if not getSymbols_scripts_to_run:
        print("[WARNING] Не найдено ни одного скрипта getSymbols для запуска.")
        return
    else:
        print(f"[INFO] Найдено {len(getSymbols_scripts_to_run)} скриптов getSymbols для запуска.")

        # Создаём задачи asyncio
        tasks = [run_script(script_path) for script_path in getSymbols_scripts_to_run]

        # Используем tqdm.gather для отслеживания прогресса
        results = await tqdm.gather(*tasks, desc="Выполнение скриптов getSymbols", total=len(tasks))

        successful_runs = sum(results)
        total_runs = len(results)
        print(f"\n[INFO] Все скрипты getSymbols завершены. Успешно: {successful_runs}/{total_runs}.")

if __name__ == "__main__":
    asyncio.run(main())