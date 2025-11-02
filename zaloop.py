import asyncio
import subprocess
import shutil
import time
import logging
import sys
from pathlib import Path
from common.file_utils import copy_json_file, setup_logging
import json


# --- Настройки ---
# Загружаем конфигурацию
with open("common/config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

# Интервал в секундах между циклами выполнения (N)
INTERVAL_SECONDS = config["zaloop_config"]["interval_seconds"]  # например, 10 минут
DESTINATION_FOLDER = config["zaloop_config"]["destination_folder"]
RESULT_JSON_PATH = config["zaloop_config"]["result_json_path"]

# Настройка логирования
logger = setup_logging()


def run_script_with_subprocess(script_path):
    """Запускает Python-скрипт с помощью subprocess и ожидает его завершения."""
    logger.info(f"Запуск скрипта: {script_path}")
    try:
        # Запуск скрипта через subprocess.run с ожиданием завершения (по умолчанию)
        result = subprocess.run([sys.executable, script_path], 
                                capture_output=True, text=True, check=True)
        logger.info(f"Скрипт успешно завершен: {script_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при выполнении скрипта {script_path}: {e}")
        logger.error(f"stderr: {e.stderr}")
        raise  # Прерываем выполнение цикла при ошибке
    except FileNotFoundError:
        logger.error(f"Файл скрипта не найден: {script_path}")
        raise


async def run_main_orchestrator():
    """Run the main orchestrator using asyncio."""
    logger.info("Запуск главного оркестратора...")
    try:
        # Import and run the main orchestrator
        from main_orchestrator import main
        await main()
        logger.info("Главный оркестратор успешно завершен")
    except Exception as e:
        logger.error(f"Ошибка при выполнении главного оркестратора: {e}")
        raise


def main():
    """Основной цикл выполнения."""
    logger.info("Запуск автоматизированного процесса с новой архитектурой.")
    while True:
        try:
            # 1. Запуск основного оркестратора (вместо отдельных скриптов)
            import asyncio
            asyncio.run(run_main_orchestrator())
            
            # 2. Копирование результирующего JSON файла
            copy_json_file(RESULT_JSON_PATH, DESTINATION_FOLDER)

            logger.info(f"Цикл выполнения завершен. Ожидание {INTERVAL_SECONDS} секунд...")
            
        except Exception as e:
            logger.error(f"Произошла ошибка в цикле: {e}. Пропуск цикла, ожидание перед следующей попыткой.")
            # Опционально: можно выполнить break или continue в зависимости от логики
            # Здесь мы просто продолжаем цикл после ошибки после ожидания

        # 3. Ожидание заданного интервала времени перед следующим циклом
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()