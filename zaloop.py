import subprocess
import shutil
import time
import logging
import sys
from pathlib import Path

# --- Настройки ---
# Укажите пути к вашим Python-скриптам
SCRIPT1_PATH = "run_all_exchanges.py"
SCRIPT2_PATH = "run_all_top10.py"

# Укажите путь к результирующему JSON файлу (ожидается, что он создается вторым скриптом)
RESULT_JSON_PATH = "result.json"

# Укажите папку-назначение для копирования JSON файла
DESTINATION_FOLDER = r"D:\Ilya\My project\React\funding-rates-app\funding-rates-app\src\data"

# Интервал в секундах между циклами выполнения (N)
INTERVAL_SECONDS = 600  # например, 1 час

# Настройка логирования (опционально, но рекомендуется)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("automation_script.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_path):
    """Запускает Python-скрипт и ожидает его завершения."""
    logger.info(f"Запуск скрипта: {script_path}")
    try:
        # Запуск скрипта через subprocess.run с ожиданием завершения (по умолчанию)
        # stderr=subprocess.PIPE позволяет захватить ошибки, если нужно их обрабатывать
        result = subprocess.run([sys.executable, script_path], 
                                capture_output=True, text=True, check=True)
        logger.info(f"Скрипт успешно завершен: {script_path}")
        # Если нужно, можно логировать stdout/stderr
        # if result.stdout: logger.debug(f"Output from {script_path}: {result.stdout}")
        # if result.stderr: logger.warning(f"Errors from {script_path}: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при выполнении скрипта {script_path}: {e}")
        logger.error(f"stderr: {e.stderr}")
        raise  # Прерываем выполнение цикла при ошибке
    except FileNotFoundError:
        logger.error(f"Файл скрипта не найден: {script_path}")
        raise

def copy_json_file(src_path, dest_folder):
    """Копирует JSON файл из исходной папки в целевую."""
    src_file = Path(src_path)
    dest_dir = Path(dest_folder)

    if not src_file.exists():
        logger.error(f"Результирующий JSON файл не найден: {src_path}")
        raise FileNotFoundError(f"JSON файл {src_path} не существует.")

    if not dest_dir.exists() or not dest_dir.is_dir():
        logger.error(f"Целевая папка не существует или не является папкой: {dest_folder}")
        raise NotADirectoryError(f"Папка назначения {dest_folder} некорректна.")

    try:
        # Создаем папку назначения, если она не существует
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Копируем файл, используя имя из исходного пути
        destination_file = dest_dir / src_file.name
        shutil.copy2(src_file, destination_file) # copy2 сохраняет метаданные
        logger.info(f"JSON файл скопирован: {src_path} -> {destination_file}")
    except Exception as e:
        logger.error(f"Ошибка при копировании файла {src_path} в {dest_folder}: {e}")
        raise

def main():
    """Основной цикл выполнения."""
    logger.info("Запуск автоматизированного процесса.")
    while True:
        try:
            # 1. Запуск первого скрипта
            run_script(SCRIPT1_PATH)
            
            # 2. Запуск второго скрипта
            run_script(SCRIPT2_PATH)

            # 3. Копирование результирующего JSON файла
            copy_json_file(RESULT_JSON_PATH, DESTINATION_FOLDER)

            logger.info(f"Цикл выполнения завершен. Ожидание {INTERVAL_SECONDS} секунд...")
            
        except Exception as e:
            logger.error(f"Произошла ошибка в цикле: {e}. Пропуск цикла, ожидание перед следующей попыткой.")
            # Опционально: можно выполнить break или continue в зависимости от логики
            # Здесь мы просто продолжаем цикл после ошибки после ожидания

        # 4. Ожидание заданного интервала времени перед следующим циклом
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()