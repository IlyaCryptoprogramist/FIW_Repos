import json
import shutil
import logging
import sys
from pathlib import Path
from datetime import datetime


def save_top10_results(all_exchange_data, output_file_name="result.json"):
    """Save top10 results to a JSON file with unique name."""
    base_dir = Path(__file__).parent.parent  # common directory parent
    output_file_path = base_dir / output_file_name
    
    try:
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(all_exchange_data, f, indent=4, ensure_ascii=False)
        print(f"[INFO] Все результаты топ-10 (включая 30 дней) сохранены в: {output_file_path}")
        logging.info(f"Top10 results saved to: {output_file_path}")
        return True
    except Exception as e:
        print(f"[ERROR] Ошибка при сохранении общего файла: {e}")
        logging.error(f"Error saving top10 results: {e}")
        return False


def copy_json_file(src_path, dest_folder):
    """Копирует JSON файл из исходной папки в целевую."""
    src_file = Path(src_path)
    dest_dir = Path(dest_folder)

    if not src_file.exists():
        print(f"[ERROR] Результирующий JSON файл не найден: {src_path}")
        raise FileNotFoundError(f"JSON файл {src_path} не существует.")

    try:
        # Создаем папку назначения, если она не существует
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Копируем файл, используя имя из исходного пути
        destination_file = dest_dir / src_file.name
        shutil.copy2(src_file, destination_file)  # copy2 сохраняет метаданные
        print(f"JSON файл скопирован: {src_path} -> {destination_file}")
        logging.info(f"JSON file copied: {src_path} -> {destination_file}")
        return True
    except Exception as e:
        print(f"[ERROR] Ошибка при копировании файла {src_path} в {dest_folder}: {e}")
        logging.error(f"Error copying file: {e}")
        raise


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("automation_script.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def generate_timestamp():
    """Generate timestamp for unique filenames."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")