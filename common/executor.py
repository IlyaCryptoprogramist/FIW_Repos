import asyncio
import sys
import json
from pathlib import Path
from tqdm.asyncio import tqdm


class UniversalExecutor:
    """
    A universal executor that can run different types of operations across exchanges
    """
    
    def __init__(self, config_path: str = None):
        if config_path:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        else:
            # Default configuration
            self.config = {
                "exchange_directories": ["BingX", "Bybite", "Gate", "Htx", "Hyper", "KuCoin", "MexC"],
                "operations": {
                    "getSymbols": {
                        "file_pattern": "_getSymbols.py",
                        "description": "Fetch trading symbols from exchanges"
                    },
                    "fetch_funding": {
                        "file_pattern": "_fetch_funding.py",
                        "description": "Fetch funding rates from exchanges"
                    },
                    "process_top10": {
                        "file_pattern": "funding_results_*.json",
                        "description": "Process top 10 funding rate data"
                    }
                }
            }
        
        # Get base directory (parent of this file)
        self.base_dir = Path(__file__).parent.parent  # common directory parent
        self.exchange_dirs = [self.base_dir / d for d in self.config["exchange_directories"] if (self.base_dir / d).is_dir()]
    
    async def run_script(self, script_path):
        """Асинхронно запускает один скрипт."""
        print(f"[INFO] Запускаю {script_path}...")
        try:
            # Запускаем скрипт как подпроцесс
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(script_path),
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
    
    async def process_json_file(self, file_path):
        """Обрабатывает один JSON файл с результатами топ-10."""
        exchange_name = file_path.parent.name  # Имя папки как имя биржи
        print(f"[INFO] Обрабатываю файл {file_path} для биржи {exchange_name}...")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Для нового формата файла структура уже содержит все нужные интервалы
            # {
            #   "1INCH/USDT:USDT": {
            #     "24h": -0.2763,
            #     "48h": -0.6167,
            #     "168h": -0.542,
            #     "720h": -0.1264,
            #     ...
            #   },
            #   ...
            # }

            # Сортируем данные по каждому интервалу и берем топ-10
            top_24h = {}
            top_48h = {}
            top_168h = {}
            top_720h = {}

            # Сортировка по каждому интервалу
            if data:
                # Сортировка по 24h (по убыванию)
                sorted_24h = sorted(data.items(), key=lambda item: item[1].get('24h', 0), reverse=True)
                top_24h = {item[0]: item[1] for item in sorted_24h[:10]}

                # Сортировка по 48h (по убыванию)
                sorted_48h = sorted(data.items(), key=lambda item: item[1].get('48h', 0), reverse=True)
                top_48h = {item[0]: item[1] for item in sorted_48h[:10]}

                # Сортировка по 168h (по убыванию)
                sorted_168h = sorted(data.items(), key=lambda item: item[1].get('168h', 0), reverse=True)
                top_168h = {item[0]: item[1] for item in sorted_168h[:10]}

                # Сортировка по 720h (по убыванию)
                sorted_720h = sorted(data.items(), key=lambda item: item[1].get('720h', 0), reverse=True)
                top_720h = {item[0]: item[1] for item in sorted_720h[:10]}

            # Возвращаем данные для этой биржи
            result = {
                "top_10_by_24h": top_24h,
                "top_10_by_48h": top_48h,
                "top_10_by_168h": top_168h,
                "top_10_by_720h": top_720h
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
    
    async def run_operation(self, operation_type: str):
        """
        Runs the specified operation across all exchanges
        :param operation_type: Type of operation to run (getSymbols, fetch_funding, process_top10)
        """
        operation_config = self.config["operations"].get(operation_type)
        if not operation_config:
            print(f"[ERROR] Operation type '{operation_type}' not found in config.")
            return []
        
        file_pattern = operation_config["file_pattern"]
        
        if operation_type == "process_top10":
            # Special handling for JSON processing
            return await self._run_json_processing(file_pattern)
        else:
            # Handle script execution
            return await self._run_script_execution(file_pattern, operation_type)
    
    async def _run_script_execution(self, file_pattern: str, operation_type: str):
        """Run script-based operations."""
        scripts_to_run = []
        
        for exchange_dir in self.exchange_dirs:
            # Remove the underscore from file_pattern to get the filename format
            if file_pattern.startswith("_"):
                expected_script_name = f"{exchange_dir.name.lower()}{file_pattern}"
            else:
                expected_script_name = file_pattern.replace("*", exchange_dir.name.lower())
            
            script_path = exchange_dir / expected_script_name
            if script_path.exists() and script_path.is_file():
                scripts_to_run.append(script_path)
            else:
                print(f"[WARNING] Скрипт не найден в {exchange_dir} по пути: {script_path}")
        
        if not scripts_to_run:
            print(f"[WARNING] Не найдено ни одного скрипта для операции '{operation_type}' для запуска.")
            return []
        
        print(f"[INFO] Найдено {len(scripts_to_run)} скриптов для операции '{operation_type}' для запуска.")

        # Создаём задачи asyncio
        tasks = [self.run_script(script_path) for script_path in scripts_to_run]

        # Используем tqdm.gather для отслеживания прогресса
        results = await tqdm.gather(*tasks, desc=f"Выполнение скриптов {operation_type}", total=len(tasks))

        successful_runs = sum(results)
        total_runs = len(results)
        print(f"\n[INFO] Все скрипты {operation_type} завершены. Успешно: {successful_runs}/{total_runs}.")
        
        return results
    
    async def _run_json_processing(self, file_pattern: str):
        """Run JSON file processing operations."""
        json_files_to_process = []
        
        for exchange_dir in self.exchange_dirs:
            # Ищем файлы по паттерну
            matching_files = list(exchange_dir.glob(file_pattern))
            if matching_files:
                # Берем первый найденный файл, если их несколько
                file_path = matching_files[0]
                json_files_to_process.append(file_path)
            else:
                print(f"[WARNING] Файл по паттерну '{file_pattern}' не найден в {exchange_dir}")

        if not json_files_to_process:
            print(f"[WARNING] Не найдено ни одного JSON-файла по паттерну '{file_pattern}' для обработки.")
            return []
        else:
            print(f"[INFO] Найдено {len(json_files_to_process)} JSON-файлов по паттерну '{file_pattern}' для обработки.")

            # Создаём задачи asyncio
            tasks = [self.process_json_file(file_path) for file_path in json_files_to_process]

            # Используем tqdm.gather для отслеживания прогресса
            results = await tqdm.gather(*tasks, desc="Обработка JSON-файлов топ-10", total=len(tasks))

            # Собираем результаты и считаем успешные
            successful_runs = sum(1 for _, _, success in results if success)
            total_runs = len(results)
            print(f"\n[INFO] Все JSON-файлы топ-10 обработаны. Успешно: {successful_runs}/{total_runs}.")

            return results