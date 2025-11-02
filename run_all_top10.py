import asyncio
import json
from pathlib import Path
from datetime import datetime
from common.executor import UniversalExecutor
from common.file_utils import save_top10_results


async def main():
    """Process top10 data from all exchanges."""
    print("[INFO] Запуск операции process_top10...")
    executor = UniversalExecutor("common/config.json")
    
    # Run the JSON processing
    results = await executor.run_operation("process_top10")
    
    # Collect results and save
    all_exchange_data = {}
    successful_runs = 0
    for exchange_name, data, success in results:
        if success and data is not None:
            all_exchange_data[exchange_name] = data
            successful_runs += 1

    total_runs = len(results)
    print(f"\n[INFO] Все JSON-файлы топ-10 обработаны. Успешно: {successful_runs}/{total_runs}.")

    # Save all results to a single file with unique name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f"top10_all_exchanges_{timestamp}.json"
    
    # Also save as result.json for compatibility
    save_top10_results(all_exchange_data, "result.json")


if __name__ == "__main__":
    asyncio.run(main())