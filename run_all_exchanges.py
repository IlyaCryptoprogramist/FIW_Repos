import asyncio
from common.executor import UniversalExecutor


async def main():
    """Run fetch_funding operation across all exchanges."""
    print("[INFO] Запуск операции fetch_funding...")
    executor = UniversalExecutor("common/config.json")
    results = await executor.run_operation("fetch_funding")
    
    successful_runs = sum(results)
    total_runs = len(results)
    print(f"\n[INFO] Все скрипты fetch_funding завершены. Успешно: {successful_runs}/{total_runs}.")


if __name__ == "__main__":
    asyncio.run(main())