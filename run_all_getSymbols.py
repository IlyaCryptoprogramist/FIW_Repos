import asyncio
from common.executor import UniversalExecutor


async def main():
    """Run getSymbols operation across all exchanges."""
    print("[INFO] Запуск операции getSymbols...")
    executor = UniversalExecutor("common/config.json")
    results = await executor.run_operation("getSymbols")
    
    successful_runs = sum(results)
    total_runs = len(results)
    print(f"\n[INFO] Все скрипты getSymbols завершены. Успешно: {successful_runs}/{total_runs}.")


if __name__ == "__main__":
    asyncio.run(main())