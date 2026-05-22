import asyncio
import subprocess
import sys
from pathlib import Path
import os

BASE_DIR = Path(__file__).parent

SCRIPTS = [
    BASE_DIR / "Aster" / "aster_fetch_funding.py",
    BASE_DIR / "BingX" / "bingx_fetch_funding.py",
    BASE_DIR / "Bybite" / "bybite_fetch_funding.py",
    BASE_DIR / "Hyper" / "hyper_fetch_funding.py",
    BASE_DIR / "KuCoin" / "kucoin_fetch_funding.py",
    BASE_DIR / "MexC" / "mexc_fetch_funding.py",
]

async def run_script(script_path):
    print(f"Запуск {script_path.name}...")
    # Устанавливаем кодировку UTF-8 для вывода Python
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=script_path.parent,
            env=env
        )
        stdout, stderr = await proc.communicate()
        # Декодируем в UTF-8, заменяя ошибки
        out_text = stdout.decode('utf-8', errors='replace')
        err_text = stderr.decode('utf-8', errors='replace')
        
        if proc.returncode == 0:
            print(f"  [OK] {script_path.name} завершён")
            # Показываем последние строки вывода
            lines = out_text.strip().split('\n')
            for line in lines[-3:]:
                if line.strip():
                    print(f"      {line}")
        else:
            print(f"  [ERROR] {script_path.name} завершился с кодом {proc.returncode}")
            if err_text:
                print("      Ошибки:")
                for line in err_text.strip().split('\n')[-10:]:
                    if line.strip():
                        print(f"      {line}")
    except Exception as e:
        print(f"  [ERROR] Не удалось запустить {script_path.name}: {e}")

async def main():
    print("="*60)
    print("Запуск сбора данных по всем биржам ПАРАЛЛЕЛЬНО")
    print("="*60)
    tasks = [run_script(script) for script in SCRIPTS]
    await asyncio.gather(*tasks)
    print("\n[INFO] Все скрипты завершены. Теперь можно запустить Flask API.")

if __name__ == "__main__":
    asyncio.run(main())