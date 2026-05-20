import os
import sys

print("Текущая рабочая директория:", os.getcwd())
print("Путь к скрипту:", sys.argv[0])
print("Директория скрипта:", os.path.dirname(os.path.abspath(sys.argv[0])))

# Проверяем наличие файлов в текущей директории
files = os.listdir('.')
print("\nФайлы в текущей директории:")
for f in files:
    print(f"  {f}")

# Проверяем наличие symbols.json
if os.path.exists('symbols.json'):
    print("\n✅ symbols.json найден!")
else:
    print("\n❌ symbols.json НЕ найден в текущей директории")
    
# Проверяем в директории скрипта
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
if os.path.exists(os.path.join(script_dir, 'symbols.json')):
    print(f"✅ symbols.json найден в {script_dir}")
else:
    print(f"❌ symbols.json НЕ найден в {script_dir}")