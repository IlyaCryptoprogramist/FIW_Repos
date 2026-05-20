import json
import requests
from aster_config import ASTER_CONFIG

# Тестируем запрос для одного символа
url = ASTER_CONFIG["base_url"] + ASTER_CONFIG["endpoints"]["funding_rate_history"]

payload = {
    "symbol": "BTCUSDT",
    "page": 1,
    "rows": 10,
    "sourceCode": ASTER_CONFIG["source_code"]
}

print("📤 Отправляем запрос:")
print(f"URL: {url}")
print(f"Payload: {json.dumps(payload, indent=2)}")
print("\n" + "="*60)

try:
    response = requests.post(
        url, 
        headers=ASTER_CONFIG["headers"],
        json=payload,
        timeout=30
    )
    
    print(f"📥 Статус код: {response.status_code}")
    print(f"📥 Заголовки: {dict(response.headers)}")
    print("\n📥 Тело ответа:")
    
    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])  # Первые 1000 символов
        
        # Проверяем структуру ответа
        print("\n" + "="*60)
        print("🔍 Анализ структуры ответа:")
        print(f"Тип данных: {type(data)}")
        if isinstance(data, dict):
            print(f"Ключи: {list(data.keys())}")
            
            if "data" in data:
                print(f"Данные в 'data': {type(data['data'])}")
                if isinstance(data['data'], list) and len(data['data']) > 0:
                    print(f"Первая запись: {json.dumps(data['data'][0], indent=2, ensure_ascii=False)}")
    else:
        print(f"Ошибка: {response.text[:500]}")
        
except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback
    traceback.print_exc()