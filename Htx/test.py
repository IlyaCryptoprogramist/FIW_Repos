import requests

url = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info?business_type=all"

try:
    # Явно указываем verify=False
    response = requests.get(url, timeout=10, verify=False)
    print("Запрос с verify=False прошёл успешно!")
    print("Статус:", response.status_code)
    print("Ответ (первые 200 символов):", response.text[:200])
except requests.exceptions.RequestException as e:
    print("Ошибка запроса к API HTX даже с verify=False:", e)