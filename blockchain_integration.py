# blockchain_integration.py
import os
import requests
from bs4 import BeautifulSoup

# ---------- API ключи ----------
CMC_API_KEY = os.environ.get('CMC_API_KEY', '')
# Единый API-ключ для Etherscan V2 (работает для Ethereum, Base, BSC, Polygon и др.)
ETHERSCAN_API_KEY = os.environ.get('ETHERSCAN_API_KEY', '')

# ---------- Конфигурация поддерживаемых сетей ----------
# Для Etherscan V2 API требуется chainid
# Веб-URL используются для парсинга (резервный способ)
SUPPORTED_NETWORKS = {
    'Ethereum': {
        'chain_id': 1,
        'web_url': 'https://etherscan.io/token/{}',
        'name': 'Etherscan'
    },
    'Base': {
        'chain_id': 8453,
        'web_url': 'https://basescan.org/token/{}',
        'name': 'BaseScan'
    },
    'BNB Smart Chain (BEP20)': {
        'chain_id': 56,
        'web_url': 'https://bscscan.com/token/{}',
        'name': 'BscScan'
    },
    'Polygon': {
        'chain_id': 137,
        'web_url': 'https://polygonscan.com/token/{}',
        'name': 'PolygonScan'
    },
    'Arbitrum': {
        'chain_id': 42161,
        'web_url': 'https://arbiscan.io/token/{}',
        'name': 'Arbiscan'
    },
    'Optimism': {
        'chain_id': 10,
        'web_url': 'https://optimistic.etherscan.io/token/{}',
        'name': 'Optimistic Etherscan'
    }
    # При необходимости можно добавить другие сети
}

# ---------- Функции работы с CoinMarketCap ----------
def get_coin_contract_from_cmc(coin_symbol, exchange_prices=None):
    """
    Получает адрес контракта и название сети для монеты из CoinMarketCap.
    Возвращает (contract_address, network_name) или (None, None).
    Если переданы exchange_prices (список цен с бирж), выполняет верификацию по цене.
    """
    if not CMC_API_KEY:
        print("⚠️ CMC API ключ не задан")
        return None, None

    try:
        # Шаг 1: получить ID монеты
        url_map = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'
        headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
        params_map = {'symbol': coin_symbol.upper()}
        resp = requests.get(url_map, headers=headers, params=params_map, timeout=10)
        data = resp.json()
        if not data.get('data'):
            print(f"❌ Монета {coin_symbol} не найдена в CMC")
            return None, None
        coin_id = data['data'][0]['id']

        # Шаг 2: получить метаданные (платформа, адрес контракта)
        url_info = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info'
        params_info = {'id': coin_id}
        resp = requests.get(url_info, headers=headers, params=params_info, timeout=10)
        data = resp.json()
        if not data.get('data') or str(coin_id) not in data['data']:
            print(f"❌ Не удалось получить метаданные для {coin_symbol}")
            return None, None

        coin_info = data['data'][str(coin_id)]
        platform = coin_info.get('platform')
        if not platform or not platform.get('token_address'):
            print(f"ℹ️ У монеты {coin_symbol} нет адреса контракта в CMC (возможно, нативная монета)")
            return None, None

        contract = platform['token_address']
        network = platform.get('name')  # например, 'Ethereum', 'Base', 'BNB Smart Chain (BEP20)'
        if not network:
            print(f"ℹ️ Для {coin_symbol} не указана сеть")
            return None, None

        # Шаг 3: верификация по цене (если переданы цены с бирж)
        if exchange_prices:
            valid_prices = [p for p in exchange_prices if p is not None]
            if valid_prices:
                avg_price = sum(valid_prices) / len(valid_prices)
                url_quotes = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
                params_quotes = {'id': coin_id}
                resp_q = requests.get(url_quotes, headers=headers, params=params_quotes, timeout=10)
                quotes = resp_q.json()
                if quotes.get('data') and str(coin_id) in quotes['data']:
                    cmc_price = quotes['data'][str(coin_id)]['quote']['USD'].get('price')
                    if cmc_price:
                        diff = abs(avg_price - cmc_price) / avg_price
                        if diff <= 0.01:
                            print(f"✓ Цена CMC ({cmc_price}) совпадает с биржевой ({avg_price}) – монета верифицирована")
                        else:
                            print(f"⚠️ Расхождение цены для {coin_symbol}: CMC={cmc_price}, биржи={avg_price} ({diff:.2%})")
                else:
                    print("   Не удалось получить цену CMC для верификации")

        print(f"✓ Найден контракт {contract} для {coin_symbol} на сети {network}")
        return contract, network

    except Exception as e:
        print(f"❌ Ошибка при получении контракта из CMC: {e}")
        return None, None

# ---------- Функции работы с блокчейн-эксплорерами (через API) ----------
def get_token_holders_from_api(contract, network):
    """
    Получает данные о держателях через Etherscan V2 API (единый endpoint для всех сетей).
    Возвращает (holders_count, top10_concentration) или (None, None).
    """
    if not ETHERSCAN_API_KEY:
        return None

    if network not in SUPPORTED_NETWORKS:
        print(f"⚠️ Сеть {network} не поддерживается API")
        return None

    chain_id = SUPPORTED_NETWORKS[network]['chain_id']
    url = "https://api.etherscan.io/v2/api"
    params = {
        'module': 'token',
        'action': 'tokenholderlist',
        'contractaddress': contract,
        'apikey': ETHERSCAN_API_KEY,
        'chainid': chain_id
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        data = response.json()

        if data.get('status') == '1' and data.get('result'):
            holders = data['result']
            if not holders:
                return None

            total_holders = len(holders)
            total_supply = 0
            balances = []

            for holder in holders:
                try:
                    bal = int(holder['TokenHolderQuantity'])
                except (ValueError, TypeError):
                    bal = 0
                balances.append(bal)
                total_supply += bal

            if total_supply == 0:
                return total_holders, 0.0

            balances.sort(reverse=True)
            top10_count = max(1, total_holders // 10)
            top10_supply = sum(balances[:top10_count])
            concentration = (top10_supply / total_supply) * 100

            print(f"   ✅ API: {total_holders} держателей, концентрация топ-10%: {concentration:.2f}%")
            return total_holders, round(concentration, 2)

        else:
            # Возможные ошибки: превышение лимита, отсутствие метода для токена и т.п.
            print(f"   ⚠️ API вернул статус {data.get('status')}: {data.get('message', '')}")
            return None

    except Exception as e:
        print(f"❌ Ошибка API для сети {network}: {e}")
        return None

# ---------- Резервная функция: парсинг веб-страницы (только количество держателей) ----------
def get_token_holders_from_webpage(contract, network):
    """Парсит страницу токена на блокчейн-эксплорере и возвращает количество держателей."""
    if network not in SUPPORTED_NETWORKS:
        return None

    web_url = SUPPORTED_NETWORKS[network]['web_url'].format(contract)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    try:
        response = requests.get(web_url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Поиск блока с информацией о держателях (адаптировано под разные эксплореры)
        holders_text = None
        # Способ 1: ищем dt с текстом "Holders"
        for dt in soup.find_all('dt'):
            if 'Holders' in dt.get_text():
                holders_text = dt.find_next('dd').get_text().strip()
                break

        if not holders_text:
            # Способ 2: ищем div с class='col-md-8' и текстом, содержащим 'Holders'
            for div in soup.find_all('div', class_='col-md-8'):
                if 'Holders' in div.get_text():
                    # Извлекаем число из текста
                    import re
                    match = re.search(r'([\d,]+)', div.get_text())
                    if match:
                        holders_text = match.group(1)
                    break

        if holders_text:
            holders_count = int(holders_text.replace(',', ''))
            print(f"   ✅ Веб-парсинг: {holders_count} держателей")
            return holders_count
        else:
            print(f"   ⚠️ Не удалось найти держателей на странице {web_url}")
            return None

    except Exception as e:
        print(f"❌ Ошибка веб-парсинга для {network}: {e}")
        return None

# ---------- Основная функция, которую вызывает funding_api.py ----------
def get_token_holders(coin_symbol, exchange_prices=None):
    """
    Основной интерфейс для получения данных о держателях.
    Аргументы:
        coin_symbol (str): тикер монеты, например 'AAVE' или 'OPG'
        exchange_prices (list, optional): список цен с бирж для верификации
    Возвращает:
        (holders_count, top10_concentration) или (None, None)
    """
    # 1. Получить контракт и сеть из CMC (с верификацией)
    contract, network = get_coin_contract_from_cmc(coin_symbol, exchange_prices)
    if not contract or not network:
        return None, None

    # 2. Проверить, поддерживается ли сеть
    if network not in SUPPORTED_NETWORKS:
        print(f"ℹ️ Сеть {network} не поддерживается (доступны: {list(SUPPORTED_NETWORKS.keys())})")
        return None, None

    print(f"🔍 Поиск держателей для {coin_symbol} на {network}...")

    # 3. Попробовать получить данные через API (Etherscan V2)
    if ETHERSCAN_API_KEY:
        api_result = get_token_holders_from_api(contract, network)
        if api_result:
            return api_result

    # 4. Резерв: парсинг веб-страницы (только количество)
    holders = get_token_holders_from_webpage(contract, network)
    if holders is not None:
        return holders, None  # концентрацию из веб-парсинга не возвращаем

    print(f"❌ Не удалось получить данные о держателях для {coin_symbol}")
    return None, None