import os
import requests

CMC_API_KEY = os.environ.get('CMC_API_KEY', '')
CMC_API_URL = 'https://pro-api.coinmarketcap.com'

def get_coin_id(symbol):
    if not CMC_API_KEY:
        return None
    url = f'{CMC_API_URL}/v1/cryptocurrency/map'
    headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
    params = {'symbol': symbol.upper()}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        data = response.json()
        if data.get('data') and len(data['data']) > 0:
            return data['data'][0]['id']
    except Exception as e:
        print(f"CMC get_coin_id error: {e}")
    return None

def get_coin_data(coin_id):
    if not CMC_API_KEY:
        return None
    url = f'{CMC_API_URL}/v2/cryptocurrency/quotes/latest'
    headers = {'X-CMC_PRO_API_KEY': CMC_API_KEY}
    params = {'id': coin_id, 'convert': 'USD'}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        data = response.json()
        if data.get('data') and str(coin_id) in data['data']:
            coin_info = data['data'][str(coin_id)]
            quote = coin_info['quote']['USD']
            return {
                'maxSupply': coin_info.get('max_supply'),
                'circulatingSupply': coin_info.get('circulating_supply'),
                'price': quote.get('price'),
                'holders': None,
                'topHoldersConcentration': None
            }
    except Exception as e:
        print(f"CMC get_coin_data error: {e}")
    return None

def enrich_with_cmc(coin_symbol, exchange_prices):
    result = {
        'maxSupply': None,
        'circulatingSupply': None,
        'holders': None,
        'topHoldersConcentration': None
    }
    if not CMC_API_KEY:
        return result
    coin_id = get_coin_id(coin_symbol)
    if not coin_id:
        return result
    cmc_data = get_coin_data(coin_id)
    if not cmc_data:
        return result

    valid_prices = [p for p in exchange_prices if p is not None]
    if valid_prices:
        avg_price = sum(valid_prices) / len(valid_prices)
        if cmc_data.get('price'):
            diff = abs(avg_price - cmc_data['price']) / avg_price
            if diff <= 0.01:
                print(f"✓ Верификация CMC для {coin_symbol} пройдена")
            else:
                print(f"⚠️ Расхождение цены CMC для {coin_symbol}: {diff:.2%}")

    result['maxSupply'] = cmc_data.get('maxSupply')
    result['circulatingSupply'] = cmc_data.get('circulatingSupply')
    return result