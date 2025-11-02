import React, { useState, useEffect } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { useSearchService, SearchResults } from './components/SearchService';

// Динамический импорт данных
let exchangeData = null;
let globalData = null;

// Функция для динамического импорта JSON файлов
const loadDataFiles = async () => {
  try {
    console.log('Начинаем загрузку данных...');
    
    // Ищем файлы в папке data
    const context = require.context('./data/', false, /\.json$/);
    const files = context.keys();
    
    console.log('Найдены файлы:', files);
    
    if (files.length === 0) {
      console.error('Не найдено JSON файлов в папке data');
      return;
    }
    
    // Загружаем все найденные файлы
    const modules = await Promise.all(files.map(file => {
      console.log('Загружаем файл:', file);
      return context(file);
    }));
    
    // Определяем, какой файл является основным, а какой - глобальным
    for (let i = 0; i < files.length; i++) {
      const fileName = files[i];
      const moduleData = modules[i];
      
      console.log(`Обрабатываем файл: ${fileName}`, moduleData);
      
      if (fileName.includes('global')) {
        globalData = moduleData;
        console.log('Назначен как globalData:', globalData);
      } else if (fileName.includes('result')) {
        exchangeData = moduleData;
        console.log('Назначен как exchangeData:', exchangeData);
      }
    }
    
    // Если exchangeData не найден, используем первый файл
    if (!exchangeData && modules.length > 0) {
      exchangeData = modules[0];
      console.log('Используем первый файл как exchangeData:', exchangeData);
    }
    
    console.log('Финальные данные - exchangeData:', exchangeData);
    console.log('Финальные данные - globalData:', globalData);
    
  } catch (error) {
    console.error('Ошибка загрузки данных:', error);
  }
};

const App = () => {
  const { 
    searchResults, 
    searchLoading, 
    searchError, 
    searchCoin, 
    clearSearch 
  } = useSearchService();
  
  const [selectedExchange, setSelectedExchange] = useState('BingX');
  const [selectedPeriod, setSelectedPeriod] = useState('24h');
  const [searchTerm, setSearchTerm] = useState('');
  const [localSearchTerm, setLocalSearchTerm] = useState('');
  const [tableData, setTableData] = useState({
    exchange: null,
    global: null
  });
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('tables');
  const [selectedCoin, setSelectedCoin] = useState(null);
  const [investmentAmount, setInvestmentAmount] = useState(10000);

  // Список доступных бирж
  const [exchanges, setExchanges] = useState(['BingX', 'Gate', 'Htx', 'Hyper', 'KuCoin', 'MexC']);

  // Функция для форматирования периода выплат
  const formatFundingInterval = (hours) => {
    if (!hours && hours !== 0) return 'N/A';
    if (hours === 1) return '1 час';
    if (hours === 2) return '2 часа';
    if (hours === 4) return '4 часа';
    if (hours === 8) return '8 часов';
    if (hours === 24) return '24 часа';
    return `${hours} ч`;
  };

  // Функция для расчета прибыли
const calculateProfit = (coin) => {
  if (!coin || !coin.fundingIntervalHours) return null;
  
  // Используем FR за 24ч для дневной прибыли
  const dailyFR = coin['24h'] || 0;
  const monthlyFR = coin['720h'] || 0;
  const fundingInterval = coin.fundingIntervalHours;
  const amount = investmentAmount;
  
  // Расчет выплат в день
  const payoutsPerDay = 24 / fundingInterval;
  
  // Расчет ставок на основе разных периодов
  const dailyRate = dailyFR; // На основе FR за 24ч
  const monthlyRate = monthlyFR; // Используем FR за 720ч
  const yearlyRate = monthlyRate * 12; // Годовая ставка
  
  // Прибыль в валюте
  const dailyProfit = amount * (dailyRate) / 100;
  const monthlyProfit = amount * (monthlyRate) / 100;
  const yearlyProfit = amount * (yearlyRate) / 100;
  
  return {
    payoutsPerDay,
    dailyRate: dailyRate,
    monthlyRate: monthlyRate,
    yearlyRate: yearlyRate,
    dailyProfit,
    monthlyProfit,
    yearlyProfit
  };
};

  // Обработчик клика по монете
  const handleCoinClick = (coin, exchange = null) => {
    const coinWithExchange = { ...coin, exchange };
    setSelectedCoin(coinWithExchange);
    setActiveTab('calculator');
  };

  // Загрузка данных при монтировании компонента
  useEffect(() => {
    const loadData = async () => {
      try {
        console.log('Запуск загрузки данных в компоненте...');
        await loadDataFiles();
        
        console.log('Устанавливаем данные в состояние:', {
          exchange: exchangeData,
          global: globalData || exchangeData
        });
        
        setTableData({
          exchange: exchangeData,
          global: globalData || exchangeData
        });
        
        // Обновляем список бирж из загруженных данных
        if (exchangeData) {
          const availableExchanges = Object.keys(exchangeData);
          console.log('Доступные биржи:', availableExchanges);
          setExchanges(availableExchanges);
          
          // Если выбранная биржа не существует в данных, выбираем первую доступную
          if (!availableExchanges.includes(selectedExchange) && availableExchanges.length > 0) {
            setSelectedExchange(availableExchanges[0]);
          }
        }
        
        setLoading(false);
        console.log('Загрузка данных завершена');
        
      } catch (error) {
        console.error('Error loading data:', error);
        setLoading(false);
      }
    };

    loadData();
  }, []);

  // Получение данных для выбранной биржи и периода
  const getExchangeTableData = () => {
    console.log('getExchangeTableData вызвана:', {
      selectedExchange,
      selectedPeriod,
      tableData: tableData.exchange
    });
    
    if (!tableData.exchange || !tableData.exchange[selectedExchange]) {
      console.log('Нет данных для биржи:', selectedExchange);
      return [];
    }
    
    const periodKey = `top_10_by_${selectedPeriod}`;
    const exchangeDataForPeriod = tableData.exchange[selectedExchange][periodKey];
    
    console.log('Данные для периода:', periodKey, exchangeDataForPeriod);
    
    if (!exchangeDataForPeriod) {
      console.log('Нет данных для периода:', periodKey);
      return [];
    }
    
    const result = Object.entries(exchangeDataForPeriod).map(([symbol, coinData]) => ({
      symbol,
      ...coinData
    }));
    
    console.log('Результат getExchangeTableData:', result);
    return result;
  };

  // Получение глобальных данных для выбранного периода
  const getGlobalTableData = () => {
    console.log('getGlobalTableData вызвана:', {
      selectedPeriod,
      tableData: tableData.global
    });
    
    if (!tableData.global) {
      console.log('Нет глобальных данных');
      return [];
    }
    
    const periodKey = `top_10_by_${selectedPeriod}`;
    const allCoins = [];
    
    // Собираем данные со всех бирж
    Object.entries(tableData.global).forEach(([exchange, exchangeData]) => {
      const exchangeDataForPeriod = exchangeData[periodKey];
      if (exchangeDataForPeriod) {
        Object.entries(exchangeDataForPeriod).forEach(([symbol, coinData]) => {
          allCoins.push({
            symbol,
            exchange,
            ...coinData
          });
        });
      }
    });
    
    // Сортируем по накопленному funding rate за выбранный период (по убыванию) и берем топ-10
    const result = allCoins
      .sort((a, b) => b[selectedPeriod] - a[selectedPeriod])
      .slice(0, 10);
    
    console.log('Результат getGlobalTableData:', result);
    return result;
  };

  // Фильтрация данных по поисковому запросу
  const filterDataBySearch = (data) => {
    if (!searchTerm.trim()) return data;
    
    const searchLower = searchTerm.toLowerCase().trim();
    return data.filter(coin => 
      coin.symbol.toLowerCase().includes(searchLower)
    );
  };

  // Обработчик поиска по API
  const handleApiSearch = (e) => {
    e.preventDefault();
    if (localSearchTerm.trim()) {
      searchCoin(localSearchTerm);
    }
  };

  const exchangeTableData = filterDataBySearch(getExchangeTableData());
  const globalTableData = filterDataBySearch(getGlobalTableData());
  const profitData = selectedCoin ? calculateProfit(selectedCoin) : null;

  // Отладочная информация
  useEffect(() => {
    console.log('Текущее состояние:', {
      loading,
      tableData,
      selectedExchange,
      selectedPeriod,
      exchangeTableData,
      globalTableData
    });
  }, [loading, tableData, selectedExchange, selectedPeriod, exchangeTableData, globalTableData]);

  if (loading) {
    return (
      <div className="container mt-4">
        <div className="d-flex justify-content-center align-items-center" style={{ height: '50vh' }}>
          <div className="text-center">
            <div className="spinner-border text-primary" role="status" style={{ width: '3rem', height: '3rem' }}>
              <span className="visually-hidden">Загрузка...</span>
            </div>
            <p className="mt-3">Загрузка данных...</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mt-4">
      <h1 className="text-center mb-4 text-primary">Funding Rates Analytics</h1>
      
      {/* Навигационные вкладки */}
      <ul className="nav nav-tabs mb-4" id="mainTabs" role="tablist">
        <li className="nav-item" role="presentation">
          <button
            className={`nav-link ${activeTab === 'tables' ? 'active' : ''}`}
            id="tables-tab"
            type="button"
            role="tab"
            onClick={() => setActiveTab('tables')}
          >
            📊 Топ-10 монет
          </button>
        </li>
        <li className="nav-item" role="presentation">
          <button
            className={`nav-link ${activeTab === 'search' ? 'active' : ''}`}
            id="search-tab"
            type="button"
            role="tab"
            onClick={() => setActiveTab('search')}
          >
            🔍 Поиск по монетам
          </button>
        </li>
        <li className="nav-item" role="presentation">
          <button
            className={`nav-link ${activeTab === 'calculator' ? 'active' : ''}`}
            id="calculator-tab"
            type="button"
            role="tab"
            onClick={() => setActiveTab('calculator')}
          >
            🧮 Калькулятор прибыли
          </button>
        </li>
      </ul>

      {/* Содержимое вкладки "Топ-10 монет" */}
      {activeTab === 'tables' && (
        <div className="tab-pane fade show active">
          {/* Фильтры для таблиц */}
          <div className="row mb-4">
            <div className="col-md-4">
              <div className="mb-3">
                <label htmlFor="exchangeSelect" className="form-label fw-bold">Выберите биржу:</label>
                <select 
                  id="exchangeSelect"
                  className="form-select"
                  value={selectedExchange}
                  onChange={(e) => setSelectedExchange(e.target.value)}
                >
                  {exchanges.map(exchange => (
                    <option key={exchange} value={exchange}>{exchange}</option>
                  ))}
                </select>
              </div>
            </div>
            <div className="col-md-4">
              <div className="mb-3">
                <label htmlFor="periodSelect" className="form-label fw-bold">Выберите период:</label>
                <select 
                  id="periodSelect"
                  className="form-select"
                  value={selectedPeriod}
                  onChange={(e) => setSelectedPeriod(e.target.value)}
                >
                  <option value="24h">24 часа</option>
                  <option value="48h">48 часов</option>
                  <option value="168h">1 неделя</option>
                  <option value="720h">30 дней</option>
                </select>
              </div>
            </div>
            <div className="col-md-4">
              <div className="mb-3">
                <label htmlFor="searchInput" className="form-label fw-bold">Поиск в топе:</label>
                <div className="input-group">
                  <input
                    id="searchInput"
                    type="text"
                    className="form-control"
                    placeholder="Фильтр по названию..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                  {searchTerm && (
                    <button
                      className="btn btn-outline-secondary"
                      type="button"
                      onClick={() => setSearchTerm('')}
                    >
                      ×
                    </button>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Первая таблица - данные по выбранной бирже */}
          <div className="card mb-5 shadow-sm">
            <div className="card-header bg-primary text-white d-flex justify-content-between align-items-center">
              <h5 className="card-title mb-0">
                📊 Топ-10 монет на {selectedExchange} 
                <small className="ms-2">
                  {selectedPeriod === '24h' ? '24 часа' : 
                   selectedPeriod === '48h' ? '48 часов' : 
                   selectedPeriod === '168h' ? '1 неделя' : 
                   '30 дней'}
                </small>
              </h5>
              <span className="badge bg-light text-dark">
                {exchangeTableData.length} монет
              </span>
            </div>
            <div className="card-body p-0">
              {exchangeTableData.length > 0 ? (
                <div className="table-responsive">
                  <table className="table table-striped table-hover mb-0">
                    <thead className="table-dark">
                      <tr>
                        <th scope="col" className="ps-3">Монета</th>
                        <th scope="col">Накопленный Funding Rate</th>
                        <th scope="col">Текущий Funding Rate</th>
                        <th scope="col">Период выплат</th>
                        <th scope="col">Объем Ask</th>
                        <th scope="col" className="pe-3">Объем Bid</th>
                      </tr>
                    </thead>
                    <tbody>
                      {exchangeTableData.map((coin, index) => (
                        <tr key={`${coin.symbol}-${index}`}>
                          <td className="fw-bold ps-3">
                            <button
                              className="btn btn-link p-0 text-decoration-none fw-bold text-dark"
                              onClick={() => handleCoinClick(coin, selectedExchange)}
                              title="Рассчитать прибыль"
                            >
                              {coin.symbol}
                            </button>
                          </td>
                          <td className={coin[selectedPeriod] >= 0 ? 'text-success' : 'text-danger'}>
                            {coin[selectedPeriod]?.toFixed(4) || 'N/A'}
                          </td>
                          <td className={coin.currentFR >= 0 ? 'text-success' : 'text-danger'}>
                            {coin.currentFR?.toFixed(4) || 'N/A'}
                          </td>
                          <td>
                            <span className="badge bg-info text-dark">
                              {formatFundingInterval(coin.fundingIntervalHours)}
                            </span>
                          </td>
                          <td>{coin.askTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
                          <td className="pe-3">{coin.bidTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center py-5">
                  <p className="text-muted">
                    {searchTerm 
                      ? `Не найдено монет по запросу "${searchTerm}" для биржи ${selectedExchange}`
                      : `Нет данных для выбранной биржи и периода`
                    }
                  </p>
                  <div className="mt-3">
                    <small className="text-muted">
                      Доступные биржи: {exchanges.join(', ')}
                    </small>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Вторая таблица - глобальные данные */}
          <div className="card shadow-sm">
            <div className="card-header bg-success text-white d-flex justify-content-between align-items-center">
              <h5 className="card-title mb-0">
                🌍 Топ-10 монет со всех бирж 
                <small className="ms-2">
                  {selectedPeriod === '24h' ? '24 часа' : 
                   selectedPeriod === '48h' ? '48 часов' : 
                   selectedPeriod === '168h' ? '1 неделя' : 
                   '30 дней'}
                </small>
              </h5>
              <span className="badge bg-light text-dark">
                {globalTableData.length} монет
              </span>
            </div>
            <div className="card-body p-0">
              {globalTableData.length > 0 ? (
                <div className="table-responsive">
                  <table className="table table-striped table-hover mb-0">
                    <thead className="table-dark">
                      <tr>
                        <th scope="col" className="ps-3">Монета</th>
                        <th scope="col">Биржа</th>
                        <th scope="col">Накопленный Funding Rate</th>
                        <th scope="col">Текущий Funding Rate</th>
                        <th scope="col">Период выплат</th>
                        <th scope="col">Объем Ask</th>
                        <th scope="col" className="pe-3">Объем Bid</th>
                      </tr>
                    </thead>
                    <tbody>
                      {globalTableData.map((coin, index) => (
                        <tr key={`${coin.symbol}-${coin.exchange}-${index}`}>
                          <td className="fw-bold ps-3">
                            <button
                              className="btn btn-link p-0 text-decoration-none fw-bold text-dark"
                              onClick={() => handleCoinClick(coin, coin.exchange)}
                              title="Рассчитать прибыль"
                            >
                              {coin.symbol}
                            </button>
                          </td>
                          <td>
                            <span className="badge bg-primary">{coin.exchange}</span>
                          </td>
                          <td className={coin[selectedPeriod] >= 0 ? 'text-success' : 'text-danger'}>
                            {coin[selectedPeriod]?.toFixed(4) || 'N/A'}
                          </td>
                          <td className={coin.currentFR >= 0 ? 'text-success' : 'text-danger'}>
                            {coin.currentFR?.toFixed(4) || 'N/A'}
                          </td>
                          <td>
                            <span className="badge bg-info text-dark">
                              {formatFundingInterval(coin.fundingIntervalHours)}
                            </span>
                          </td>
                          <td>{coin.askTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
                          <td className="pe-3">{coin.bidTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center py-5">
                  <p className="text-muted">
                    {searchTerm 
                      ? `Не найдено монет по запросу "${searchTerm}" в глобальных данных`
                      : `Нет глобальных данных для выбранного периода`
                    }
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Содержимое вкладки "Поиск по монетам" */}
      {activeTab === 'search' && (
        <div className="tab-pane fade show active">
          {/* Форма поиска в карточке */}
          <div className="card mb-4 shadow-sm">
            <div className="card-header bg-warning text-dark">
              <h5 className="card-title mb-0">🔍 Поиск монеты по всем биржам</h5>
            </div>
            <div className="card-body">
              <form onSubmit={handleApiSearch}>
                <div className="row align-items-end">
                  <div className="col-md-8">
                    <div className="mb-3">
                      <label htmlFor="coinSearchInput" className="form-label fw-bold">Название монеты:</label>
                      <div className="input-group">
                        <input
                          id="coinSearchInput"
                          type="text"
                          className="form-control"
                          placeholder="Введите название монеты (например: BTC, ETH, USDT)..."
                          value={localSearchTerm}
                          onChange={(e) => setLocalSearchTerm(e.target.value)}
                        />
                        {localSearchTerm && (
                          <button
                            className="btn btn-outline-secondary"
                            type="button"
                            onClick={() => setLocalSearchTerm('')}
                          >
                            ×
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                  <div className="col-md-4">
                    <div className="mb-3">
                      <button 
                        className="btn btn-primary w-100" 
                        type="submit"
                        disabled={searchLoading}
                      >
                        {searchLoading ? (
                          <>
                            <span className="spinner-border spinner-border-sm me-2" role="status"></span>
                            Поиск...
                          </>
                        ) : (
                          'Найти на всех биржах'
                        )}
                      </button>
                    </div>
                  </div>
                </div>
              </form>

              {searchError && (
                <div className="alert alert-danger mt-3">
                  {searchError}
                </div>
              )}
            </div>
          </div>

          {/* Результаты поиска в стиле таблиц */}
          {searchResults && (
            <div className="search-results">
              {/* Заголовок результатов */}
              <div className="card mb-4">
                <div className="card-header bg-info text-white d-flex justify-content-between align-items-center">
                  <h5 className="card-title mb-0">
                    📋 Результаты поиска для "{searchResults.coin}"
                  </h5>
                  <div>
                    <span className="badge bg-light text-dark me-2">
                      {searchResults.total_matches} совпадений
                    </span>
                    <button 
                      className="btn btn-sm btn-light"
                      onClick={clearSearch}
                    >
                      × Очистить
                    </button>
                  </div>
                </div>
              </div>

              {/* Таблицы по биржам */}
{Object.entries(searchResults.results).map(([exchange, pairs]) => (
  <div key={exchange} className="card mb-4 shadow-sm">
    <div className="card-header bg-secondary text-white d-flex justify-content-between align-items-center">
      <h6 className="card-title mb-0">
        {exchange}
      </h6>
      <span className="badge bg-light text-dark">
        {Object.keys(pairs).length} пар
      </span>
    </div>
    <div className="card-body p-0">
      <div className="table-responsive">
        <table className="table table-striped table-hover mb-0">
          <thead className="table-dark">
            <tr>
              <th scope="col" className="ps-3">Торговая пара</th>
              <th scope="col">Текущий FR</th>
              <th scope="col">24ч FR</th>
              <th scope="col">48ч FR</th>
              <th scope="col">168ч FR</th>
              <th scope="col">720ч FR</th>
              <th scope="col">Период выплат</th>
              <th scope="col">Объем Ask</th>
              <th scope="col" className="pe-3">Объем Bid</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(pairs).map(([pair, data]) => (
              <tr key={pair}>
                <td className="fw-bold ps-3">
                  <button
                    className="btn btn-link p-0 text-decoration-none fw-bold text-dark"
                    onClick={() => handleCoinClick(data, exchange)}
                    title="Рассчитать прибыль"
                  >
                    {pair}
                  </button>
                </td>
                <td className={data.currentFR >= 0 ? 'text-success' : 'text-danger'}>
                  {typeof data.currentFR === 'number' ? data.currentFR.toFixed(4) : 'N/A'}
                </td>
                <td className={data['24h'] >= 0 ? 'text-success' : 'text-danger'}>
                  {typeof data['24h'] === 'number' ? data['24h'].toFixed(4) : 'N/A'}
                </td>
                <td className={data['48h'] >= 0 ? 'text-success' : 'text-danger'}>
                  {typeof data['48h'] === 'number' ? data['48h'].toFixed(4) : 'N/A'}
                </td>
                <td className={data['168h'] >= 0 ? 'text-success' : 'text-danger'}>
                  {typeof data['168h'] === 'number' ? data['168h'].toFixed(4) : 'N/A'}
                </td>
                <td className={data['720h'] >= 0 ? 'text-success' : 'text-danger'}>
                  {typeof data['720h'] === 'number' ? data['720h'].toFixed(4) : 'N/A'}
                </td>
                <td>
                  <span className="badge bg-info text-dark">
                    {data.fundingIntervalHours ? formatFundingInterval(data.fundingIntervalHours) : 'N/A'}
                  </span>
                </td>
                <td>{data.askTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
                <td className="pe-3">{data.bidTotalVolume?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || 'N/A'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  </div>
))}

              {/* Сообщение если нет результатов */}
              {searchResults.total_matches === 0 && (
                <div className="card">
                  <div className="card-body text-center py-5">
                    <p className="text-muted mb-0">
                      Монета "{searchResults.coin}" не найдена на подключенных биржах.
                    </p>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Информация о поиске */}
          {!searchResults && (
            <div className="card">
              <div className="card-body text-center py-5">
                <p className="text-muted mb-3">
                  🔍 Введите название монеты для поиска по всем биржам
                </p>
                <small className="text-muted">
                  Поиск использует данные со всех подключенных бирж в реальном времени
                </small>
              </div>
            </div>
          )}
        </div>
      )}

     {/* Содержимое вкладки "Калькулятор прибыли" */}
{activeTab === 'calculator' && (
  <div className="tab-pane fade show active">
    <div className="card shadow-sm">
      <div className="card-header bg-info text-white">
        <h5 className="card-title mb-0">🧮 Калькулятор прибыли</h5>
      </div>
      <div className="card-body">
        {selectedCoin ? (
          <>
            {/* Информация о выбранной монете */}
            <div className="row mb-4">
              <div className="col-md-6">
                <h6>Информация о монете:</h6>
                <table className="table table-sm table-bordered">
                  <tbody>
                    <tr>
                      <td className="fw-bold">Монета:</td>
                      <td>{selectedCoin.symbol}</td>
                    </tr>
                    <tr>
                      <td className="fw-bold">Биржа:</td>
                      <td>
                        <span className="badge bg-primary">{selectedCoin.exchange}</span>
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">Текущий FR (24ч):</td>
                      <td className={selectedCoin.currentFR >= 0 ? 'text-success' : 'text-danger'}>
                        {selectedCoin.currentFR?.toFixed(4) || 'N/A'}
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">Период выплат:</td>
                      <td>
                        <span className="badge bg-info text-dark">
                          {formatFundingInterval(selectedCoin.fundingIntervalHours)}
                        </span>
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">24ч FR:</td>
                      <td className={selectedCoin['24h'] >= 0 ? 'text-success' : 'text-danger'}>
                        {selectedCoin['24h']?.toFixed(4) || 'N/A'} <small className="text-muted">(за день)</small>
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">48ч FR:</td>
                      <td>
                        {selectedCoin['48h']?.toFixed(4) || 'N/A'} <small className="text-muted">(за 2 дня)</small>
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">168ч FR:</td>
                      <td>
                        {selectedCoin['168h']?.toFixed(4) || 'N/A'} <small className="text-muted">(за неделю)</small>
                      </td>
                    </tr>
                    <tr>
                      <td className="fw-bold">720ч FR:</td>
                      <td className={selectedCoin['720h'] >= 0 ? 'text-success' : 'text-danger'}>
                        {selectedCoin['720h']?.toFixed(4) || 'N/A'} <small className="text-muted">(за месяц)</small>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              
              {/* Поле для ввода суммы */}
              <div className="col-md-6">
                <div className="mb-4">
                  <label htmlFor="investmentAmount" className="form-label fw-bold">
                    Сумма инвестиции ($):
                  </label>
                  <div className="input-group">
                    <span className="input-group-text">$</span>
                    <input
                      id="investmentAmount"
                      type="number"
                      className="form-control"
                      value={investmentAmount}
                      onChange={(e) => setInvestmentAmount(Number(e.target.value))}
                      min="0"
                      step="100"
                    />
                  </div>
                  <small className="text-muted">
                    Введите сумму для расчета потенциальной прибыли
                  </small>
                </div>

                {/* Кнопка для возврата */}
                <button
                  className="btn btn-outline-secondary"
                  onClick={() => setActiveTab('tables')}
                >
                  ← Назад к таблицам
                </button>
              </div>
            </div>

           {/* Результаты расчета */}
{profitData && (
  <div className="row">
    <div className="col-12">
      <h6 className="border-bottom pb-2">Результаты расчета:</h6>
      <div className="table-responsive">
        <table className="table table-bordered table-hover">
          <thead className="table-light">
            <tr>
              <th>Период</th>
              <th>Процентная ставка</th>
              <th>Прибыль ($)</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td className="fw-bold">В день</td>
              <td className="text-success fw-bold">{profitData.dailyRate.toFixed(4)}%</td>
              <td className="text-success fw-bold">${profitData.dailyProfit.toFixed(2)}</td>
            </tr>
            <tr>
              <td className="fw-bold">В месяц (30 дней)</td>
              <td className="text-success fw-bold">{profitData.monthlyRate.toFixed(4)}%</td>
              <td className="text-success fw-bold">${profitData.monthlyProfit.toFixed(2)}</td>
            </tr>
            <tr>
              <td className="fw-bold">В год (12 месяцев)</td>
              <td className="text-success fw-bold">{profitData.yearlyRate.toFixed(4)}%</td>
              <td className="text-success fw-bold">${profitData.yearlyProfit.toFixed(2)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
)}
          </>
        ) : (
          <div className="text-center py-5">
            <p className="text-muted mb-3">
              🎯 Выберите монету из таблиц для расчета прибыли
            </p>
            <button
              className="btn btn-primary"
              onClick={() => setActiveTab('tables')}
            >
              Перейти к таблицам
            </button>
          </div>
        )}
      </div>
    </div>
  </div>
)}

      {/* Общая информация о данных */}
      <div className="mt-4 p-3 bg-light rounded">
        <small className="text-muted">
          <strong>Примечание:</strong> Funding Rate отображается в десятичном формате. Положительные значения выделены зеленым, отрицательные - красным.
          <br />
          <strong>Расчет прибыли:</strong> День - на основе текущего FR (24ч), Месяц - на основе FR за 720ч (30 дней), Год - на основе FR за 720ч × 12.
        </small>
      </div>
    </div>
  );
};

export default App;