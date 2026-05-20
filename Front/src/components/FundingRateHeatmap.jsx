import React, { useState, useEffect, useMemo } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

const API_BASE_URL = 'http://localhost:5000/api';

const FundingRateHeatmap = () => {
  const [period, setPeriod] = useState('current');
  const [exchangeData, setExchangeData] = useState({});
  const [allSymbols, setAllSymbols] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sortConfig, setSortConfig] = useState({ exchange: null, direction: null });

  const periods = [
    { key: 'current', label: 'Current', dataKey: 'currentFR' },
    { key: 'day', label: '1 Day', dataKey: '24h' },
    { key: 'week', label: '7 Day', dataKey: '168h' },
    { key: 'month', label: '30 Day', dataKey: '720h' },
    { key: 'year', label: '1 Year', dataKey: '720h' }
  ];

  const currentPeriod = periods.find(p => p.key === period) || periods[0];

  // Загрузка данных с API
  useEffect(() => {
    fetchFundingData();
  }, []);

  const fetchFundingData = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE_URL}/funding-data`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setExchangeData(data.exchanges || {});
      setAllSymbols(data.symbols || []);
    } catch (err) {
      console.error('Error fetching funding data:', err);
      setError(err.message);
      // Загружаем тестовые данные при ошибке
      loadMockData();
    } finally {
      setLoading(false);
    }
  };

  // Тестовые данные на случай, если API недоступен
  const loadMockData = () => {
    const mockData = {
      Binance: {
        BTC: { currentFR: 0.000249, "24h": 0.0300, "168h": 0.2100, "720h": 0.8500 },
        ETH: { currentFR: 0.000128, "24h": 0.0128, "168h": 0.1000, "720h": 0.4000 },
        SOL: { currentFR: -0.000124, "24h": -0.0124, "168h": -0.0800, "720h": -0.3000 },
        XRP: { currentFR: 0.000204, "24h": 0.0204, "168h": 0.1500, "720h": 0.6000 }
      },
      OKX: {
        BTC: { currentFR: -0.000023, "24h": -0.0023, "168h": -0.0200, "720h": -0.1000 },
        ETH: { currentFR: -0.000070, "24h": -0.0070, "168h": -0.0500, "720h": -0.2000 },
        SOL: { currentFR: -0.000182, "24h": -0.0182, "168h": -0.1200, "720h": -0.5000 },
        XRP: { currentFR: 0.000300, "24h": 0.0300, "168h": 0.2000, "720h": 0.8000 }
      },
      Bybit: {
        BTC: { currentFR: 0.000242, "24h": 0.0242, "168h": 0.1800, "720h": 0.7500 },
        ETH: { currentFR: 0.000198, "24h": 0.0198, "168h": 0.1400, "720h": 0.6000 },
        SOL: { currentFR: 0.000211, "24h": 0.0211, "168h": 0.1600, "720h": 0.6500 },
        XRP: { currentFR: 0.000300, "24h": 0.0300, "168h": 0.2100, "720h": 0.9000 }
      }
    };
    setExchangeData(mockData);
    setAllSymbols(['BTC', 'ETH', 'SOL', 'XRP']);
  };

  // Получение списка бирж из данных
  const exchangeList = useMemo(() => {
    return Object.keys(exchangeData).sort();
  }, [exchangeData]);

  // Получение значения по монете и бирже
  const getValue = (coin, exchange) => {
    const exchangeInfo = exchangeData[exchange];
    if (!exchangeInfo) return null;
    
    const coinData = exchangeInfo[coin];
    if (!coinData) return null;
    
    const value = coinData[currentPeriod.dataKey];
    return value !== undefined && value !== null ? value : null;
  };

  // Сортировка монет
  const sortedSymbols = useMemo(() => {
    if (!sortConfig.exchange || !sortConfig.direction) {
      return allSymbols;
    }

    const coinsWithValues = allSymbols.map(coin => ({
      coin,
      value: getValue(coin, sortConfig.exchange)
    }));

    const sorted = [...coinsWithValues].sort((a, b) => {
      // Обработка null значений
      if (a.value === null && b.value === null) return 0;
      if (a.value === null) return 1;
      if (b.value === null) return -1;

      if (sortConfig.direction === 'desc') {
        // Самые отрицательные сверху
        return a.value - b.value;
      } else {
        // Самые положительные сверху
        return b.value - a.value;
      }
    });

    return sorted.map(item => item.coin);
  }, [sortConfig, allSymbols, period]);

  // Обработчик клика по бирже
  const handleExchangeClick = (exchange) => {
    setSortConfig(prev => {
      if (prev.exchange === exchange) {
        if (prev.direction === 'desc') {
          return { exchange, direction: 'asc' };
        }
        if (prev.direction === 'asc') {
          return { exchange: null, direction: null };
        }
      }
      return { exchange, direction: 'desc' };
    });
  };

  // Форматирование процента
  const formatPercent = (value) => {
    if (value === null || value === undefined) return '—';
    return `${(value * 100).toFixed(4)}%`;
  };

  // Определение цвета фона
  const getBackgroundColor = (value) => {
    if (value === null || value === undefined) return '#f8f9fa';
    const intensity = Math.min(Math.abs(value) * 30, 0.7);
    
    if (value > 0) {
      const green = 180 + Math.floor(75 * (1 - intensity));
      return `rgb(144, ${green}, 144)`;
    } else if (value < 0) {
      const red = 180 + Math.floor(75 * (1 - intensity));
      return `rgb(${red}, 144, 144)`;
    }
    return '#ffffff';
  };

  // Иконка сортировки
  const getSortIcon = (exchange) => {
    if (sortConfig.exchange !== exchange) return ' ↕️';
    if (sortConfig.direction === 'desc') return ' ↓ (убыток)';
    if (sortConfig.direction === 'asc') return ' ↑ (прибыль)';
    return ' ↕️';
  };

  if (loading) {
    return (
      <div className="container mt-5 text-center">
        <div className="spinner-border text-primary" role="status">
          <span className="visually-hidden">Загрузка...</span>
        </div>
        <p className="mt-3">Загрузка данных о финансировании...</p>
      </div>
    );
  }

  if (error && Object.keys(exchangeData).length === 0) {
    return (
      <div className="container mt-5">
        <div className="alert alert-danger" role="alert">
          <h4 className="alert-heading">Ошибка загрузки данных!</h4>
          <p>{error}</p>
          <hr />
          <button className="btn btn-primary" onClick={fetchFundingData}>
            Попробовать снова
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container-fluid mt-4">
      {/* Табы периодов */}
      <ul className="nav nav-tabs mb-3">
        {periods.map(p => (
          <li className="nav-item" key={p.key}>
            <button
              className={`nav-link ${period === p.key ? 'active' : ''}`}
              onClick={() => setPeriod(p.key)}
            >
              {p.label}
            </button>
          </li>
        ))}
      </ul>

      {/* Информация о данных */}
      <div className="row mb-3">
        <div className="col">
          <div className="alert alert-info">
            <strong>📊 Данные:</strong> Загружено {exchangeList.length} бирж и {allSymbols.length} монет
            <button 
              className="btn btn-sm btn-outline-primary ms-3"
              onClick={fetchFundingData}
            >
              Обновить
            </button>
          </div>
        </div>
      </div>

      {/* Пояснение по сортировке */}
      <div className="alert alert-secondary alert-dismissible fade show mb-3" role="alert">
        <strong>ℹ️ Сортировка:</strong> Нажмите на название биржи один раз — убыточные монеты сверху, 
        второй раз — прибыльные монеты сверху, третий раз — сброс.
        <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
      </div>

      {/* Таблица с данными */}
      <div className="table-responsive">
        <table className="table table-bordered table-hover text-center align-middle">
          <thead className="table-dark">
            <tr>
              <th>Symbol</th>
              {exchangeList.map(ex => (
                <th 
                  key={ex} 
                  onClick={() => handleExchangeClick(ex)}
                  style={{ cursor: 'pointer', userSelect: 'none' }}
                  className="position-relative"
                >
                  {ex}{getSortIcon(ex)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedSymbols.map(coin => (
              <tr key={coin}>
                <td className="fw-bold">{coin}</td>
                {exchangeList.map(ex => {
                  const value = getValue(coin, ex);
                  const bgColor = getBackgroundColor(value);
                  const isSortedColumn = sortConfig.exchange === ex;
                  
                  return (
                    <td
                      key={`${coin}-${ex}`}
                      style={{ 
                        backgroundColor: bgColor,
                        transition: 'background-color 0.2s',
                        fontWeight: isSortedColumn ? 'bold' : 'normal',
                        borderLeft: isSortedColumn ? '2px solid #ffc107' : '1px solid #dee2e6',
                        borderRight: isSortedColumn ? '2px solid #ffc107' : '1px solid #dee2e6'
                      }}
                    >
                      {formatPercent(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default FundingRateHeatmap;