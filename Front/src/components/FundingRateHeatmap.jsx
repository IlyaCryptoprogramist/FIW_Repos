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
      
      // Нормализуем данные после получения
      const normalizedData = normalizeExchangeData(data.exchanges || {});
      setExchangeData(normalizedData);
      
      // Собираем все уникальные символы
      const allCoins = new Set();
      Object.values(normalizedData).forEach(exchange => {
        Object.keys(exchange).forEach(coin => allCoins.add(coin));
      });
      setAllSymbols(Array.from(allCoins).sort());
    } catch (err) {
      console.error('Error fetching funding data:', err);
      setError(err.message);
      loadMockData();
    } finally {
      setLoading(false);
    }
  };

  // Функция нормализации значений (приведение к десятичному виду 0.01 = 1%)
  const normalizeValue = (value) => {
    if (value === null || value === undefined) return null;
    
    const numValue = parseFloat(value);
    if (isNaN(numValue)) return null;
    
    // Если значение больше 1 или меньше -1, скорее всего это уже проценты
    // Нужно разделить на 100, чтобы получить десятичный вид
    if (Math.abs(numValue) > 1) {
      return numValue / 100;
    }
    
    // Если значение в диапазоне [-1, 1], скорее всего уже в десятичном виде
    return numValue;
  };

  // Нормализация всех данных по всем биржам
  const normalizeExchangeData = (data) => {
    const normalized = {};
    
    for (const [exchange, coins] of Object.entries(data)) {
      normalized[exchange] = {};
      
      for (const [coin, values] of Object.entries(coins)) {
        normalized[exchange][coin] = {};
        
        for (const [key, value] of Object.entries(values)) {
          if (['currentFR', '24h', '48h', '168h', '720h'].includes(key)) {
            normalized[exchange][coin][key] = normalizeValue(value);
          } else {
            normalized[exchange][coin][key] = value;
          }
        }
      }
    }
    
    return normalized;
  };

  const loadMockData = () => {
    const mockData = {
      Binance: {
        BTC: { currentFR: 0.000249, "24h": 0.0300, "168h": 0.2100, "720h": 0.8500 },
        ETH: { currentFR: 0.000128, "24h": 0.0128, "168h": 0.1000, "720h": 0.4000 },
        PROMPT: { currentFR: 0.00125, "24h": -3.015619, "168h": -2.835619, "720h": -2.115799 }
      },
      Mexc: {
        BTC: { currentFR: 0.000249, "24h": 0.0249, "168h": 0.1900, "720h": 0.8000 },
        PROMPT: { currentFR: -0.03, "24h": -2.7558, "168h": -2.5758, "720h": -2.3408 }
      },
      BingX: {
        BTC: { currentFR: 0.000249, "24h": 0.0249, "168h": 0.1900, "720h": 0.8000 },
        PROMPT: { currentFR: -0.0281, "24h": -3.09, "168h": -2.922, "720h": -2.6985 }
      },
      Bybit: {
        BTC: { currentFR: 0.000242, "24h": 0.0242, "168h": 0.1800, "720h": 0.7500 },
        PROMPT: { currentFR: 0.00125, "24h": -3.015619, "168h": -2.835619, "720h": -2.115799 }
      }
    };
    
    const normalizedData = normalizeExchangeData(mockData);
    setExchangeData(normalizedData);
    
    const allCoins = new Set();
    Object.values(normalizedData).forEach(exchange => {
      Object.keys(exchange).forEach(coin => allCoins.add(coin));
    });
    setAllSymbols(Array.from(allCoins).sort());
  };

  const exchangeList = useMemo(() => {
    return Object.keys(exchangeData).sort();
  }, [exchangeData]);

  const getCoinCountForExchange = (exchange) => {
    const exchangeInfo = exchangeData[exchange];
    if (!exchangeInfo) return 0;
    return Object.keys(exchangeInfo).length;
  };

  const getValue = (coin, exchange) => {
    const exchangeInfo = exchangeData[exchange];
    if (!exchangeInfo) return null;
    
    const coinData = exchangeInfo[coin];
    if (!coinData) return null;
    
    const value = coinData[currentPeriod.dataKey];
    return value !== undefined && value !== null ? value : null;
  };

  const sortedSymbols = useMemo(() => {
    if (!sortConfig.exchange || !sortConfig.direction) {
      return allSymbols;
    }

    const coinsWithValues = allSymbols.map(coin => ({
      coin,
      value: getValue(coin, sortConfig.exchange)
    }));

    const sorted = [...coinsWithValues].sort((a, b) => {
      if (a.value === null && b.value === null) return 0;
      if (a.value === null) return 1;
      if (b.value === null) return -1;

      if (sortConfig.direction === 'desc') {
        return a.value - b.value;
      } else {
        return b.value - a.value;
      }
    });

    return sorted.map(item => item.coin);
  }, [sortConfig, allSymbols, period]);

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

  // Форматирование процента - теперь всегда правильное
  const formatPercent = (value) => {
    if (value === null || value === undefined) return '—';
    
    // Значение уже в десятичном виде (0.01 = 1%)
    const percentValue = value * 100;
    
    // Для очень маленьких значений показываем больше знаков
    if (Math.abs(percentValue) < 0.01 && percentValue !== 0) {
      return `${percentValue.toFixed(6)}%`;
    }
    
    // Для обычных значений
    if (Math.abs(percentValue) < 1) {
      return `${percentValue.toFixed(4)}%`;
    }
    
    return `${percentValue.toFixed(2)}%`;
  };

  // Улучшенное определение цвета на основе нормализованного значения
  const getBackgroundColor = (value) => {
    if (value === null || value === undefined) return '#f8f9fa';
    
    // value уже в десятичном виде (0.01 = 1%)
    const percentValue = value * 100;
    const intensity = Math.min(Math.abs(percentValue) / 10, 0.8); // Капитализация интенсивности
    
    if (percentValue > 0) {
      // Положительные значения - зеленые оттенки
      const green = 180 + Math.floor(75 * (1 - intensity));
      return `rgb(144, ${green}, 144)`;
    } else if (percentValue < 0) {
      // Отрицательные значения - красные оттенки
      const red = 180 + Math.floor(75 * (1 - intensity));
      return `rgb(${red}, 144, 144)`;
    }
    return '#ffffff';
  };

  const getSortIcon = (exchange) => {
    if (sortConfig.exchange !== exchange) return ' ↕️';
    if (sortConfig.direction === 'desc') return ' ↓';
    if (sortConfig.direction === 'asc') return ' ↑';
    return ' ↕️';
  };

  const getSortTooltip = (exchange) => {
    if (sortConfig.exchange !== exchange) return 'Нажмите для сортировки по убытку';
    if (sortConfig.direction === 'desc') return 'Сортировка по убытку (от большего убытка). Нажмите для сортировки по прибыли';
    if (sortConfig.direction === 'asc') return 'Сортировка по прибыли (от большей прибыли). Нажмите для сброса';
    return 'Нажмите для сортировки';
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

  return (
    <div className="container-fluid mt-4">
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

      <div className="row mb-3">
        <div className="col">
          <div className="alert alert-info">
            <strong>📊 Статистика:</strong> 
            {' '}{exchangeList.length} бирж, {allSymbols.length} монет
            <button 
              className="btn btn-sm btn-outline-primary ms-3"
              onClick={fetchFundingData}
            >
              🔄 Обновить
            </button>
          </div>
        </div>
      </div>

      <div className="alert alert-warning alert-dismissible fade show mb-3" role="alert">
        <strong>⚠️ Важное примечание:</strong> 
        {' '}Значения ставок финансирования автоматически нормализуются для корректного отображения.
        {' '}Отрицательные значения (красный) = трейдеры платят за шорт, положительные (зеленый) = платят за лонг.
        <button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
      </div>

      <div className="table-responsive">
        <table className="table table-bordered table-hover text-center align-middle">
          <thead className="table-dark">
            <tr>
              <th style={{ minWidth: '80px' }}>Symbol</th>
              {exchangeList.map(ex => {
                const coinCount = getCoinCountForExchange(ex);
                return (
                  <th 
                    key={ex} 
                    onClick={() => handleExchangeClick(ex)}
                    style={{ 
                      cursor: 'pointer', 
                      userSelect: 'none',
                      minWidth: '120px'
                    }}
                    title={getSortTooltip(ex)}
                  >
                    <div>
                      {ex}
                      {getSortIcon(ex)}
                    </div>
                    <small className="d-block text-white-50" style={{ fontSize: '0.7rem' }}>
                      {coinCount} {coinCount === 1 ? 'coin' : 'coins'}
                    </small>
                  </th>
                );
              })}
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
                  const hasData = value !== null;
                  
                  return (
                    <td
                      key={`${coin}-${ex}`}
                      style={{ 
                        backgroundColor: bgColor,
                        transition: 'background-color 0.2s',
                        fontWeight: isSortedColumn ? 'bold' : 'normal',
                        borderLeft: isSortedColumn ? '2px solid #ffc107' : '1px solid #dee2e6',
                        borderRight: isSortedColumn ? '2px solid #ffc107' : '1px solid #dee2e6',
                        opacity: hasData ? 1 : 0.6
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

      <div className="card mt-4">
        <div className="card-body">
          <h6 className="card-title">🎨 Цветовая легенда (нормализованные значения)</h6>
          <div className="d-flex justify-content-around flex-wrap">
            <div className="me-3">
              <span style={{ 
                display: 'inline-block', 
                width: '20px', 
                height: '20px', 
                backgroundColor: 'rgb(240, 144, 144)',
                marginRight: '5px'
              }}></span>
              <span>Сильно отрицательный (убыток) &lt; -5%</span>
            </div>
            <div className="me-3">
              <span style={{ 
                display: 'inline-block', 
                width: '20px', 
                height: '20px', 
                backgroundColor: 'rgb(200, 160, 160)',
                marginRight: '5px'
              }}></span>
              <span>Умеренно отрицательный -1% до -5%</span>
            </div>
            <div className="me-3">
              <span style={{ 
                display: 'inline-block', 
                width: '20px', 
                height: '20px', 
                backgroundColor: 'rgb(180, 180, 180)',
                marginRight: '5px'
              }}></span>
              <span>Нейтральный (-1% до +1%)</span>
            </div>
            <div className="me-3">
              <span style={{ 
                display: 'inline-block', 
                width: '20px', 
                height: '20px', 
                backgroundColor: 'rgb(160, 200, 160)',
                marginRight: '5px'
              }}></span>
              <span>Умеренно положительный +1% до +5%</span>
            </div>
            <div>
              <span style={{ 
                display: 'inline-block', 
                width: '20px', 
                height: '20px', 
                backgroundColor: 'rgb(144, 180, 144)',
                marginRight: '5px'
              }}></span>
              <span>Сильно положительный &gt; +5%</span>
            </div>
          </div>
          <div className="mt-2 text-muted small">
            * Значения автоматически нормализованы: если приходит число больше 1 или меньше -1, оно делится на 100.
            Например: -3.015619 = -3.02% (а не -302%)
          </div>
        </div>
      </div>
    </div>
  );
};

export default FundingRateHeatmap;