// FundingRateHeatmap.js
import React, { useState, useEffect, useMemo, useRef } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

const API_BASE_URL = 'http://localhost:5000/api';

const FundingRateHeatmap = () => {
  const [period, setPeriod] = useState('24h');
  const [exchangeData, setExchangeData] = useState({});
  const [allSymbols, setAllSymbols] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sortConfig, setSortConfig] = useState({ exchange: null, direction: null });
  const [firstColumnWidth, setFirstColumnWidth] = useState(120);
  const [isResizing, setIsResizing] = useState(false);
  const [searchTerm, setSearchTerm] = useState(''); // состояние поиска

  const startXRef = useRef(0);
  const startWidthRef = useRef(0);

  const periods = [
    { key: 'currentFR', label: 'Current', dataKey: 'currentFR' },
    { key: '24h', label: '1 Day', dataKey: '24h' },
    { key: '168h', label: '7 Day', dataKey: '168h' },
    { key: '720h', label: '30 Day', dataKey: '720h' }
  ];

  const currentPeriod = periods.find(p => p.key === period) || periods[0];

  useEffect(() => {
    fetchFundingData();
  }, []);

  useEffect(() => {
    const handleMouseMove = (e) => {
      if (!isResizing) return;
      const delta = e.clientX - startXRef.current;
      const newWidth = startWidthRef.current + delta;
      if (newWidth >= 50) setFirstColumnWidth(newWidth);
    };
    const handleMouseUp = () => setIsResizing(false);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);

  const handleResizeStart = (e) => {
    e.preventDefault();
    startXRef.current = e.clientX;
    startWidthRef.current = firstColumnWidth;
    setIsResizing(true);
  };

  const fetchFundingData = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/funding-data`);
      const data = await response.json();
      setExchangeData(data.exchanges || {});
      const allCoins = new Set();
      Object.values(data.exchanges || {}).forEach(exchange => {
        Object.keys(exchange).forEach(coin => allCoins.add(coin));
      });
      setAllSymbols(Array.from(allCoins).sort());
    } catch (err) {
      console.error(err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const exchangeList = useMemo(() => Object.keys(exchangeData).sort(), [exchangeData]);
  const getCoinCount = (exchange) => Object.keys(exchangeData[exchange] || {}).length;

  const getValue = (coin, exchange) => {
    const coinData = exchangeData[exchange]?.[coin];
    if (!coinData) return null;
    const val = coinData[currentPeriod.dataKey];
    return val !== undefined && val !== null ? val : null;
  };

  // Сначала сортируем все монеты (если активна сортировка по бирже)
  const sortedSymbols = useMemo(() => {
    if (!sortConfig.exchange || !sortConfig.direction) return allSymbols;
    const withValues = allSymbols.map(coin => ({ coin, value: getValue(coin, sortConfig.exchange) }));
    withValues.sort((a, b) => {
      if (a.value === null && b.value === null) return 0;
      if (a.value === null) return 1;
      if (b.value === null) return -1;
      return sortConfig.direction === 'desc' ? a.value - b.value : b.value - a.value;
    });
    return withValues.map(item => item.coin);
  }, [sortConfig, allSymbols, period]);

  // Фильтрация по поисковому запросу (регистронезависимая)
  const filteredSymbols = useMemo(() => {
    if (!searchTerm.trim()) return sortedSymbols;
    const lowerTerm = searchTerm.toLowerCase().trim();
    return sortedSymbols.filter(coin => coin.toLowerCase().includes(lowerTerm));
  }, [sortedSymbols, searchTerm]);

  const handleExchangeClick = (exchange) => {
    setSortConfig(prev => {
      if (prev.exchange !== exchange) return { exchange, direction: 'desc' };
      if (prev.direction === 'desc') return { exchange, direction: 'asc' };
      return { exchange: null, direction: null };
    });
  };

  const formatWithPercent = (value) => {
    if (value === null || value === undefined) return '—';
    return `${value.toFixed(5)}%`;
  };

  const getTextColor = (value) => {
    if (value === null || value === undefined) return '#6c757d';
    if (value > 0) return '#28a745';
    if (value < 0) return '#dc3545';
    return '#212529';
  };

  const getSortIcon = (exchange) => {
    if (sortConfig.exchange !== exchange) return ' ↕️';
    return sortConfig.direction === 'desc' ? ' ↓' : ' ↑';
  };

  if (loading) {
    return (
      <div className="container mt-5 text-center">
        <div className="spinner-border" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="alert alert-danger m-3">
        Error: {error}
      </div>
    );
  }

  return (
    <div className="container-fluid mt-4">
      <div className="row mb-3">
        <div className="col-md-6">
          <ul className="nav nav-tabs">
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
        </div>
        <div className="col-md-6 d-flex justify-content-end">
          <div className="input-group" style={{ maxWidth: '300px' }}>
            <span className="input-group-text">🔍</span>
            <input
              type="text"
              className="form-control"
              placeholder="Search coin..."
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

      <div className="table-responsive">
        <table className="table table-bordered table-hover text-center align-middle">
          <thead className="bg-dark text-white">
            <tr>
              <th
                style={{ width: `${firstColumnWidth}px`, position: 'relative', backgroundColor: '#212529', color: 'white', borderColor: '#454d55' }}
              >
                Symbol
                <div
                  style={{
                    position: 'absolute',
                    right: 0,
                    top: 0,
                    width: '5px',
                    height: '100%',
                    cursor: 'col-resize',
                    userSelect: 'none',
                    backgroundColor: 'rgba(255,255,255,0.2)',
                  }}
                  onMouseDown={handleResizeStart}
                />
              </th>
              {exchangeList.map(ex => (
                <th
                  key={ex}
                  onClick={() => handleExchangeClick(ex)}
                  style={{ cursor: 'pointer', backgroundColor: '#212529', color: 'white', borderColor: '#454d55' }}
                >
                  {ex}{getSortIcon(ex)}
                  <br />
                  <small className="text-white-50">{getCoinCount(ex)} coins</small>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredSymbols.map(coin => (
              <tr key={coin}>
                <td className="fw-bold" style={{ width: `${firstColumnWidth}px`, backgroundColor: '#f8f9fa' }}>{coin}</td>
                {exchangeList.map(ex => {
                  const value = getValue(coin, ex);
                  return (
                    <td
                      key={`${coin}-${ex}`}
                      style={{
                        color: getTextColor(value),
                        backgroundColor: '#fff',
                      }}
                    >
                      {formatWithPercent(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
            {filteredSymbols.length === 0 && (
              <tr>
                <td colSpan={exchangeList.length + 1} className="text-center py-4 text-muted">
                  No coins found for "{searchTerm}"
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="mt-2 text-muted small">
        {filteredSymbols.length} / {sortedSymbols.length} coins displayed
      </div>
    </div>
  );
};

export default FundingRateHeatmap;