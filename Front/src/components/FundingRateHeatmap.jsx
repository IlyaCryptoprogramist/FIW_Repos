import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import FundingHistoryChart from './FundingHistoryChart';

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
  const [searchTerm, setSearchTerm] = useState('');
  const [lastUpdate, setLastUpdate] = useState(null);
  const [selectedCoin, setSelectedCoin] = useState(null);
  const [selectedExchange1, setSelectedExchange1] = useState('');
  const [selectedExchange2, setSelectedExchange2] = useState('');
  const [comparisonData, setComparisonData] = useState(null);
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [compareError, setCompareError] = useState(null);
  const [historyDays, setHistoryDays] = useState(7);

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
    fetchLastUpdate();
    const interval = setInterval(fetchLastUpdate, 10000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setSelectedExchange1('');
    setSelectedExchange2('');
    setComparisonData(null);
    setCompareError(null);
  }, [selectedCoin]);

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

  const fetchLastUpdate = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/last-update`);
      const data = await response.json();
      setLastUpdate(data.lastUpdate);
    } catch (err) {
      console.error('Failed to fetch last update time', err);
    }
  };

  const exchangeList = useMemo(() => Object.keys(exchangeData).sort(), [exchangeData]);
  const getCoinCount = useCallback((exchange) => Object.keys(exchangeData[exchange] || {}).length, [exchangeData]);

  const getValue = useCallback((coin, exchange) => {
    const coinData = exchangeData[exchange]?.[coin];
    if (!coinData) return null;
    return coinData[currentPeriod.dataKey];
  }, [exchangeData, currentPeriod.dataKey]);

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
  }, [sortConfig, allSymbols, getValue]);

  const filteredSymbols = useMemo(() => {
    if (!searchTerm.trim()) return sortedSymbols;
    const lowerTerm = searchTerm.toLowerCase().trim();
    return sortedSymbols.filter(coin => coin.toLowerCase().includes(lowerTerm));
  }, [sortedSymbols, searchTerm]);

  const handleExchangeClick = useCallback((exchange) => {
    setSortConfig(prev => {
      if (prev.exchange !== exchange) return { exchange, direction: 'desc' };
      if (prev.direction === 'desc') return { exchange, direction: 'asc' };
      return { exchange: null, direction: null };
    });
  }, []);

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

  const handleCoinIconClick = (coin) => {
    if (selectedCoin === coin) setSelectedCoin(null);
    else setSelectedCoin(coin);
  };

  const closePanel = () => setSelectedCoin(null);

  const availableExchangesForCoin = useMemo(() => {
    if (!selectedCoin) return [];
    return exchangeList.filter(ex => exchangeData[ex]?.[selectedCoin]);
  }, [selectedCoin, exchangeList, exchangeData]);

  const handleCompare = async () => {
    if (!selectedCoin || !selectedExchange1 || !selectedExchange2) return;
    setLoadingCompare(true);
    setCompareError(null);
    try {
      const response = await fetch(`${API_BASE_URL}/compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          coin: selectedCoin,
          exchange1: selectedExchange1,
          exchange2: selectedExchange2
        })
      });
      if (!response.ok) {
        const errData = await response.json();
        throw new Error(errData.error || 'Failed to compare');
      }
      const result = await response.json();
      setComparisonData(result);
    } catch (err) {
      console.error(err);
      setCompareError(err.message);
    } finally {
      setLoadingCompare(false);
    }
  };

  if (loading) return <div className="container mt-5 text-center"><div className="spinner-border" role="status"><span className="visually-hidden">Loading...</span></div></div>;
  if (error) return <div className="alert alert-danger m-3">Error: {error}</div>;

  return (
    <div className="container-fluid mt-4">
      <div className="row mb-3">
        <div className="col-md-6">
          <ul className="nav nav-tabs">
            {periods.map(p => (
              <li className="nav-item" key={p.key}>
                <button className={`nav-link ${period === p.key ? 'active' : ''}`} onClick={() => setPeriod(p.key)}>
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
              <button className="btn btn-outline-secondary" type="button" onClick={() => setSearchTerm('')}>×</button>
            )}
          </div>
        </div>
      </div>

      <div className="table-responsive">
        <table className="table table-bordered table-hover text-center align-middle">
          <thead className="bg-dark text-white">
            <tr>
              <th style={{ width: `${firstColumnWidth}px`, position: 'relative', backgroundColor: '#212529', color: 'white', borderColor: '#454d55' }}>
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
                    backgroundColor: 'rgba(255,255,255,0.2)'
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
                  {ex}{getSortIcon(ex)}<br /><small className="text-white-50">{getCoinCount(ex)} coins</small>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredSymbols.map(coin => (
              <React.Fragment key={coin}>
                <tr>
                  <td className="fw-bold" style={{ width: `${firstColumnWidth}px`, backgroundColor: '#f8f9fa' }}>
                    {coin}
                    <button
                      className="btn btn-sm btn-outline-secondary ms-2"
                      onClick={() => handleCoinIconClick(coin)}
                      style={{ padding: '2px 6px', fontSize: '0.7rem' }}
                      title={selectedCoin === coin ? "Hide comparison" : "Compare on two exchanges"}
                    >
                      🔍
                    </button>
                   </td>
                  {exchangeList.map(ex => {
                    const value = getValue(coin, ex);
                    return (
                      <td
                        key={`${coin}-${ex}`}
                        style={{ color: getTextColor(value), backgroundColor: '#fff' }}
                      >
                        {formatWithPercent(value)}
                      </td>
                    );
                  })}
                </tr>
                {selectedCoin === coin && (
                  <tr>
                    <td colSpan={exchangeList.length + 1} style={{ padding: '0.75rem', backgroundColor: '#f8f9fa' }}>
                      <div className="border rounded p-3">
                        <div className="d-flex justify-content-between align-items-center mb-3">
                          <h6 className="mb-0">Compare {coin}</h6>
                          <button className="btn btn-sm btn-secondary" onClick={closePanel}>Hide Statistics</button>
                        </div>
                        <div className="row">
                          <div className="col-md-5">
                            <select
                              className="form-select"
                              value={selectedExchange1}
                              onChange={(e) => setSelectedExchange1(e.target.value)}
                            >
                              <option value="">Select first exchange</option>
                              {availableExchangesForCoin.map(ex => <option key={ex} value={ex}>{ex}</option>)}
                            </select>
                          </div>
                          <div className="col-md-5">
                            <select
                              className="form-select"
                              value={selectedExchange2}
                              onChange={(e) => setSelectedExchange2(e.target.value)}
                            >
                              <option value="">Select second exchange</option>
                              {availableExchangesForCoin.map(ex => <option key={ex} value={ex}>{ex}</option>)}
                            </select>
                          </div>
                          <div className="col-md-2">
                            <button
                              className="btn btn-primary w-100"
                              onClick={handleCompare}
                              disabled={!selectedExchange1 || !selectedExchange2 || loadingCompare}
                            >
                              {loadingCompare ? 'Loading...' : 'Compare'}
                            </button>
                          </div>
                        </div>
                        {compareError && <div className="alert alert-danger mt-3">Error: {compareError}</div>}

                        {selectedExchange1 && selectedExchange2 && (
                          <div className="mt-4">
                            <div className="d-flex justify-content-between align-items-center mb-3">
                              <h6>Funding Rate History</h6>
                              <select
                                className="form-select form-select-sm w-auto"
                                value={historyDays}
                                onChange={(e) => setHistoryDays(parseInt(e.target.value))}
                              >
                                <option value={7}>7 days</option>
                                <option value={14}>14 days</option>
                                <option value={30}>30 days</option>
                              </select>
                            </div>
                            <div className="row">
                              <div className="col-md-6">
                                <FundingHistoryChart
                                  coin={selectedCoin}
                                  exchange={selectedExchange1}
                                  days={historyDays}
                                  lineColor="#1890ff"
                                />
                              </div>
                              <div className="col-md-6">
                                <FundingHistoryChart
                                  coin={selectedCoin}
                                  exchange={selectedExchange2}
                                  days={historyDays}
                                  lineColor="#ff7a45"
                                />
                              </div>
                            </div>
                          </div>
                        )}

                        {comparisonData && !loadingCompare && (
                          <div className="mt-4">
                            <h6>Comparison Results</h6>
                            <div className="table-responsive">
                              <table className="table table-sm table-bordered text-center align-middle">
                                <colgroup>
                                  <col style={{ width: '35%' }} />
                                  <col style={{ width: '32.5%' }} />
                                  <col style={{ width: '32.5%' }} />
                                </colgroup>
                                <thead className="table-secondary">
                                  <tr>
                                    <th>Metric</th>
                                    <th>{comparisonData.exchange1}</th>
                                    <th>{comparisonData.exchange2}</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  <tr>
                                    <td className="text-start fw-bold">24h Volume (USD)</td>
                                    <td>{comparisonData.stats.volume24h[comparisonData.exchange1].toLocaleString()}</td>
                                    <td>{comparisonData.stats.volume24h[comparisonData.exchange2].toLocaleString()}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Open Interest (USD)</td>
                                    <td>{comparisonData.stats.openInterest[comparisonData.exchange1]?.toLocaleString() || '—'}</td>
                                    <td>{comparisonData.stats.openInterest[comparisonData.exchange2]?.toLocaleString() || '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Orderbook Volume (USD)</td>
                                    <td>{comparisonData.stats.orderbookVolume[comparisonData.exchange1]?.toLocaleString() || '—'}</td>
                                    <td>{comparisonData.stats.orderbookVolume[comparisonData.exchange2]?.toLocaleString() || '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Spread (%)</td>
                                    <td>{comparisonData.stats.orderbookSpread[comparisonData.exchange1] !== null ? `${comparisonData.stats.orderbookSpread[comparisonData.exchange1]}%` : '—'}</td>
                                    <td>{comparisonData.stats.orderbookSpread[comparisonData.exchange2] !== null ? `${comparisonData.stats.orderbookSpread[comparisonData.exchange2]}%` : '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Funding Rate (%)</td>
                                    <td>{comparisonData.stats.fundingRate[comparisonData.exchange1]?.toFixed(4)}%</td>
                                    <td>{comparisonData.stats.fundingRate[comparisonData.exchange2]?.toFixed(4)}%</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Funding Interval (hours)</td>
                                    <td>{comparisonData.stats.fundingInterval[comparisonData.exchange1]} h</td>
                                    <td>{comparisonData.stats.fundingInterval[comparisonData.exchange2]} h</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Current Price (USD)</td>
                                    <td>{comparisonData.stats.currentPrice?.[comparisonData.exchange1]?.toFixed(4) ?? '—'}</td>
                                    <td>{comparisonData.stats.currentPrice?.[comparisonData.exchange2]?.toFixed(4) ?? '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Price Spread (%)</td>
                                    <td colSpan="2" className="text-center">
                                      {comparisonData.stats.priceSpreadPercent !== null && comparisonData.stats.priceSpreadPercent !== undefined
                                        ? `${comparisonData.stats.priceSpreadPercent.toFixed(4)}%`
                                        : '—'}
                                    </td>
                                  </tr>
                                </tbody>
                              </table>
                            </div>
                            <h6 className="mt-3">CoinMarketCap Data</h6>
                            <div className="table-responsive">
                              <table className="table table-sm table-bordered text-center align-middle">
                                <colgroup>
                                  <col style={{ width: '35%' }} />
                                  <col style={{ width: '65%' }} />
                                </colgroup>
                                <tbody>
                                  <tr>
                                    <td className="text-start fw-bold">Max Supply</td>
                                    <td>{comparisonData.cmc.maxSupply?.toLocaleString() || '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Circulating Supply</td>
                                    <td>{comparisonData.cmc.circulatingSupply?.toLocaleString() || '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Holders</td>
                                    <td>{comparisonData.cmc.holders?.toLocaleString() || '—'}</td>
                                  </tr>
                                  <tr>
                                    <td className="text-start fw-bold">Top Holders Concentration (%)</td>
                                    <td>{comparisonData.cmc.topHoldersConcentration !== null ? `${comparisonData.cmc.topHoldersConcentration}%` : '—'}</td>
                                  </tr>
                                </tbody>
                              </table>
                            </div>
                          </div>
                        )}
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
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
      <div className="mt-2 d-flex justify-content-between align-items-center">
        <div className="text-muted small">
          {filteredSymbols.length} / {sortedSymbols.length} coins displayed
        </div>
        <div className="text-muted small">
          Last data update: {lastUpdate ? new Date(lastUpdate).toLocaleString() : 'Never'}
        </div>
      </div>
    </div>
  );
};

export default FundingRateHeatmap;