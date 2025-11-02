import React, { useState } from 'react';

const API_BASE_URL = 'http://localhost:5000/api';

export const useSearchService = () => {
  const [searchResults, setSearchResults] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState(null);

  const searchCoin = async (coinName) => {
    if (!coinName.trim()) {
      setSearchResults(null);
      return;
    }

    setSearchLoading(true);
    setSearchError(null);

    try {
      const response = await fetch(`${API_BASE_URL}/search/${encodeURIComponent(coinName)}`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      setSearchResults(data);
    } catch (error) {
      console.error('Search error:', error);
      setSearchError(`Ошибка поиска: ${error.message}. Убедитесь, что бэкенд сервер запущен на порту 5000.`);
      setSearchResults(null);
    } finally {
      setSearchLoading(false);
    }
  };

  const clearSearch = () => {
    setSearchResults(null);
    setSearchError(null);
  };

  return {
    searchResults,
    searchLoading,
    searchError,
    searchCoin,
    clearSearch
  };
};

// Компонент для отображения результатов поиска
export const SearchResults = ({ results, onClose }) => {
  if (!results) return null;

  const { coin, results: exchangeResults, total_matches } = results;

  if (total_matches === 0) {
    return (
      <div className="alert alert-warning mt-3">
        <h5>Результаты поиска для "{coin}"</h5>
        <p className="mb-0">Монета не найдена на подключенных биржах.</p>
      </div>
    );
  }

  return (
    <div className="card mt-4">
      <div className="card-header bg-info text-white d-flex justify-content-between align-items-center">
        <h5 className="mb-0">
          🔍 Результаты поиска для "{coin}"
        </h5>
        <div>
          <span className="badge bg-light text-dark me-2">
            {total_matches} совпадений
          </span>
          <button 
            className="btn btn-sm btn-light"
            onClick={onClose}
          >
            ×
          </button>
        </div>
      </div>
      <div className="card-body">
        {Object.entries(exchangeResults).map(([exchange, pairs]) => (
          <div key={exchange} className="mb-4">
            <h6 className="text-primary border-bottom pb-2">
              {exchange} <span className="badge bg-secondary">{Object.keys(pairs).length} пар</span>
            </h6>
            <div className="table-responsive">
              <table className="table table-sm table-bordered">
                <thead className="table-light">
                  <tr>
                    <th>Торговая пара</th>
                    <th>24ч FR</th>
                    <th>48ч FR</th>
                    <th>168ч FR</th>
                    <th>Текущий FR</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(pairs).map(([pair, data]) => (
                    <tr key={pair}>
                      <td className="fw-bold">{pair}</td>
                      <td className={data['24h'] >= 0 ? 'text-success' : 'text-danger'}>
                        {typeof data['24h'] === 'number' ? data['24h'].toFixed(4) : 'N/A'}
                      </td>
                      <td className={data['48h'] >= 0 ? 'text-success' : 'text-danger'}>
                        {typeof data['48h'] === 'number' ? data['48h'].toFixed(4) : 'N/A'}
                      </td>
                      <td className={data['168h'] >= 0 ? 'text-success' : 'text-danger'}>
                        {typeof data['168h'] === 'number' ? data['168h'].toFixed(4) : 'N/A'}
                      </td>
                      <td className={data.currentFR >= 0 ? 'text-success' : 'text-danger'}>
                        {typeof data.currentFR === 'number' ? data.currentFR.toFixed(4) : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default useSearchService;