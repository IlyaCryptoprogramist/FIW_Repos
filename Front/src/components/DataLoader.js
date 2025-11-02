import React, { useState, useEffect } from 'react';

// Хук для загрузки данных
export const useDataLoader = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        // Имитация загрузки данных с задержкой
        setTimeout(async () => {
          try {
            // В реальном приложении замените на актуальные пути к файлам
            const exchangeData = await import('../data/top10_all_exchanges_20251022_151505.json');
            const globalData = await import('../data/top10_all_exchanges_global_20251022_151553.json');
            
            setData({
              exchange: exchangeData.default,
              global: globalData.default
            });
            setLoading(false);
          } catch (importError) {
            setError('Ошибка загрузки файлов данных: ' + importError.message);
            setLoading(false);
          }
        }, 1000); // Задержка для демонстрации загрузки
      } catch (err) {
        setError('Ошибка при загрузке данных: ' + err.message);
        setLoading(false);
      }
    };

    loadData();
  }, []);

  return { data, loading, error };
};

// Компонент для отображения ошибок
export const ErrorDisplay = ({ error }) => (
  <div className="container mt-4">
    <div className="alert alert-danger" role="alert">
      <h4 className="alert-heading">Ошибка загрузки данных</h4>
      <p>{error}</p>
      <hr />
      <p className="mb-0">
        Убедитесь, что файлы данных находятся в папке src/data/
      </p>
    </div>
  </div>
);

export default useDataLoader;