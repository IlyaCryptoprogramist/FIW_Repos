import React, { useState, useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

const FundingHistoryChart = ({ coin, exchange, days, lineColor = '#1890ff' }) => {
  const [data, setData] = useState([]);
  const [changes, setChanges] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const chartRef = useRef(null);
  const chartInstance = useRef(null);

  const fetchHistory = async () => {
    if (!coin || !exchange) return;
    setLoading(true);
    setError(null);
    try {
      const response = await fetch('http://localhost:5000/api/funding-history', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          coin: coin.toUpperCase(),
          exchange: exchange,
          daysBack: days
        })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Failed to load funding history');
      const chartData = result.history.map(item => ({
        timestamp: new Date(item.timestamp).toLocaleString(),
        fundingRate: parseFloat(item.fundingRate.toFixed(5))
      }));
      setData(chartData);
      setChanges(result.intervalChanges || []);
    } catch (err) {
      console.error(err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
  }, [coin, exchange, days]);

  useEffect(() => {
    if (loading || error || !chartRef.current || data.length === 0) return;
    if (chartInstance.current) chartInstance.current.destroy();

    const rates = data.map(d => d.fundingRate);
    const maxRate = Math.max(...rates, 0.05);
    const yPos = maxRate + (maxRate * 0.08); // чуть выше максимума

    const scatterData = changes.map(change => ({
      x: new Date(change.timestamp).toLocaleString(),
      y: yPos,
      oldInterval: change.oldInterval,
      newInterval: change.newInterval
    }));

    const ctx = chartRef.current.getContext('2d');
    chartInstance.current = new Chart(ctx, {
      type: 'line',
      data: {
        labels: data.map(d => d.timestamp),
        datasets: [
          {
            label: `Funding Rate (%) - ${exchange}`,
            data: data.map(d => d.fundingRate),
            borderColor: lineColor,
            backgroundColor: `${lineColor}33`,
            borderWidth: 2,
            pointRadius: 2,
            pointHoverRadius: 5,
            tension: 0.3,
            fill: true,
            order: 1
          },
          {
            type: 'scatter',
            label: 'Interval change',
            data: scatterData,
            backgroundColor: '#ff4d4f',
            borderColor: '#ff4d4f',
            pointRadius: 6,
            pointHoverRadius: 9,
            showLine: false,
            order: 0,
            parsing: false,
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const point = ctx.raw;
                  return `Interval changed: ${point.oldInterval}h → ${point.newInterval}h`;
                },
                title: (ctx) => {
                  const point = ctx.raw;
                  return new Date(point.x).toLocaleString();
                }
              }
            }
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: (context) => {
                if (context.dataset.label.includes('Funding Rate')) {
                  return `${context.dataset.label}: ${context.raw}%`;
                }
                return null;
              }
            }
          }
        },
        scales: {
          x: {
            ticks: {
              maxRotation: 45,
              minRotation: 45,
              autoSkip: true,
              maxTicksLimit: 10
            },
            title: { display: true, text: 'Date & Time' }
          },
          y: {
            title: { display: true, text: 'Funding Rate (%)' },
            ticks: { callback: (val) => `${val}%` },
            suggestedMin: -0.05,
            suggestedMax: 0.10,
            beginAtZero: false,
            grace: '5%'
          }
        }
      }
    });

    return () => {
      if (chartInstance.current) chartInstance.current.destroy();
    };
  }, [data, changes, loading, error, exchange, lineColor]);

  if (!exchange) return null;

  return (
    <div className="mb-4">
      <h6 className="mb-2">{exchange}</h6>
      {loading && <div className="text-center py-4"><div className="spinner-border text-primary" role="status"><span className="visually-hidden">Loading...</span></div></div>}
      {error && <div className="alert alert-danger">{error}</div>}
      {!loading && !error && data.length === 0 && <div className="alert alert-warning">No funding rate data for {exchange}</div>}
      {!loading && !error && data.length > 0 && (
        <div style={{ overflowX: 'auto', overflowY: 'hidden', width: '100%' }}>
          <div style={{ minWidth: '800px', height: '400px' }}>
            <canvas ref={chartRef} style={{ width: '100%', height: '100%' }} />
          </div>
        </div>
      )}
    </div>
  );
};

export default FundingHistoryChart;