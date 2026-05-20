import React, { useState, useMemo } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Table, Nav, Container, Row, Col, Alert } from 'react-bootstrap';

// --- Моковые данные (имитация вашей структуры) ---

// Эмуляция класса FundData
// const fundData = { name: "BTC/USDT", day: 0.03, week: 0.114, month: -1.29, currentFR: 0.005 };

// Эмуляция класса ExchangeFundData
const exchangesData = [
  {
    name: "Binance",
    fundData: [
      { name: "ESP/USDT:USDT", day: 0.03, week: 0.114802, month: -1.292974, currentFR: 0.005 },
      { name: "BTC/USDT", day: 0.001, week: 0.007, month: 0.03, currentFR: 0.0001 },
      { name: "ETH/USDT", day: 0.002, week: 0.014, month: 0.06, currentFR: 0.0002 },
    ]
  },
  {
    name: "Bybit",
    fundData: [
      { name: "ESP/USDT:USDT", day: 0.032, week: 0.12, month: -1.30, currentFR: 0.0048 },
      { name: "BTC/USDT", day: 0.0012, week: 0.008, month: 0.035, currentFR: 0.00012 },
      { name: "SOL/USDT", day: 0.005, week: 0.035, month: 0.15, currentFR: 0.0005 },
    ]
  },
  {
    name: "OKX",
    fundData: [
      { name: "ESP/USDT:USDT", day: 0.028, week: 0.10, month: -1.15, currentFR: 0.0055 },
      { name: "SOL/USDT", day: 0.0048, week: 0.032, month: 0.14, currentFR: 0.00045 },
      // BTC/USDT здесь отсутствует для проверки прочерка
    ]
  }
];

// --- Вспомогательные функции ---

const formatValue = (value) => {
  if (value === null || value === undefined) return '-';
  // Умножаем на 100 для процентного вида (опционально, можно убрать *100)
  const num = parseFloat(value);
  return isNaN(num) ? '-' : num.toFixed(4);
};

const getColor = (value) => {
  if (value === null || value === undefined) return {};
  const num = parseFloat(value);
  if (isNaN(num)) return {};
  return { color: num >= 0 ? 'green' : 'red' };
};

// --- Основной компонент ---

function FundingRatesTable() {
  const [activeTab, setActiveTab] = useState('currentFR');

  // Определяем ключи и названия для вкладок
  const tabsConfig = [
    { key: 'currentFR', label: 'Текущий фандинг' },
    { key: 'day', label: '1 День' },
    { key: 'week', label: '7 Дней' },
    { key: 'month', label: '30 Дней' },
  ];

  // Собираем список всех уникальных монет для строк таблицы
  const allCoins = useMemo(() => {
    const coinSet = new Set();
    exchangesData.forEach(exchange => {
      exchange.fundData.forEach(coin => {
        coinSet.add(coin.name);
      });
    });
    return Array.from(coinSet).sort(); // Сортировка по алфавиту
  }, []);

  // Создаем мапу для быстрого поиска данных: { "BTC/USDT": { "Binance": {данные}, "Bybit": {данные} } }
  const lookupTable = useMemo(() => {
    const map = {};
    exchangesData.forEach(exchange => {
      exchange.fundData.forEach(coinData => {
        if (!map[coinData.name]) {
          map[coinData.name] = {};
        }
        map[coinData.name][exchange.name] = coinData;
      });
    });
    return map;
  }, []);

  return (
    <Container className="mt-4">

      <Row className="mb-3">
        <Col>
          <h3>Funding Rates Dashboard</h3>
        </Col>
      </Row>

      {/* Навигация (Вкладки) */}
      <Nav variant="tabs" defaultActiveKey="currentFR" onSelect={(k) => setActiveTab(k)}>
        {tabsConfig.map(tab => (
          <Nav.Item key={tab.key}>
            <Nav.Link eventKey={tab.key}>{tab.label}</Nav.Link>
          </Nav.Item>
        ))}
      </Nav>

      {/* Таблица */}
      <Table striped bordered hover responsive className="mt-2">
        <thead className="thead-dark">
          <tr>
            <th>Монета</th>
            {/* Заголовки - названия бирж */}
            {exchangesData.map(exchange => (
              <th key={exchange.name} className="text-center">{exchange.name}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {/* Строки - названия монет */}
          {allCoins.map(coinName => (
            <tr key={coinName}>
              <td><strong>{coinName}</strong></td>
              {/* Ячейки - значения для конкретной биржи и монеты */}
              {exchangesData.map(exchange => {
                const coinData = lookupTable[coinName]?.[exchange.name];
                const value = coinData ? coinData[activeTab] : undefined;
                
                return (
                  <td key={exchange.name + coinName} className="text-center" style={getColor(value)}>
                    {formatValue(value)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </Table>
    </Container>
  );
}

export default FundingRatesTable;