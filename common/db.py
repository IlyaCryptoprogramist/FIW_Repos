# common/db.py
import aiosqlite
import asyncio
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "funding_data.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS funding_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                rate REAL NOT NULL,
                interval_hours INTEGER,
                UNIQUE(exchange, symbol, timestamp)
            )
        ''')
        await db.execute('''
            CREATE INDEX IF NOT EXISTS idx_exchange_symbol_timestamp 
            ON funding_rates(exchange, symbol, timestamp)
        ''')
        await db.commit()

async def get_last_timestamp(exchange: str, symbol: str) -> int:
    """Возвращает максимальный timestamp для данной биржи и символа, или 0."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            'SELECT MAX(timestamp) FROM funding_rates WHERE exchange = ? AND symbol = ?',
            (exchange, symbol)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 0

async def save_funding_rates(exchange: str, symbol: str, records: list):
    """Сохраняет список записей (каждая запись - dict с timestamp, rate, interval_hours)."""
    if not records:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            'INSERT OR IGNORE INTO funding_rates (exchange, symbol, timestamp, rate, interval_hours) VALUES (?, ?, ?, ?, ?)',
            [(exchange, symbol, r['timestamp'], r['rate'], r.get('interval_hours')) for r in records]
        )
        await db.commit()

async def get_history(exchange: str, symbol: str, start_ts: int, end_ts: int) -> list:
    """Возвращает записи за период (включительно)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            'SELECT timestamp, rate, interval_hours FROM funding_rates WHERE exchange = ? AND symbol = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp',
            (exchange, symbol, start_ts, end_ts)
        ) as cursor:
            rows = await cursor.fetchall()
            return [{'timestamp': r[0], 'rate': r[1], 'interval_hours': r[2]} for r in rows]