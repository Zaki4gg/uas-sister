import asyncpg
import os

# Tambahkan parameter dsn (Data Source Name)
async def init_db(dsn: str = None):
    # Jika dsn tidak diberikan, ambil dari ENV (fallback untuk docker)
    if dsn is None:
        dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
    
    # Buat koneksi pool menggunakan dsn tersebut
    pool = await asyncpg.create_pool(dsn=dsn)
    
    # Jalankan query inisialisasi tabel (opsional jika sudah ada di init.sql)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT,
                event_id TEXT,
                ts_ingest TIMESTAMPTZ,
                source TEXT,
                payload JSONB,
                PRIMARY KEY (topic, event_id)
            );
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                val BIGINT DEFAULT 0
            );
            INSERT INTO stats (key, val) VALUES 
                ('received', 0), 
                ('unique_processed', 0), 
                ('duplicate_dropped', 0)
            ON CONFLICT DO NOTHING;
        """)
        
    return pool