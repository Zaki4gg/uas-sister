import os
import asyncio
import pathlib
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://user:pass@storage:5432/db")

# kalau file ini ada di aggregator/app/db.py, maka schema ada di aggregator/sql/schema.sql
SCHEMA_PATH = pathlib.Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


async def init_db() -> asyncpg.Pool:
    # retry connect (biar aman kalau postgres belum ready)
    last = None
    for _ in range(60):
        try:
            pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=10)
            break
        except Exception as e:
            last = e
            await asyncio.sleep(0.5)
    else:
        raise RuntimeError(f"Gagal connect DB: {last!r}")

    # init schema
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    async with pool.acquire() as conn:
        await conn.execute(sql)

    return pool
