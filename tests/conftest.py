import asyncio
import pytest
from asgi_lifespan import LifespanManager
import httpx
from testcontainers.postgres import PostgresContainer

from aggregator.app.main import create_app  # Tambahkan ini
from aggregator.app.settings import Settings # Tambahkan ini

# FIX: Gunakan scope session dan pastikan loop policy benar untuk Windows/Linux
@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()

# FIX: Pastikan PostgresContainer menggunakan driver asyncpg (postgresql+asyncpg)
# karena library yang kamu gunakan (asyncpg) membutuhkan skema URL tersebut
@pytest.fixture(scope="session")
def pg():
    with PostgresContainer("postgres:16-alpine") as pg:
        # Kita perlu sedikit memodifikasi URL agar kompatibel dengan asyncpg
        url = pg.get_connection_url().replace("postgresql://", "postgresql://")
        # Simpan URL asli ke objek pg agar bisa dipanggil client
        pg.db_url = url
        yield pg

@pytest.fixture
async def client(pg):
    # Gunakan pg.db_url yang sudah kita siapkan
    settings = Settings(
        database_url=pg.get_connection_url(), # Testcontainers return jdbc/psql style
        workers=0,
        batch_size=50,
        poll_interval_ms=10,
        stuck_processing_sec=60,
    )
    # Membuat app instance baru dengan settings testing
    app = create_app(settings)
    
    # LifespanManager memicu startup/shutdown (termasuk init_db)
    async with LifespanManager(app):
        # Gunakan transport untuk httpx agar bisa komunikasi langsung ke app
        async with httpx.AsyncClient(app=app, base_url="http://test") as c:
            yield c

@pytest.fixture
async def client_with_workers(pg):
    settings = Settings(
        database_url=pg.get_connection_url(),
        workers=4,
        batch_size=200,
        poll_interval_ms=10,
        stuck_processing_sec=60,
    )
    app = create_app(settings)
    async with LifespanManager(app):
        async with httpx.AsyncClient(app=app, base_url="http://test") as c:
            yield c