import asyncio
import pytest
from asgi_lifespan import LifespanManager
import httpx
from testcontainers.postgres import PostgresContainer

from aggregator.app.main import create_app
from aggregator.app.settings import Settings


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def pg():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture
async def client(pg):
    settings = Settings(
        database_url=pg.get_connection_url(),
        workers=0,
        batch_size=50,
        poll_interval_ms=10,
        stuck_processing_sec=60,
    )
    app = create_app(settings)
    async with LifespanManager(app):
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
