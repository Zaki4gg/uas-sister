import pytest
from datetime import datetime, timezone
from asgi_lifespan import LifespanManager
import httpx
from aggregator.app.main import create_app  # Tambahkan ini
from aggregator.app.settings import Settings # Tambahkan ini


def ev(topic="auth", event_id="RST-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"a": 1},
    }


@pytest.mark.asyncio
async def test_persistence_like_restart_same_db(pg):
    s1 = Settings(database_url=pg.get_connection_url(), workers=0, batch_size=50, poll_interval_ms=10, stuck_processing_sec=60)
    app1 = create_app(s1)
    async with LifespanManager(app1):
        async with httpx.AsyncClient(app=app1, base_url="http://test") as c1:
            r1 = await c1.post("/publish", json=ev())
            assert r1.json()["inserted"] == 1

    s2 = Settings(database_url=pg.get_connection_url(), workers=0, batch_size=50, poll_interval_ms=10, stuck_processing_sec=60)
    app2 = create_app(s2)
    async with LifespanManager(app2):
        async with httpx.AsyncClient(app=app2, base_url="http://test") as c2:
            r2 = await c2.post("/publish", json=ev())
            assert r2.json()["inserted"] == 0
            stats = (await c2.get("/stats")).json()
            assert stats["received"] == 2
            assert stats["duplicate_dropped"] == 1
