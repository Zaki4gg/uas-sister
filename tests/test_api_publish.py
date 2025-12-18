import pytest
from datetime import datetime, timezone


def ev(topic="auth", event_id="A-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"x": 1},
    }


@pytest.mark.asyncio
async def test_publish_single_valid(client):
    r = await client.post("/publish", json=ev())
    assert r.status_code == 200
    j = r.json()
    assert j["received"] == 1
    assert j["inserted"] == 1
    assert j["duplicates"] == 0


@pytest.mark.asyncio
async def test_publish_batch_valid(client):
    payload = [ev(event_id="B-1"), ev(event_id="B-2"), ev(event_id="B-3")]
    r = await client.post("/publish", json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j["received"] == 3
    assert j["inserted"] == 3


@pytest.mark.asyncio
async def test_publish_schema_missing_field(client):
    bad = {"topic": "x"}
    r = await client.post("/publish", json=bad)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_publish_empty_batch(client):
    r = await client.post("/publish", json=[])
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_publish_too_large_batch(client):
    payload = [ev(event_id=f"x-{i}") for i in range(1001)]
    r = await client.post("/publish", json=payload)
    assert r.status_code == 413
