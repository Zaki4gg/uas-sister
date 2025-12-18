import pytest
from datetime import datetime, timezone


def ev(topic="auth", event_id="DUP-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"k": "v"},
    }


@pytest.mark.asyncio
async def test_dedup_duplicate_dropped(client):
    r1 = await client.post("/publish", json=ev())
    r2 = await client.post("/publish", json=ev())
    assert r1.json()["inserted"] == 1
    assert r2.json()["inserted"] == 0
    assert r2.json()["duplicates"] == 1

    s = (await client.get("/stats")).json()
    assert s["received"] == 2
    assert s["duplicate_dropped"] == 1


@pytest.mark.asyncio
async def test_dedup_unique_constraint_across_topics(client):
    r = await client.post("/publish", json=[ev(topic="t1", event_id="X"), ev(topic="t2", event_id="X")])
    assert r.status_code == 200
    assert r.json()["inserted"] == 2


@pytest.mark.asyncio
async def test_batch_mixed_duplicates(client):
    payload = [ev(event_id="M1"), ev(event_id="M1"), ev(event_id="M2"), ev(event_id="M2"), ev(event_id="M3")]
    r = await client.post("/publish", json=payload)
    j = r.json()
    assert j["received"] == 5
    assert j["inserted"] == 3
    assert j["duplicates"] == 2
