import pytest
import asyncio
from datetime import datetime, timezone


def ev(topic="orders", event_id="P-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"n": 1},
    }


async def wait_until_done(client, expected: int, timeout_s: float = 5.0):
    start = asyncio.get_event_loop().time()
    while True:
        s = (await client.get("/stats")).json()
        if s["unique_processed"] >= expected:
            return s
        if asyncio.get_event_loop().time() - start > timeout_s:
            raise AssertionError(f"timeout waiting processed={expected}, got={s['unique_processed']}")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_worker_processes_events(client_with_workers):
    r = await client_with_workers.post("/publish", json=[ev(event_id="W1"), ev(event_id="W2"), ev(event_id="W3")])
    assert r.json()["inserted"] == 3
    await wait_until_done(client_with_workers, 3)

    items = (await client_with_workers.get("/events?topic=orders")).json()
    assert len(items) == 3
    assert all(i["topic"] == "orders" for i in items)


@pytest.mark.asyncio
async def test_multiworker_no_double_process(client_with_workers):
    payload = [ev(event_id=f"E{i}") for i in range(500)]
    r = await client_with_workers.post("/publish", json=payload)
    assert r.json()["inserted"] == 500
    s = await wait_until_done(client_with_workers, 500, timeout_s=10.0)
    assert s["unique_processed"] == 500


@pytest.mark.asyncio
async def test_concurrent_publish_same_event_one_insert(client_with_workers):
    async def send():
        return await client_with_workers.post("/publish", json=ev(topic="auth", event_id="RACE-1"))

    rs = await asyncio.gather(*[send() for _ in range(50)])
    assert all(r.status_code == 200 for r in rs)

    await wait_until_done(client_with_workers, 1, timeout_s=10.0)
    s = (await client_with_workers.get("/stats")).json()
    assert s["unique_processed"] == 1
