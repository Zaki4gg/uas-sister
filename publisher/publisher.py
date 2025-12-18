import asyncio
import os
import random
from datetime import datetime, timezone
import httpx

TARGET_URL = os.getenv("TARGET_URL", "http://localhost:8080/publish")
COUNT = int(os.getenv("COUNT", "20000"))
DUP_RATE = float(os.getenv("DUP_RATE", "0.30"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "50"))
TOPICS = os.getenv("TOPICS", "auth,payment,orders").split(",")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


def make_event(event_id: str) -> dict:
    return {
        "topic": random.choice(TOPICS),
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher",
        "payload": {"rand": random.randint(0, 10_000_000)},
    }


async def post_with_retry(client: httpx.AsyncClient, url: str, json_body, *, retries: int = 30):
    last = None
    for i in range(retries):
        try:
            r = await client.post(url, json=json_body)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            await asyncio.sleep(min(2.0, 0.1 * (i + 1)))
    raise RuntimeError(f"publish gagal setelah retry: {last!r}")


async def main():
    base_unique = max(1, int(COUNT * (1.0 - DUP_RATE)))
    base_ids = [f"e-{i}" for i in range(base_unique)]

    def pick_event_id(i: int) -> str:
        if random.random() < DUP_RATE:
            return random.choice(base_ids)
        return f"u-{i}-{random.getrandbits(32):08x}"

    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def send_batch(batch_events: list[dict]):
            async with sem:
                return await post_with_retry(client, TARGET_URL, batch_events)

        tasks = []
        batch = []
        for i in range(COUNT):
            batch.append(make_event(pick_event_id(i)))
            if len(batch) >= BATCH_SIZE:
                tasks.append(asyncio.create_task(send_batch(batch)))
                batch = []
        if batch:
            tasks.append(asyncio.create_task(send_batch(batch)))

        inserted = 0
        received = 0
        dup = 0
        for t in asyncio.as_completed(tasks):
            out = await t
            received += out["received"]
            inserted += out["inserted"]
            dup += out["duplicates"]

        print(f"done received={received} inserted={inserted} duplicates={dup} dup_rate={dup/received:.2%}")


if __name__ == "__main__":
    asyncio.run(main())
