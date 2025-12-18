from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, List, Optional

import json  # <<< WAJIB

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError

from .db import init_db

# =========================
# Pydantic models (validation)
# =========================

class EventIn(BaseModel):
    topic: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    timestamp: datetime
    source: str = Field(min_length=1)
    payload: dict[str, Any]


def _normalize_payload(body: Any) -> List[dict]:
    """
    Terima beberapa bentuk input:
    1) {"events": [ ... ]}  -> batch
    2) [ ... ]              -> batch
    3) { ... }              -> single event
    """
    if isinstance(body, dict) and "events" in body:
        events = body["events"]
        if not isinstance(events, list):
            raise HTTPException(status_code=400, detail="'events' harus array")
        return events

    if isinstance(body, list):
        return body

    if isinstance(body, dict):
        return [body]

    raise HTTPException(status_code=400, detail="Body harus object / array / {events:[...]}")


def _parse_events(raw_events: List[dict]) -> List[EventIn]:
    """Validasi semua event dulu (fail-fast)."""
    try:
        return [EventIn.model_validate(e) for e in raw_events]
    except ValidationError as ve:
        # 400 biar konsisten dengan gaya kamu (bisa juga 422)
        raise HTTPException(status_code=400, detail=ve.errors())


# =========================
# App + lifecycle
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # init_db() diasumsikan mengembalikan asyncpg pool
    pool = await init_db()
    app.state.db_pool = pool
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="PubSub Log Aggregator", lifespan=lifespan)


# =========================
# Routes
# =========================

@app.post("/publish")
async def publish(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    raw_events = _normalize_payload(body)
    events = _parse_events(raw_events)

    pool = request.app.state.db_pool

    received = len(events)
    inserted = 0  # unique processed
    duplicates = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Insert satu-satu (tetap aman + idempotent via ON CONFLICT)
            # Tapi stats di-update sekali di akhir supaya minim query.
            for e in events:
                row = await conn.fetchrow(
                    """
                    INSERT INTO processed_events(topic, event_id, ts_ingest, source, payload)
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    RETURNING 1
                    """,
                    e.topic,
                    e.event_id,
                    e.timestamp,          # pydantic sudah parse ISO8601 termasuk "Z"
                    e.source,
                    json.dumps(e["payload"]) # <<< INI KUNCI FIX (aman kalau kolom TEXT/JSON)
                )

                if row is not None:
                    inserted += 1
                else:
                    duplicates += 1

            # Update stats cukup 3 statement (lebih efisien + tetap transaksi)
            await conn.execute(
                "UPDATE stats SET val = val + $1 WHERE key = 'received'",
                received,
            )
            if inserted:
                await conn.execute(
                    "UPDATE stats SET val = val + $1 WHERE key = 'unique_processed'",
                    inserted,
                )
            if duplicates:
                await conn.execute(
                    "UPDATE stats SET val = val + $1 WHERE key = 'duplicate_dropped'",
                    duplicates,
                )

    return {"accepted": received, "inserted": inserted, "duplicates": duplicates}


@app.get("/events")
async def list_events(request: Request, topic: Optional[str] = None, limit: int = Query(100, ge=1, le=5000)):
    pool = request.app.state.db_pool

    base = "SELECT topic,event_id,ts_ingest,source,payload FROM processed_events"
    if topic:
        q = base + " WHERE topic=$1 ORDER BY ts_ingest DESC LIMIT $2"
        args = (topic, limit)
    else:
        q = base + " ORDER BY ts_ingest DESC LIMIT $1"
        args = (limit,)

    async with pool.acquire() as conn:
        rows = await conn.fetch(q, *args)

    return [dict(r) for r in rows]


@app.get("/stats")
async def get_stats(request: Request):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key,val FROM stats")
    return {r["key"]: r["val"] for r in rows}


@app.get("/health")
async def health():
    return {"ok": True}
