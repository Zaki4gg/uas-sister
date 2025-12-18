# UAS Sistem Terdistribusi — Pub-Sub Log Aggregator (Docker Compose)

Stack: **Python (FastAPI + asyncio)** + **Postgres**. Dedup kuat via **UNIQUE(topic,event_id)**, concurrency control via **SELECT … FOR UPDATE SKIP LOCKED**.

## Jalankan
```bash
docker compose up --build
```
Aggregator: `http://localhost:8080`

## Endpoint
- `POST /publish` (single/batch)
- `GET /events?topic=...`
- `GET /stats`
- `GET /health`

## Demo duplikasi cepat (PowerShell)
```powershell
curl.exe -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d "[{`"topic`":`"auth`",`"event_id`":`"A-1`",`"timestamp`":`"2025-12-19T00:00:00Z`",`"source`":`"demo`",`"payload`":{`"x`":1}}]"
```

## Load test (opsional) dengan k6
```bash
docker compose --profile load run --rm k6
```

## Jalankan publisher simulator
Publisher akan otomatis jalan saat compose up. Untuk jalankan manual:
```bash
docker compose run --rm publisher
```

## Tests
Integration tests menggunakan **testcontainers** (butuh Docker lokal).
```bash
pip install -r tests/requirements.txt
pytest -q
```
