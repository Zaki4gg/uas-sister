# Laporan UAS — Pub-Sub Log Aggregator Terdistribusi
> Bahasa Indonesia, istilah teknis Inggris bila relevan. Sitasi APA 7th.

## Ringkasan Sistem
Tuliskan ringkas: tujuan, komponen, alur data, dan alasan desain.

## Arsitektur & Keputusan Desain
- Pub-Sub: publisher → /publish → durable queue (DB) → workers.
- Idempotency + Dedup: UNIQUE(topic,event_id) + ON CONFLICT DO NOTHING.
- Concurrency control: SELECT … FOR UPDATE SKIP LOCKED (claim queue).
- Transaksi: insert batch + update stats dalam 1 transaksi; mark done + increment stats dalam 1 transaksi.

## T1–T10 (150–250 kata per poin)
### T1 (Bab 1) Karakteristik sistem terdistribusi & trade-off Pub-Sub aggregator
(Tulis jawaban + sitasi)

### T2 (Bab 2) Kapan memilih publish–subscribe dibanding client–server?
(Tulis jawaban + sitasi)

### T3 (Bab 3) At-least-once vs exactly-once; peran idempotent consumer
(Tulis jawaban + sitasi)

### T4 (Bab 4) Skema penamaan topic dan event_id untuk dedup
(Tulis jawaban + sitasi)

### T5 (Bab 5) Ordering praktis (timestamp + monotonic counter) & dampaknya
(Tulis jawaban + sitasi)

### T6 (Bab 6) Failure modes & mitigasi (retry/backoff/durable dedup/crash recovery)
(Tulis jawaban + sitasi)

### T7 (Bab 7) Eventual consistency; peran idempotency + dedup
(Tulis jawaban + sitasi)

### T8 (Bab 8) Desain transaksi: ACID, isolation level, hindari lost-update
- Jelaskan kenapa **READ COMMITTED** cukup (unique constraint + row locks).
- (Opsional) SERIALIZABLE + retry bila mau nilai plus.
(Tulis jawaban + sitasi)

### T9 (Bab 9) Kontrol konkurensi: locking/unique constraints/upsert; idempotent write
- Dedup atomik: `UNIQUE(topic,event_id)` + `ON CONFLICT DO NOTHING`.
- Claim queue aman: `FOR UPDATE SKIP LOCKED` mencegah double-process.
(Tulis jawaban + sitasi)

### T10 (Bab 10–13) Orkestrasi Compose, keamanan jaringan lokal, persistensi, observability
- `networks.internal=true`, tidak ada akses publik dari container.
- Volume Postgres untuk persistensi.
- Logging + /stats.
(Tulis jawaban + sitasi)

## Metrik Performa
- Target: ≥20.000 event, ≥30% duplikasi.
- Sertakan hasil (throughput/latency/duplicate rate) dari publisher/k6.

## Bukti Uji Konkurensi
Ringkas hasil test: multi-worker, publish paralel, tidak ada double-process.

## Referensi (APA 7th)
- Buku utama: (isi metadata sesuai `docs/buku-utama.pdf`)
