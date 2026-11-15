# SOP: Ingestion Layer (1111)
## 1. Intent
To capture and persist high-fidelity market physics data (Velocity, Work, Acceleration) into a partitioned, durable storage format suitable for downstream research.

## 2. ISO 9001 Alignment
* **Clause 7.1.5 (Monitoring and Measuring Resources):** Ensures the accuracy of ingested ticks via `t_msc` and synchronized server time.
* **Clause 8.5.2 (Identification and Traceability):** Data is partitioned by symbol (`/D:/market_physics/{symbol}/`) to ensure full traceability of every price movement.

## 3. Operational Logic
* **Storage:** Persists data via `duckdb` (disk-based) to ensure zero data loss on service interruption.
* **Rotation:** Atomic flush from memory to Parquet files allows for lean memory usage.
* **Extension:** Update `rotate_to_parquet` if storage destination requirements (e.g., cloud buckets) evolve.

## 4. Risk Management (ISO 9001 Clause 6.1)
| Risk Item | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **Storage Corruption** | Data loss during write. | DuckDB write-ahead logging (WAL) and atomic Parquet flushes. |
| **Partition Bottleneck** | IOPS exhaustion on HDD/SSD. | Partitioning by Symbol folder prevents single-file write locks. |
| **Buffer Overrun** | Memory accumulation. | Regular `rotate_to_parquet` cycles prevent unbounded memory growth. |
| **Service Hang** | Data stale/delayed. | 10s ZMQ heartbeat polling triggers automated reconnect sequence. |