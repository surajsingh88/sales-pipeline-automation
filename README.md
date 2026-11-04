# 🚀 Enterprise Sales Analytics Data Pipeline

An enterprise-grade, automated **Medallion Architecture ETL Pipeline** built with **Python, Prefect 3.x, DuckDB, and Apache Parquet**. Designed for resilient batch processing, defensive shift-left data quality validation, and high-performance in-process OLAP analytics.

---

## 🏗️ System Architecture

```text
[ Raw Transactional CSV ]
           │
           ▼
┌────────────────────────────────────────────────────────┐
│ 🥉 BRONZE LAYER (Ingestion & Schema Conformance)        │
│   - Ingests raw sales logs into DuckDB staging tables   │
└────────────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│ 🥈 SILVER LAYER (Defensive Cleaning & Shift-Left QA)   │
│   - Defensive boundary assertions (Sales > 0, Discount)│
│   - Null handling, deduplication, and net revenue logic│
│   - Persistent OLAP storage in DuckDB                 │
└────────────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER (OLAP Intelligence & Mart Export)        │
│   - Advanced Window SQL (DENSE_RANK, Running Totals)   │
│   - Zero-copy columnar export to Apache Parquet        │
└────────────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────────────────────────────────────┐
│ ⚙️ ORCHESTRATION & DEPLOYMENT (Prefect 3.x)            │
│   - Automated Cron scheduling ("0 9 * * *")            │
│   - Exponential backoff retries & failure hooks        │
│   - Real-time telemetry via Prefect Web Dashboard      │
└────────────────────────────────────────────────────────┘

## 📊 Executive Sales Intelligence Dashboard

![Executive Sales Intelligence Dashboard](dashboard.png)