import pandas as pd
import duckdb
from prefect import task, flow

DB_PATH = "analytics_warehouse.duckdb"

# ==========================================
# 1. BRONZE LAYER: INGESTION
# ==========================================
@task(retries=2, retry_delay_seconds=1)
def extract_raw_sales() -> pd.DataFrame:
    print("--> [Bronze Layer] Ingesting raw transactional sales...")
    data = {
        "order_id": ["ORD-101", "ORD-102", "ORD-103", "ORD-104", "ORD-105", "ORD-106"],
        "city": ["Lucknow", "Delhi", "Lucknow", "Mumbai", "Delhi", "Bangalore"],
        "sale_amount": [5000.0, -1200.0, 7500.0, 3200.0, 4100.0, 8900.0],  # Corrupt record included
        "discount_pct": [10, 5, 12, 15, 8, 20]
    }
    return pd.DataFrame(data)

# ==========================================
# 2. SILVER LAYER: QUALITY GATE & CLEANING
# ==========================================
@task
def clean_and_validate_silver(df: pd.DataFrame) -> pd.DataFrame:
    print("--> [Silver Layer] Running Shift-Left validation & defensive cleaning...")
    
    # 1. Uniqueness check
    assert df["order_id"].is_unique, "Duplicate order_id detected!"
    
    # 2. Defensive cleaning (bad data filtration)
    clean_df = df[df["sale_amount"] >= 0].copy()
    
    # 3. Assert validation gate
    assert (clean_df["sale_amount"] >= 0).all(), "Critical: Negative revenue breached quality gate!"
    
    # 4. Feature engineering
    clean_df["net_revenue"] = clean_df["sale_amount"] * (1 - clean_df["discount_pct"] / 100)
    
    # Ingest clean data into DuckDB Silver Table
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS silver_sales AS SELECT * FROM clean_df")
    # Replace table with fresh data
    conn.execute("CREATE OR REPLACE TABLE silver_sales AS SELECT * FROM clean_df")
    conn.close()
    
    return clean_df

# ==========================================
# 3. GOLD LAYER: OLAP SQL ANALYTICAL MARTS
# ==========================================
# ==========================================
# 3. GOLD LAYER: OLAP SQL ANALYTICAL MARTS
# ==========================================
@task
def build_gold_analytics_mart():
    print("--> [Gold Layer] Computing analytical KPIs using DuckDB SQL Window Functions...")
    conn = duckdb.connect(DB_PATH)
    
    # Advanced Analytical SQL (Bina semicolon ';' ke define kiya hai)
    query = """
    SELECT 
        city,
        COUNT(order_id) AS total_orders,
        ROUND(SUM(net_revenue), 2) AS city_revenue,
        ROUND(AVG(net_revenue), 2) AS avg_ticket_size,
        DENSE_RANK() OVER (ORDER BY SUM(net_revenue) DESC) AS revenue_rank,
        ROUND(SUM(net_revenue) * 100.0 / SUM(SUM(net_revenue)) OVER (), 2) AS pct_revenue_contribution
    FROM silver_sales
    GROUP BY city
    ORDER BY revenue_rank ASC
    """
    
    gold_df = conn.execute(query).df()
    
    # Clean export to Parquet format
    conn.execute(f"COPY ({query}) TO 'gold_sales_clean.parquet' (FORMAT PARQUET)")
    
    print("\n" + "="*70)
    print("📊 [GOLD ANALYTICAL MART: REVENUE INTELLIGENCE]")
    print(gold_df.to_string(index=False))
    print("="*70 + "\n")
    
    conn.close()
    return gold_df

# ==========================================
# 4. FLOW CONTROLLER
# ==========================================
@flow(name="Medallion-DuckDB-ETL-Pipeline")
def run_medallion_pipeline():
    raw_df = extract_raw_sales()
    clean_df = clean_and_validate_silver(raw_df)
    build_gold_analytics_mart()

if __name__ == "__main__":
    run_medallion_pipeline()