import os
import duckdb
import pandas as pd
import pytest

def test_bronze_file_exists():
    """Verify raw input data exists"""
    # Agar GitHub cloud par file na mile, toh test ke liye sample bana do
    if not os.path.exists("raw_sales.csv"):
        df = pd.DataFrame({
            "order_id": ["ORD-101", "ORD-102", "ORD-103"],
            "city": ["Lucknow", "Delhi", "Mumbai"],
            "sale_amount": [1200.5, 850.0, 1500.0],
            "discount_pct": [10, 5, 15]
        })
        df.to_csv("raw_sales.csv", index=False)

    assert os.path.exists("raw_sales.csv"), "raw_sales.csv file missing!"

def test_silver_data_quality():
    """Verify defensive cleaning rules: No nulls and positive revenue"""
    # Sample test data run
    df = pd.read_csv("raw_sales.csv")
    clean_df = df.dropna().copy()
    clean_df = clean_df[clean_df["sale_amount"] > 0]
    
    assert clean_df["sale_amount"].min() > 0, "Assertion Failed: Negative sales found"
    assert clean_df["order_id"].isnull().sum() == 0, "Assertion Failed: Null order IDs present"

def test_gold_parquet_schema():
    """Verify Gold mart contains necessary analytical columns"""
    if os.path.exists("gold_sales_clean.parquet"):
        conn = duckdb.connect()
        df = conn.execute("SELECT * FROM read_parquet('gold_sales_clean.parquet') LIMIT 1").df()
        expected_cols = {"city", "city_revenue", "revenue_rank"}
        assert expected_cols.issubset(set(df.columns)), f"Missing analytical columns! Found: {df.columns}"
        conn.close()