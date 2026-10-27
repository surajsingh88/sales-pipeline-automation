import pandas as pd
from prefect import task, flow

# Task 1: Data Ingestion (Agar connection fail ho toh 3 baar retry karega)
@task(retries=3, retry_delay_seconds=2)
def extract_sales_data():
    print("Ingesting sales data...")
    data = {
        "order_id": ["ORD-201", "ORD-202", "ORD-203", "ORD-204", "ORD-205"],
        "city": ["Lucknow", "Kanpur", "Varanasi", "Noida", "Agra"],
        "sale_amount": [2500.0, 1800.5, 3200.0, 4100.0, 950.0],
        "discount_pct": [10, 5, 15, 20, 0]
    }
    return pd.DataFrame(data)

# Task 2: Shift-Left Quality Gates (Assert checks)
@task
def validate_and_transform(df: pd.DataFrame):
    print("Running quality gates...")
    assert df["order_id"].is_unique, "Duplicate order_id found!"
    assert (df["sale_amount"] >= 0).all(), "Negative sale_amount found!"
    
    # Revenue calculation
    df["net_revenue"] = df["sale_amount"] * (1 - df["discount_pct"] / 100)
    return df

# Task 3: Load / Export Report
@task
def export_report(df: pd.DataFrame):
    output_filename = "daily_revenue_summary.csv"
    df.to_csv(output_filename, index=False)
    print(f"Report exported successfully as {output_filename}")

# Main Flow: Saare tasks ko orchestrate karne wala controller
@flow(name="Daily-Sales-ETL-Flow")
def run_sales_pipeline():
    raw_df = extract_sales_data()
    clean_df = validate_and_transform(raw_df)
    export_report(clean_df)

if __name__ == "__main__":
    run_sales_pipeline()