import pandas as pd
from prefect import task, flow

# 1. Alert Handler Function (Jab pipeline fail hogi, Prefect isko khud chalayega)
def send_failure_alert(flow, flow_run, state):
    print("\n" + "="*60)
    print("🚨 [CRITICAL ALERT DISPATCHED] Pipeline Crash Ho Gayi Hai!")
    print(f"📌 Flow Name     : {flow.name}")
    print(f"🆔 Run ID        : {flow_run.id}")
    print(f"❌ Failure Cause : {state.message}")
    print("🔔 Action Taken  : Slack / Discord channel & Boss ko Email alert bhej diya gaya!")
    print("="*60 + "\n")
    # Production me yahan requests.post(webhook_url, json={...}) lagakar Slack ya Teams par notification jata hai.

# 2. Task 1: Raw Data Extract (Is baar jaan-boojhkar ganda data pass kar rahe hain)
@task
def extract_sales_data():
    print("--> Ingesting raw incoming sales data...")
    corrupted_data = {
        "order_id": ["ORD-301", "ORD-302", "ORD-303", "ORD-304"],
        "city": ["Lucknow", "Delhi", "Mumbai", "Bangalore"],
        "sale_amount": [2500.0, -1500.0, 3200.0, 1100.0],  # ⚠️ Dirty record (-1500.0)
        "discount_pct": [10, 5, 15, 10]
    }
    return pd.DataFrame(corrupted_data)

# 3. Task 2: Shift-Left Quality Gate (Assert check)
@task
def validate_and_transform(df: pd.DataFrame):
    print("--> Running Shift-Left validation gates...")
    assert df["order_id"].is_unique, "Duplicate order_id found!"
    
    # Ye assertion gande data (-1500) ko pakad kar pipeline crash karega
    assert (df["sale_amount"] >= 0).all(), "CRITICAL: Negative sale_amount (-1500.0) detected at ingestion!"
    
    df["net_revenue"] = df["sale_amount"] * (1 - df["discount_pct"] / 100)
    return df

# 4. Task 3: Export Report (Fail hone par ye execute hi nahi hoga)
@task
def export_report(df: pd.DataFrame):
    df.to_csv("daily_revenue_summary.csv", index=False)
    print("Report exported successfully!")

# 5. Main Flow with on_failure Hook attached
@flow(name="Daily-Sales-ETL-Flow", on_failure=[send_failure_alert])
def run_sales_pipeline():
    raw_df = extract_sales_data()
    clean_df = validate_and_transform(raw_df)
    export_report(clean_df)

if __name__ == "__main__":
    run_sales_pipeline()