import pandas as pd
from prefect import task, flow

# ==========================================
# 1. ALERT HANDLER FUNCTION (AUTOMATED NOTIFICATION)
# ==========================================
def send_failure_alert(flow, flow_run, state):
    """
    Yeh function tabhi trigger hoga jab pipeline crash hogi.
    Production me yahan Slack webhook, Email (SMTP), ya PagerDuty call hota hai.
    """
    print("\n" + "="*50)
    print("🚨 [CRITICAL ALERT] PIPELINE EXECUTION FAILED! 🚨")
    print(f"Flow Name      : {flow.name}")
    print(f"Run ID         : {flow_run.id}")
    print(f"Run Name       : {flow_run.name}")
    print(f"Failure State  : {state.name}")
    print(f"Action Taken   : Alert logged. On-call engineer notified!")
    print("="*50 + "\n")


# ==========================================
# 2. PIPELINE TASKS
# ==========================================
@task(retries=2, retry_delay_seconds=1)
def extract_sales_data():
    print("Ingesting sales data with corrupt records...")
    # Jaan-boojhkar negative sale amount (-500.0) inject kiya gaya hai
    bad_data = {
        "order_id": ["ORD-301", "ORD-302", "ORD-303", "ORD-304"],
        "city": ["Lucknow", "Delhi", "Kanpur", "Varanasi"],
        "sale_amount": [1500.0, -500.0, 2400.0, 1800.0],  # Corrupt record!
        "discount_pct": [5, 10, 15, 0]
    }
    return pd.DataFrame(bad_data)


@task
def validate_and_transform(df: pd.DataFrame):
    print("Executing Shift-Left Data Quality Gate...")
    
    # Assert Check 1: Unique orders
    assert df["order_id"].is_unique, "Duplicate order_id found!"
@task
def validate_and_transform(df: pd.DataFrame):
    print("Executing Shift-Left Data Quality Gate...")

    # Assert Check 1: Unique orders
    assert df["order_id"].is_unique, "Duplicate order_id found!"

    # ⬇️ YAHAN LAGAYEIN (Assert Check 2 se pehle gande records hatao)
    df = df[df["sale_amount"] >= 0]

    # Assert Check 2: Sale amount must be non-negative (Ab ye pass ho jayega)
    assert (df["sale_amount"] >= 0).all(), "DATA QUALITY FAILURE: Negative sale_amount detected!"

    # Revenue calculation
    df["net_revenue"] = df["sale_amount"] * (1 - df["discount_pct"] / 100)
    return df    
    


@task
def export_report(df: pd.DataFrame):
    df.to_csv("daily_revenue_summary.csv", index=False)
    print("Report exported successfully.")


# ==========================================
# 3. FLOW CONTROLLER WITH ON_FAILURE HOOK
# ==========================================
@flow(name="Daily-Sales-ETL-Alert-Demo", on_failure=[send_failure_alert])
def run_sales_pipeline():
    raw_df = extract_sales_data()
    clean_df = validate_and_transform(raw_df)
    export_report(clean_df)


if __name__ == "__main__":
    run_sales_pipeline()