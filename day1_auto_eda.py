import pandas as pd

# 1. Wahi Ganda (Dirty) Sales Dataset
raw_data = {
    "order_id": ["ORD-101", "ORD-102", "ORD-102", "ORD-103", "ORD-104", "ORD-105", "ORD-106", "ORD-107"],
    "city": ["Lucknow", "Delhi", "Delhi", "Mumbai", None, "Lucknow", None, "Bangalore"],
    "sale_amount": [1200.50, 850.00, 850.00, -500.00, 1500.00, 2200.00, 99999.00, 1100.00],
    "discount_pct": [10, 5, 5, 0, 15, 20, 50, 10],
    "currency": ["INR", "INR", "INR", "INR", "INR", "INR", "INR", "INR"]
}

df = pd.DataFrame(raw_data)
df.to_csv("raw_sales_data.csv", index=False)

# 2. Humara Khud ka Auto-EDA Quality Check Function
print("=== AUTOMATED DATA QUALITY REPORT ===")
print(f"1. Total Rows: {len(df)} | Duplicate Rows: {df.duplicated().sum()}")
print("\n2. Missing Values per Column:")
print(df.isnull().sum()[df.isnull().sum() > 0])

print("\n3. Negative Sales Check:")
print(df[df["sale_amount"] < 0][["order_id", "sale_amount"]])

print("\n4. Constant Columns (Jisme sirf 1 hi unique value hai):")
constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
print(constant_cols)