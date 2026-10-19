import pandas as pd

# 1. Raw Data Load Karein
raw_data = {
    "order_id": ["ORD-101", "ORD-102", "ORD-102", "ORD-103", "ORD-104", "ORD-105", "ORD-106", "ORD-107"],
    "city": ["Lucknow", "Delhi", "Delhi", "Mumbai", None, "Lucknow", None, "Bangalore"],
    "sale_amount": [1200.50, 850.00, 850.00, -500.00, 1500.00, 2200.00, 99999.00, 1100.00],
    "discount_pct": [10, 5, 5, 0, 15, 20, 50, 10],
    "currency": ["INR", "INR", "INR", "INR", "INR", "INR", "INR", "INR"]
}
df = pd.DataFrame(raw_data)

print("Step 1: Basic Cleaning chalu...")
# Duplicate rows hatayein
df = df.drop_duplicates()

# Missing city ki jagah 'Unknown' bharein
df["city"] = df["city"].fillna("Unknown")

# Bekar constant column (currency) ko hata dein
df = df.drop(columns=["currency"])

# Negative sales (-500) ko hatane wali line (Abhi humne iske aage # lagakar band rakha hai)
# df = df[df["sale_amount"] >= 0]
df = df[df["sale_amount"] >= 0]


print("Step 2: Running Shift-Left Quality Checks (Assert Gates)...")

# Rule 1: Order ID unique honi chahiye
assert df["order_id"].is_unique, "QUALITY ERROR: Duplicate Order ID mil gayi hai!"

# Rule 2: City column me koi NULL value nahi बचनी chahiye
assert df["city"].isnull().sum() == 0, "QUALITY ERROR: City column me abhi bhi missing value hai!"

# Rule 3: Sale amount kabhi negative nahi ho sakta
assert (df["sale_amount"] >= 0).all(), "QUALITY ERROR: Negative sale_amount pakda gaya! Pipeline roki ja rahi hai!"

# Rule 4: Discount 0 se 100 percent ke beech hi hona chahiye
assert df["discount_pct"].between(0, 100).all(), "QUALITY ERROR: Invalid discount percentage!"


# Agar upar ke charo Assert checks pass ho gaye, tabhi ye niche wala code chalega
df.to_csv("clean_sales_data.csv", index=False)
print("SUCCESS: Saare Quality Checks pass ho gaye! Clean file save ho gayi.")