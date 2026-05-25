import pandas as pd

CSV_RAW     = "../data/zomato_delivery.csv"
CSV_CLEANED = "../data/zomato_dataset_cleaned.csv"

# ── 1. Load ──────────────────────────────────────────────────────
df = pd.read_csv(CSV_RAW)
print(f"Raw shape  : {df.shape}")

# ── 2. Select Columns ────────────────────────────────────────────
selected_columns = [
    'ID', 'Delivery_person_ID', 'Delivery_person_Ratings',
    'Order_Date', 'Time_Orderd', 'Time_Order_picked',
    'Weather_conditions', 'Road_traffic_density',
    'Type_of_order', 'Type_of_vehicle',
    'Festival', 'Time_taken (min)'
]
df = df[selected_columns]

# ── 3. Drop Null Values ───────────────────────────────────────────
print("\nNull counts before drop:")
print(df.isna().sum())
df = df.dropna()
print(f"\nShape after dropna: {df.shape}")

# ── 4. Validate & Fix Time Columns ───────────────────────────────
def is_valid_time_format(time_str):
    if pd.isna(time_str):
        return False
    if isinstance(time_str, float):
        return False
    try:
        pd.to_datetime(time_str, format='%H:%M', errors='raise')
        return True
    except ValueError:
        return False

df = df[
    df['Time_Orderd'].apply(is_valid_time_format) &
    df['Time_Order_picked'].apply(is_valid_time_format)
]
df['Time_Orderd']       = pd.to_datetime(df['Time_Orderd'],
                                          format='%H:%M').dt.time
df['Time_Order_picked'] = pd.to_datetime(df['Time_Order_picked'],
                                          format='%H:%M').dt.time

# ── 5. Fix Data Types ─────────────────────────────────────────────
df['Order_Date']           = pd.to_datetime(df['Order_Date'], format='%d-%m-%Y')
df['Weather_conditions']   = df['Weather_conditions'].astype('category')
df['Road_traffic_density'] = df['Road_traffic_density'].astype('category')
df['Festival']             = df['Festival'].astype('category')
df['Type_of_order']        = df['Type_of_order'].astype('category')
df['Type_of_vehicle']      = df['Type_of_vehicle'].astype('category')

# ── 6. Save ───────────────────────────────────────────────────────
df.to_csv(CSV_CLEANED, index=False)
print(f"\nCleaned dataset saved → {CSV_CLEANED}")
print(f"Final shape: {df.shape}")
print("\nSample:")
print(df.head())