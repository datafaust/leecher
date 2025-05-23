
import pandas as pd
import sqlite3

CSV_PATH = "businesses.csv"
DB_PATH = "businesses.db"
TABLE_NAME = "businesses"
TEMP_TABLE = "businesses_temp"
FAILURE_TABLE = "website_recovery_failures"
EMAIL_FAILURE_TABLE = "email_recovery_failures"

# Load CSV
df = pd.read_csv(CSV_PATH)

# Drop 'valid_check' column if it exists
df = df.drop(columns=["valid_check"], errors="ignore")

# Remove duplicates by yelp_id
df = df.drop_duplicates(subset="yelp_id", keep="first")

# Replace empty strings with None so they become NULL in SQLite
df.replace("", None, inplace=True)

# Normalize optional datetime columns
for col in ["website_checked_at", "email_checked_at"]:
    if col not in df.columns:
        df[col] = None

# Ensure boolean
df["form_only"] = df["form_only"].astype(bool)

# Write to temporary table
conn = sqlite3.connect(DB_PATH)
df.to_sql(TEMP_TABLE, conn, if_exists="replace", index=False)

# Drop existing main table and recreate with primary key
conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
conn.execute(f"""
CREATE TABLE {TABLE_NAME} (
    yelp_id TEXT PRIMARY KEY,
    name TEXT,
    address TEXT,
    image_url TEXT,
    phone_number TEXT,
    website TEXT,
    real_website TEXT,
    email TEXT,
    latitude REAL,
    longitude REAL,
    zip_code TEXT,
    country TEXT,
    city TEXT,
    address1 TEXT,
    category_term TEXT,
    category_code TEXT,
    scraped_at TEXT,
    website_checked_at TEXT,
    email_checked_at TEXT,
    form_only BOOLEAN
)
""")

# Copy data from temp into final table
conn.execute(f"""
INSERT INTO {TABLE_NAME}
SELECT * FROM {TEMP_TABLE}
""")
conn.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")

# Create failure table if not exists
conn.execute(f"""
CREATE TABLE IF NOT EXISTS {FAILURE_TABLE} (
    yelp_id TEXT PRIMARY KEY,
    website_checked_at TEXT,
    reason TEXT
)
""")

# Create email recovery failure table if not exists
conn.execute(f"""
CREATE TABLE IF NOT EXISTS {EMAIL_FAILURE_TABLE} (
    yelp_id TEXT PRIMARY KEY,
    website_checked_at TEXT,
    reason TEXT
)
""")

conn.commit()

# Preview result
preview_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5", conn)
conn.close()

print(f"✅ SQLite database '{DB_PATH}' created with primary key on 'yelp_id'")
print(f"✅ Table '{FAILURE_TABLE}' ensured for recovery logs")
print(f"✅ Table '{EMAIL_FAILURE_TABLE}' ensured for email recovery logs\n")
print("🔍 Preview of first 5 rows:")
print(preview_df.to_string(index=False))
