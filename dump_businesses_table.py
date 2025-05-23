import sqlite3
import pandas as pd
from pathlib import Path

# Paths
DB_PATH = "businesses.db"
DUMP_DIR = Path("database_dump")
DUMP_FILE = DUMP_DIR / "businesses_dump.csv"

# Ensure dump directory exists
DUMP_DIR.mkdir(parents=True, exist_ok=True)

# Connect to DB and read table
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM businesses", conn)
conn.close()

# Save to CSV
df.to_csv(DUMP_FILE, index=False)
print(f"✅ Dumped {len(df)} rows to {DUMP_FILE}")
