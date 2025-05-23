# import pandas as pd
# import os
# import logging

# CSV_PATH = 'businesses_nyc.csv'
# LOG_PATH = 'scraper.log'

# # Configure logging
# logging.basicConfig(
#     filename=LOG_PATH,
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# def load_existing_data():
#     try:
#         return pd.read_csv(CSV_PATH)
#     except FileNotFoundError:
#         return pd.DataFrame()

# def save_combined_data(existing_df, new_df):
#     if not existing_df.empty:
#         combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['yelp_id'])
#     else:
#         combined_df = new_df
#     combined_df.to_csv(CSV_PATH, index=False)
#     logging.info(f"Saved {len(new_df)} new records. Total: {len(combined_df)}")


# sqlite version
import sqlite3
import pandas as pd
import logging
from multiprocessing import Lock
from pathlib import Path
from datetime import datetime

DB_PATH = "businesses.db"
TABLE_NAME = "businesses"
LOG_PATH = "scraper.log"

# Configure logging
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class SQLiteHandler:
    def __init__(self, db_path=DB_PATH, table_name=TABLE_NAME, lock=None):
        self.db_path = db_path
        self.table_name = table_name
        self.lock = lock or Lock()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def load_all_data(self):
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(f"SELECT * FROM {self.table_name}", conn)
            return df
        except Exception as e:
            logging.error(f"Error loading data from {self.db_path}: {e}")
            return pd.DataFrame()

    def insert_or_replace_rows(self, df: pd.DataFrame):
        if df.empty:
            return

        # Ensure we have a lock to prevent concurrent write
        with self.lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()

                    columns = df.columns.tolist()
                    placeholders = ", ".join(["?"] * len(columns))
                    column_names = ", ".join(columns)

                    query = f"""
                    INSERT OR REPLACE INTO {self.table_name} ({column_names})
                    VALUES ({placeholders})
                    """

                    cursor.executemany(query, df.values.tolist())
                    conn.commit()

                logging.info(f"✅ Inserted or replaced {len(df)} records.")
            except Exception as e:
                logging.error(f"❌ Failed to insert rows: {e}")

    def upsert_single_row(self, row_dict: dict):
        with self.lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    columns = ", ".join(row_dict.keys())
                    placeholders = ", ".join(["?"] * len(row_dict))
                    query = f"""
                    INSERT OR REPLACE INTO {self.table_name} ({columns})
                    VALUES ({placeholders})
                    """
                    cursor.execute(query, tuple(row_dict.values()))
                    conn.commit()
                logging.info(f"✅ Upserted single row for yelp_id={row_dict.get('yelp_id')}")
            except Exception as e:
                logging.error(f"❌ Failed to upsert row: {e}")

    def update_business_email_info(self, yelp_id, email, form_only, email_checked_at=None):
        if not email_checked_at:
            email_checked_at = datetime.utcnow().isoformat()

        with self.lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        UPDATE {self.table_name}
                        SET email = ?, form_only = ?, email_checked_at = ?
                        WHERE yelp_id = ?
                    """, (email, form_only, email_checked_at, yelp_id))
                    conn.commit()
                logging.info(f"✅ Updated email info for yelp_id={yelp_id}")
            except Exception as e:
                logging.error(f"❌ Failed to update email info for {yelp_id}: {e}")

    def log_failure(self, table_name, yelp_id, reason):
        timestamp = datetime.utcnow().isoformat()
        with self.lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            yelp_id TEXT PRIMARY KEY,
                            website_checked_at TEXT,
                            reason TEXT
                        )
                    """)
                    cursor.execute(f"""
                        INSERT OR REPLACE INTO {table_name} (yelp_id, website_checked_at, reason)
                        VALUES (?, ?, ?)
                    """, (yelp_id, timestamp, reason))
                    conn.commit()
                logging.info(f"⚠️ Logged failure for {yelp_id} in {table_name}")
            except Exception as e:
                logging.error(f"❌ Failed to log failure for {yelp_id}: {e}")

