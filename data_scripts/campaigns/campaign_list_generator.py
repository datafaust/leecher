import os
import pandas as pd
import sqlite3

DB_PATH = "businesses.db"
TABLE_NAME = "businesses"

# Connect to the database
conn = sqlite3.connect(DB_PATH)

# Load data from the businesses table
master = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
conn.close()

print("Columns:", master.columns.tolist())
print("Categories:", master['category_code'].unique())

# Remove rows with missing email
master = master[master['email'].notna()]
print("Rows with email:", master.shape[0])

# Define columns to keep
relevant_columns = [
    'yelp_id', 'name', 'phone_number', 'email',
    'real_website', 'zip_code',
    'city', 'category_term', 'category_code'
]

# Filter relevant columns and drop rows with missing email
master_filtered = master[relevant_columns].dropna(subset=['email'])

# Rename column for output
master_filtered.rename(columns={'real_website': 'website'}, inplace=True)

print("After filtering:", master_filtered.shape[0])

# Drop duplicate emails
master_filtered = master_filtered.drop_duplicates(subset='email')
print("After deduplication:", master_filtered.shape[0])

# Create output directory if needed
output_dir = "data_scripts/campaigns/by_category_exports"
os.makedirs(output_dir, exist_ok=True)

category_counts = []

# Group by category and write to CSV
for category, group in master_filtered.groupby('category_code'):
    safe_category = category.strip().replace(" ", "_").lower()
    output_path = os.path.join(output_dir, f"{safe_category}.csv")
    group.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

    category_counts.append({'category_code': category, 'email_count': len(group)})

# Save summary report
summary_df = pd.DataFrame(category_counts)
summary_path = os.path.join(output_dir, "REPORT_category_email_counts.csv")
summary_df.to_csv(summary_path, index=False)
print(f"Saved summary: {summary_path}")
