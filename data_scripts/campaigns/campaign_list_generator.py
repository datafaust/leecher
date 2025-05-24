import sys
import os
import re
import pandas as pd
import sqlite3
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Relative import fix
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from scraper.email_utils import clean_email

# === SETTINGS ===
UPLOAD_BREVO = True
DB_PATH = "businesses.db"
TABLE_NAME = "businesses"
OUTPUT_DIR = "data_scripts/campaigns/by_category_exports"
BREVO_FOLDER_ID = 353

# === ENV SETUP ===
load_dotenv()
api_key = os.getenv("BREVO_API_KEY")
headers = {
    "api-key": api_key,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# === LOAD DATA ===
conn = sqlite3.connect(DB_PATH)
master = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
conn.close()

# === EMAIL CLEANING ===
master = master[master['email'].notna()].copy()
master["email"] = master["email"].apply(lambda e: clean_email(str(e)))
master = master[master["email"].str.contains("@", na=False) & (master["email"].str.strip() != "")]
print("✅ Valid emails after cleaning:", master.shape[0])

# === FILTER + DEDUPE ===
relevant_columns = [
    'yelp_id', 'name', 'phone_number', 'email',
    'real_website', 'zip_code', 'city',
    'category_term', 'category_code'
]
master_filtered = master[relevant_columns].drop_duplicates(subset='email')
master_filtered.rename(columns={'real_website': 'website'}, inplace=True)
print("✅ After deduplication:", master_filtered.shape[0])

# === DIRECTORY SETUP ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
category_counts = []

# === UTILS ===
def get_existing_lists():
    try:
        response = requests.get("https://api.brevo.com/v3/contacts/lists", headers=headers)
        if response.status_code == 200:
            return {lst['name']: lst['id'] for lst in response.json().get("lists", [])}
        else:
            print(f"⚠️ Failed to fetch existing lists: {response.text}")
    except Exception as e:
        print(f"⚠️ Error fetching lists: {e}")
    return {}

# === MAIN LOOP ===
existing_lists = get_existing_lists()

for category, group in master_filtered.groupby('category_code'):
    safe_category = re.sub(r"\W+", "_", category.strip().lower())
    list_name = f"{safe_category}_contacts"

    # Clean again in case of re-runs
    group = group.copy()
    group["email"] = group["email"].apply(lambda e: clean_email(str(e)))
    group = group[group["email"].str.contains("@", na=False) & (group["email"].str.strip() != "")]

    output_path = os.path.join(OUTPUT_DIR, f"{safe_category}.csv")
    group.to_csv(output_path, index=False)
    print(f"📁 Saved: {output_path}")
    category_counts.append({'category_code': category, 'email_count': len(group)})

    if not UPLOAD_BREVO:
        continue

    # === LIST MANAGEMENT (SAFE) ===
    list_id = get_existing_lists().get(list_name)
    if not list_id:
        response = requests.post(
            "https://api.brevo.com/v3/contacts/lists",
            headers=headers,
            json={"name": list_name, "folderId": BREVO_FOLDER_ID}
        )
        if response.status_code == 201:
            list_id = response.json()["id"]
            existing_lists[list_name] = list_id
            print(f"✅ Created new Brevo list: {list_name} (ID: {list_id})")
        elif response.status_code == 400 and "duplicate_parameter" in response.text.lower():
            print(f"⚠️ Race condition detected. Refreshing list...")
            list_id = get_existing_lists().get(list_name)
            if not list_id:
                print(f"❌ Could not resolve list ID for: {list_name}")
                continue
        else:
            print(f"❌ Failed to create list '{list_name}': {response.text}")
            continue
    else:
        print(f"🔁 Using existing Brevo list: {list_name} (ID: {list_id})")

    # === EMAIL UPLOAD ===
    successful = 0
    for _, row in tqdm(group.iterrows(), total=group.shape[0], desc=f"Uploading to {list_name}"):
        email = clean_email(str(row["email"]))
        if not email:
            continue

        # Step 1: Create or update the contact
        contact_resp = requests.post(
            "https://api.brevo.com/v3/contacts",
            headers=headers,
            json={"email": email, "updateEnabled": True}
        )
        if contact_resp.status_code not in (200, 201, 202, 204):
            print(f"❌ Error creating contact {email}: {contact_resp.text}")
            continue

        # Step 2: Add to the specific list
        list_resp = requests.post(
            f"https://api.brevo.com/v3/contacts/lists/{list_id}/contacts/add",
            headers=headers,
            json={"emails": [email]}
        )
        if list_resp.status_code in (200, 201, 202):
            successful += 1
        else:
            print(f"❌ Error adding {email} to list {list_name}: {list_resp.text}")

    print(f"📤 {successful} contacts added to Brevo list: {list_name}")

# === SAVE SUMMARY ===
summary_df = pd.DataFrame(category_counts)
summary_path = os.path.join(OUTPUT_DIR, "REPORT_category_email_counts.csv")
summary_df.to_csv(summary_path, index=False)
print("✅ Summary saved.")


# import os
# import pandas as pd
# import sqlite3
# from dotenv import load_dotenv

# load_dotenv()
# api_key = os.getenv("BREVO_API_KEY")


# DB_PATH = "businesses.db"
# TABLE_NAME = "businesses"

# # Connect to the database
# conn = sqlite3.connect(DB_PATH)

# # Load data from the businesses table
# master = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
# conn.close()

# print("Columns:", master.columns.tolist())
# print("Categories:", master['category_code'].unique())

# # Remove rows with missing email
# master = master[master['email'].notna()]
# print("Rows with email:", master.shape[0])

# # Define columns to keep
# relevant_columns = [
#     'yelp_id', 'name', 'phone_number', 'email',
#     'real_website', 'zip_code',
#     'city', 'category_term', 'category_code'
# ]

# # Filter relevant columns and drop rows with missing email
# master_filtered = master[relevant_columns].dropna(subset=['email'])

# # Rename column for output
# master_filtered.rename(columns={'real_website': 'website'}, inplace=True)

# print("After filtering:", master_filtered.shape[0])

# # Drop duplicate emails
# master_filtered = master_filtered.drop_duplicates(subset='email')
# print("After deduplication:", master_filtered.shape[0])

# # Create output directory if needed
# output_dir = "data_scripts/campaigns/by_category_exports"
# os.makedirs(output_dir, exist_ok=True)

# category_counts = []

# # Group by category and write to CSV
# for category, group in master_filtered.groupby('category_code'):
#     safe_category = category.strip().replace(" ", "_").lower()
#     output_path = os.path.join(output_dir, f"{safe_category}.csv")
#     group.to_csv(output_path, index=False)
#     print(f"Saved: {output_path}")

#     category_counts.append({'category_code': category, 'email_count': len(group)})

# # Save summary report
# summary_df = pd.DataFrame(category_counts)
# summary_path = os.path.join(output_dir, "REPORT_category_email_counts.csv")
# summary_df.to_csv(summary_path, index=False)
# print(f"Saved summary: {summary_path}")
