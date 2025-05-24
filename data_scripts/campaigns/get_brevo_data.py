import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BREVO_API_KEY")

headers = {
    "accept": "application/json",
    "api-key": api_key
}

# === STEP 1: List all folders ===
folder_url = "https://api.brevo.com/v3/contacts/folders"
folder_response = requests.get(folder_url, headers=headers)

if folder_response.status_code == 200:
    folders = folder_response.json().get("folders", [])
    print("📁 Contact Folders:")
    for folder in folders:
        folder_id = folder['id']
        folder_name = folder['name']
        print(f"\n📂 Folder ID: {folder_id} | Name: {folder_name}")

        # === STEP 2: For each folder, list all lists in it ===
        lists_url = f"https://api.brevo.com/v3/contacts/folders/{folder_id}/lists?limit=50"
        lists_response = requests.get(lists_url, headers=headers)

        if lists_response.status_code == 200:
            lists = lists_response.json().get("lists", [])
            if not lists:
                print("   🔸 No contact lists in this folder.")
            else:
                for lst in lists:
                    print(f"   📋 List ID: {lst['id']} | Name: {lst['name']}")
        else:
            print(f"   ❌ Failed to get lists for folder {folder_id}: {lists_response.text}")
else:
    print(f"❌ Failed to get folders: {folder_response.text}")
