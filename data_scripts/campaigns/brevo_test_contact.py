import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BREVO_API_KEY")

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "api-key": api_key
}

def create_contact(folder_id, list_id, email):
    print(f"➡️ Attempting to create contact: {email}")
    
    payload = {
        "email": email,
        "listIds": [list_id],
        "updateEnabled": True
    }

    response = requests.post(
        "https://api.brevo.com/v3/contacts",
        headers=headers,
        json=payload
    )

    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if response.status_code in (200, 201, 202):
        print(f"✅ Successfully created contact: {email}")
    else:
        print(f"❌ Error creating contact {email}:")
        print(f"   Status Code: {response.status_code}")
        print(f"   Response: {response.text}")
        print(f"   Payload: {payload}")

if __name__ == "__main__":
    FOLDER_ID = 353
    LIST_ID = 354
    EMAIL = "jackerman@wffacpa.com"
    #EMAIL = "test_integration_12345@example.com"
    create_contact(FOLDER_ID, LIST_ID, EMAIL)
