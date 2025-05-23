import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from scraper.proxies import maybe_use_proxy
from scraper.headers import get_random_headers
from scraper.website_utils import get_real_website_from_yelp
from scraper.email_utils import extract_email_from_website
#from scraper.data_utils import load_existing_data, save_combined_data, logging
#from scraper.custom_email_recovery_worker import scrape_site_for_email, clean_email

from scraper.data_utils import SQLiteHandler, logging
from multiprocessing import Lock
lock = Lock()
db = SQLiteHandler(lock=lock)

load_dotenv()
API_KEY = os.getenv('API_KEY')

def fetch_businesses(term, city, zip_code, categories, limit=50):
    location = f"{zip_code}, {city}"
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        **get_random_headers()
    }

    params = {
        'term': term,
        'location': location,
        'categories': categories,
        'limit': limit
    }

    url = 'https://api.yelp.com/v3/businesses/search'
    response = requests.get(url, headers=headers, params=params, proxies=maybe_use_proxy(), timeout=10)

    #existing_df = load_existing_data()
    #existing_ids = set(existing_df['yelp_id'].tolist()) if not existing_df.empty else set()
    existing_df = db.load_all_data()
    existing_ids = set(existing_df["yelp_id"].tolist())

    num_saved = 0

    if response.status_code == 200:
        for business in response.json().get('businesses', []):
            if business['id'] in existing_ids:
                logging.info(f"Skipping already-saved business: {business['id']}")
                continue

            yelp_url = business.get('url', 'N/A')
            real_website = get_real_website_from_yelp(yelp_url)
            email = extract_email_from_website(real_website)

            scraped_at = datetime.utcnow().isoformat()
            row = {
                'yelp_id': business['id'],
                'name': business['name'],
                'address': ', '.join(business['location']['display_address']),
                'image_url': business.get('image_url', 'N/A'),
                'phone_number': business.get('phone', 'N/A'),
                'website': yelp_url,
                'real_website': real_website,
                'email': email,
                'latitude': business['coordinates']['latitude'],
                'longitude': business['coordinates']['longitude'],
                'zip_code': business['location'].get('zip_code', 'N/A'),
                'country': business['location'].get('country', 'N/A'),
                'city': business['location'].get('city', 'N/A'),
                'address1': business['location'].get('address1', 'N/A'),
                'category_term': term,
                'category_code': categories,
                'scraped_at': scraped_at 
            }

            # df = pd.DataFrame([row])
            # save_combined_data(existing_df, df)
            # existing_df = pd.concat([existing_df, df], ignore_index=True)
            # num_saved += 1

            # sql lite version
            df = pd.DataFrame([row])
            db.insert_or_replace_rows(df)
            existing_ids.add(row["yelp_id"])
            num_saved += 1


        logging.info(f"Fetched and saved {num_saved} new businesses for {zip_code} - {term}")
        print(f"✅ Saved {num_saved} new businesses for ZIP {zip_code} - {term}")

    else:
        msg = f"❌ Error {response.status_code}: {response.text}"
        print(msg)
        logging.error(msg)

    return []  # Function no longer returns list; data is saved directly
