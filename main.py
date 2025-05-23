import random
import time
import subprocess
from scraper.config import CATEGORIES, NYC_ZIPS, CITY, RECORD_LIMIT
from scraper.yelp_api import fetch_businesses
from tqdm import tqdm

def run_scraper():
    total_tasks = len(NYC_ZIPS) * len(CATEGORIES)
    with tqdm(total=total_tasks, desc="Scraping Progress") as pbar:
        for zip_code in NYC_ZIPS:
            for target in CATEGORIES:
                fetch_businesses(
                    term=target['term'],
                    city=CITY,
                    zip_code=zip_code,
                    categories=target['category'],
                    limit=RECORD_LIMIT
                )
                sleep_time = random.randint(5, 20)
                print(f"⏳ Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
                pbar.update(1)


def run_website_recovery():
    print("\n🛠️ Starting post-processing: recovering real_website values using Playwright...\n")
    subprocess.run(["python", "scraper/website_recovery_worker.py"])
    print("\n✅ Website recovery complete.\n")

def run_email_recovery():
    print("\n📬 Starting email scraping from real_website...\n")
    subprocess.run(["python", "scraper/email_recovery_worker.py"])
    print("\n✅ Email recovery complete.\n")

if __name__ == '__main__':
    run_scraper()
    run_website_recovery()
    run_email_recovery()