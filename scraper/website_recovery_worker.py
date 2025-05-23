import random
import time
import re
import urllib.parse
import logging
import traceback
import numpy as np
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import pandas as pd
import sqlite3
from multiprocessing import Process, cpu_count, Value, Manager, Lock
from datetime import datetime
from tqdm import tqdm
from config import FORCE_WEBSITE_OVERWRITE

DB_PATH = "businesses.db"
TABLE_NAME = "businesses"
FAILURE_TABLE = "website_recovery_failures"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def human_pause(min_delay=2.5, max_delay=5.0):
    time.sleep(random.uniform(min_delay, max_delay))

def is_valid_website(url: str) -> bool:
    if not url:
        return False
    invalid_patterns = ["s3-media", "yelpcdn.com", "bphoto", ".jpg", ".jpeg", ".png", ".gif", "yelp.com"]
    return not any(pattern in url.lower() for pattern in invalid_patterns)

def validate_website(url: str) -> str:
    if not url:
        return url
    match = re.match(r'^(https?://[^/]*?\.(com|org|net|edu|gov|io|co|us|biz|info))\b', url, re.IGNORECASE)
    return match.group(1) if match else url

def extract_website_from_href(href):
    if href and "url=" in href:
        match = re.search(r"url=([^&]+)", href)
        if match:
            return urllib.parse.unquote(match.group(1))
    return None

def get_website_from_jsonld(page):
    try:
        jsonld_script = page.locator('script[type="application/ld+json"]').first
        if jsonld_script.count() > 0:
            json_data = jsonld_script.inner_text()
            match = re.search(r'"url":"(https?://[^"]+)",?', json_data)
            if match:
                return validate_website(match.group(1))
    except Exception as e:
        logging.error(f"Error extracting website from JSON-LD: {e}")
    return None

def get_website_from_yelp(yelp_url, max_retries=3):
    headers = {
        "Accept-Language": random.choice(["en-US", "en-GB", "es-ES", "fr-FR"]),
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ])
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers(headers)

        attempt = 0
        while attempt < max_retries:
            try:
                page.goto(yelp_url, timeout=30000)
                page.wait_for_load_state("networkidle")
                human_pause(2.0, 4.0)
                page.mouse.wheel(0, 2000)
                human_pause(1.0, 2.0)

                link = page.locator('a[href*="/biz_redir?url="]').first
                if link.count() > 0:
                    href = link.get_attribute("href")
                    url = extract_website_from_href(href)
                    if url and is_valid_website(url):
                        return validate_website(url)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                for a_tag in soup.find_all("a", href=True):
                    if "/biz_redir?url=" in a_tag["href"]:
                        url = extract_website_from_href(a_tag["href"])
                        if url and is_valid_website(url):
                            return validate_website(url)

                jsonld_url = get_website_from_jsonld(page)
                if jsonld_url and is_valid_website(jsonld_url):
                    return validate_website(jsonld_url)

                return None
            except Exception as e:
                logging.error(f"Error scraping {yelp_url} on attempt {attempt + 1}: {e}")
                logging.error(traceback.format_exc())
                attempt += 1
                time.sleep(2 ** attempt)

        browser.close()
        return None

def ensure_failure_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {FAILURE_TABLE} (
            yelp_id TEXT PRIMARY KEY,
            website_checked_at TEXT,
            reason TEXT
        )
    """)
    conn.commit()

def log_failure(conn, yelp_id, timestamp, reason):
    conn.execute(f"""
        INSERT INTO {FAILURE_TABLE} (yelp_id, website_checked_at, reason)
        VALUES (?, ?, ?)
        ON CONFLICT(yelp_id) DO UPDATE SET
            website_checked_at=excluded.website_checked_at,
            reason=excluded.reason
    """, (yelp_id, timestamp, reason))

def worker(worker_id, chunk_df, progress, total, lock):
    conn = sqlite3.connect(DB_PATH)
    ensure_failure_table(conn)
    for _, row in chunk_df.iterrows():
        yelp_id = row["yelp_id"]
        yelp_url = row["website"]

        if not yelp_url or yelp_url.strip() == "":
            continue

        logging.info(f"[Worker {worker_id}] Scraping: {yelp_id}")
        try:
            scraped = get_website_from_yelp(yelp_url)
            website_checked_at = datetime.now().isoformat()
            if scraped:
                conn.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET real_website = ?, website_checked_at = ?
                    WHERE yelp_id = ?
                """, (scraped, website_checked_at, yelp_id))
                logging.info(f"[Worker {worker_id}] ✅ {yelp_id} -> {scraped}")
            else:
                conn.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET website_checked_at = ?
                    WHERE yelp_id = ?
                """, (website_checked_at, yelp_id))
                log_failure(conn, yelp_id, website_checked_at, "No website found")
                logging.info(f"[Worker {worker_id}] ❌ No website for {yelp_id}")

            conn.commit()
            human_pause(5, 12)
        except Exception as e:
            logging.error(f"[Worker {worker_id}] Error: {e}")
            logging.error(traceback.format_exc())
        finally:
            with lock:
                progress.value += 1
    conn.close()

def run_workers():
    conn = sqlite3.connect(DB_PATH)
    if FORCE_WEBSITE_OVERWRITE:
        query = f"""
            SELECT * FROM {TABLE_NAME}
            WHERE website IS NOT NULL AND TRIM(website) != ''
        """
    else:
        query = f"""
            SELECT * FROM {TABLE_NAME}
            WHERE (real_website IS NULL OR TRIM(real_website) = '')
            AND (website_checked_at IS NULL OR TRIM(website_checked_at) = '')
        """
    df = pd.read_sql(query, conn)
    conn.close()

    total_rows = len(df)
    if total_rows == 0:
        logging.info("No rows to process.")
        return

    num_cpus = max(cpu_count() - 2, 1)
    chunks = np.array_split(df, num_cpus)

    manager = Manager()
    lock = Lock()
    progress = Value("i", 0)

    with tqdm(total=total_rows, desc="Scraping Progress") as pbar:
        def update_progress():
            last_val = 0
            while True:
                with lock:
                    current_val = progress.value
                pbar.update(current_val - last_val)
                last_val = current_val
                if current_val >= total_rows:
                    break
                time.sleep(0.5)

        from threading import Thread
        progress_thread = Thread(target=update_progress)
        progress_thread.start()

        processes = []
        for i, chunk in enumerate(chunks):
            p = Process(target=worker, args=(i, chunk, progress, total_rows, lock))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        progress_thread.join()

    logging.info("🎉 All workers completed.")

if __name__ == "__main__":
    run_workers()
