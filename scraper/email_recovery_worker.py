import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import time
import random
import logging
import numpy as np
from multiprocessing import Process, Queue, cpu_count
from tqdm import tqdm
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
from data_utils import SQLiteHandler
import warnings
from urllib3.exceptions import InsecureRequestWarning
from config import FORCE_EMAIL_OVERWRITE



warnings.filterwarnings("ignore", message="resource_tracker: There appear to be .* leaked semaphore")

# Suppress unverified HTTPS request warnings
warnings.simplefilter("ignore", InsecureRequestWarning)

# Optional: suppress multiprocessing semaphore warnings
warnings.filterwarnings("ignore", message="resource_tracker: There appear to be .* leaked semaphore")


EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

db = SQLiteHandler()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

headers = {
    "User-Agent": "Mozilla/5.0"
}

def clean_email(email: str) -> str:
    if not email or "@" not in email:
        return ""

    email = email.strip().lower()
    email = email.replace("mailto:", "")
    email = email.replace(" ", "").replace("\n", "").replace("\t", "")
    email = re.sub(r"(\[at\]|\(at\)|\sat\s)", "@", email, flags=re.IGNORECASE)
    email = re.sub(r"(\[dot\]|\(dot\)|\sdot\s)", ".", email, flags=re.IGNORECASE)
    email = re.sub(r"[\"\'<>\\]", "", email)
    email = re.sub(r"^(email|[\d\-\(\)\.]{5,})+", "", email)

    if not EMAIL_REGEX.match(email):
        return ""

    tlds = [".com", ".org", ".net", ".edu", ".gov", ".us", ".co", ".io", ".info", ".biz"]
    tld_regex = "(" + "|".join(re.escape(tld) for tld in tlds) + ")"
    match = re.search(tld_regex, email)
    if match:
        email = email[:match.end()]

    return email

def extract_emails_from_text(text):
    emails = set()
    raw_candidates = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    for email in raw_candidates:
        if "@" in email and "." in email.split("@")[-1]:
            emails.add(email.strip())
    return emails

def extract_emails_from_soup(soup):
    text = soup.get_text()
    emails = extract_emails_from_text(text)
    for a in soup.find_all("a", href=True):
        if "mailto:" in a["href"]:
            email = a["href"].split("mailto:")[-1].split("?")[0]
            emails.add(email.strip())
    return emails

def contains_contact_form(soup):
    form_keywords = ['contact', 'message', 'email', 'name', 'submit']
    forms = soup.find_all("form")
    for form in forms:
        form_text = form.get_text(" ").lower()
        if any(kw in form_text for kw in form_keywords):
            return True
    return False

def scrape_site_for_email(base_url):
    found_emails = set()
    has_contact_form = False
    visited = set()

    def try_requests(url):
        nonlocal has_contact_form
        try:
            response = requests.get(url, headers=headers, timeout=10, verify=False)
            soup = BeautifulSoup(response.text, "html.parser")
            found_emails.update(extract_emails_from_soup(soup))
            if contains_contact_form(soup):
                has_contact_form = True
            return soup
        except:
            return None

    def try_playwright(url):
        nonlocal has_contact_form
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=15000)
                page.wait_for_load_state("networkidle")
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                found_emails.update(extract_emails_from_soup(soup))
                if contains_contact_form(soup):
                    has_contact_form = True
                browser.close()
                return soup
        except TimeoutError:
            return None
        except:
            return None

    homepage_url = base_url if base_url.endswith('/') else base_url + '/'
    homepage_soup = try_requests(homepage_url)
    if not homepage_soup:
        homepage_soup = try_playwright(homepage_url)

    candidate_paths = set(["/contact", "/contact-us", "/about", "/team", "/support", "/staff", "/help", "/info"])
    if homepage_soup:
        for a in homepage_soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(kw in href for kw in ["contact", "about", "support", "help"]):
                path = urlparse(href).path if "http" in href else href
                full_url = urljoin(base_url, path)
                if full_url not in visited:
                    candidate_paths.add(full_url)

    for path in candidate_paths:
        url = urljoin(base_url, path)
        if url in visited:
            continue
        visited.add(url)
        soup = try_requests(url)
        if not soup:
            soup = try_playwright(url)
        if found_emails:
            break

    return list(found_emails), has_contact_form

def worker(worker_id, chunk, q):
    results = []
    for row in chunk.to_dict(orient="records"):
        yelp_id = row["yelp_id"]
        website = row.get("real_website", "")
        if not website or website == "N/A":
            continue

        try:
            emails, form_only = scrape_site_for_email(website)
            email_to_use = row.get("email", "")
            for email in emails:
                cleaned = clean_email(email)
                if EMAIL_REGEX.match(cleaned):
                    email_to_use = cleaned
                    break
            scraped_at = datetime.utcnow().isoformat()
            db.update_business_email_info(yelp_id, email_to_use, form_only, scraped_at)
        except Exception as e:
            logging.error(f"[Worker {worker_id}] Error on {yelp_id}: {e}")
            db.log_failure("email_recovery_failures", yelp_id, reason=str(e))
        time.sleep(random.uniform(2.0, 4.0))
    q.put(results)

def run():
    df = db.load_all_data()

    if FORCE_EMAIL_OVERWRITE:
        # Process all rows that have a real website
        df = df[(df['real_website'].notna()) & (df['real_website'].str.strip() != '')]
    else:
        # Process only those without email_checked_at
        df = df[
            (df['real_website'].notna()) &
            (df['real_website'].str.strip() != '') &
            (df['email_checked_at'].isna() | df['email_checked_at'].str.strip().eq(''))
        ]


    if df.empty:
        print("✅ No entries require email recovery.")
        return

    q = Queue()
    num_workers = max(cpu_count() - 2, 1)
    chunks = np.array_split(df, num_workers)
    processes = []

    for i, chunk in enumerate(chunks):
        p = Process(target=worker, args=(i, chunk, q))
        p.start()
        processes.append(p)

    for _ in tqdm(processes, desc='🔄 Collecting worker results'):
        q.get()

    for p in processes:
        p.join()

    logging.info("✅ Email recovery complete.")

if __name__ == "__main__":
    run()
