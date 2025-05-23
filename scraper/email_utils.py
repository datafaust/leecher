import requests
import re
from bs4 import BeautifulSoup
from scraper.headers import get_random_headers
from scraper.proxies import maybe_use_proxy
import socket
import logging

def extract_email_from_website(url):
    if not url or url == "N/A":
        return "N/A"

    # Optional: Skip unresolvable domains
    try:
        hostname = urllib.parse.urlparse(url).hostname
        if hostname:
            socket.gethostbyname(hostname)
    except Exception:
        logging.warning(f"❌ Skipping unresolvable domain: {url}")
        return "N/A"

    try:
        res = requests.get(url, headers=get_random_headers(), proxies=maybe_use_proxy(), timeout=10, verify=False)
        if res.status_code != 200:
            return 'N/A'

        soup = BeautifulSoup(res.text, 'html.parser')
        emails = set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", soup.get_text()))
        return next(iter(emails), 'N/A')
    except Exception as e:
        logging.error(f"Error scraping email from {url}: {e}")
        return 'N/A'


# def extract_email_from_website(url):
#     try:
#         res = requests.get(url, headers=get_random_headers(), proxies=maybe_use_proxy(), timeout=10)
#         if res.status_code != 200:
#             return 'N/A'

#         soup = BeautifulSoup(res.text, 'html.parser')
#         emails = set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", soup.get_text()))
#         return next(iter(emails), 'N/A')
#     except Exception as e:
#         print(f"Error scraping email from {url}: {e}")
#         return 'N/A'

# def extract_email_from_website(url):
#     if not url or url.strip().lower() == "n/a":
#         return 'N/A'  # Early return for invalid URLs

#     try:
#         res = requests.get(url, headers=get_random_headers(), proxies=maybe_use_proxy(), timeout=10)
#         if res.status_code != 200:
#             return 'N/A'

#         soup = BeautifulSoup(res.text, 'html.parser')
#         emails = set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", soup.get_text()))
#         return next(iter(emails), 'N/A')
#     except Exception as e:
#         print(f"Error scraping email from {url}: {e}")
#         return 'N/A'

