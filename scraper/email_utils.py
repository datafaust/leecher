import requests
import re
from bs4 import BeautifulSoup
from scraper.headers import get_random_headers
from scraper.proxies import maybe_use_proxy
import socket
import logging
import urllib


#EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
COMMON_TLDS = (
    ".com", ".org", ".net", ".edu", ".gov", ".us", ".co", ".io", ".info", ".biz"
)


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
    
def clean_email(raw: str) -> str:
    if not isinstance(raw, str):
        return ""

    # Normalize the string
    raw = raw.strip().lower()

    # Extract all potential email candidates
    candidates = EMAIL_REGEX.findall(raw)

    for email in candidates:
        # Remove known bad suffixes/prefixes (digits, junk strings)
        email = re.sub(r"^(?:[\d\-\+\.]+)?", "", email)  # Remove prefix numbers/symbols
        email = re.sub(r"(phone|fax|office|toll|protected|start|end)?[\d\-\.\+]*$", "", email, flags=re.IGNORECASE)

        email = email.strip(" .,:;*/\\'\"")

        # Sanity checks
        if any(email.endswith(tld) for tld in COMMON_TLDS) and "@" in email and email.count("@") == 1:
            return email

    return ""



# def clean_email(email: str) -> str:
#     if not email or "@" not in email:
#         return ""

#     email = email.strip().lower()
#     email = email.replace("mailto:", "")
#     email = email.replace(" ", "").replace("\n", "").replace("\t", "")
#     email = re.sub(r"(\[at\]|\(at\)|\sat\s)", "@", email, flags=re.IGNORECASE)
#     email = re.sub(r"(\[dot\]|\(dot\)|\sdot\s)", ".", email, flags=re.IGNORECASE)
#     email = re.sub(r"[\"\'<>\\]", "", email)

#     # Extract the first valid-looking email from the string
#     match = EMAIL_REGEX.search(email)
#     if match:
#         cleaned = match.group(0)
#     else:
#         return ""

#     # Remove trailing common garbage patterns (e.g. Fax, Phone, etc.)
#     cleaned = re.sub(r"(fax|phone|protected|start|toll|office)+$", "", cleaned, flags=re.IGNORECASE)

#     # Ensure no junk like trailing punctuation
#     cleaned = cleaned.strip(" .,:;*")

#     # Final validation
#     return cleaned if EMAIL_REGEX.match(cleaned) else ""

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

